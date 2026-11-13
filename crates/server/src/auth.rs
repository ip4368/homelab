//! Logging in: who may use the server, their sessions, and the guard in
//! front of the API.
//!
//! **Users** live in a YAML file in the shape of Authelia's file users
//! database (`users: {name: {displayname, password: $argon2id$…, disabled}}`),
//! so the same file can seed Authelia when the cluster moves to OIDC. It is
//! re-read when it changes: `server user passwd|delete` (or an edit) takes
//! effect without a restart, and ends the sessions of a user it removes,
//! disables or gives a new password.
//!
//! **Sessions** are random tokens in an `HttpOnly`, `SameSite=Strict` cookie,
//! held in this process's memory (a restart logs everyone out). A session
//! records *how* its user proved who they are ([`Identity`]), so an OIDC
//! login can later make the same kind of session as a password does.
//!
//! **The guard** ([`guard`]) answers 401 for any `/api` path but the three
//! under `/api/auth/` without a live session, and 403 for a state-changing
//! request or WebSocket whose `Origin` is another site's (the cookie alone
//! would not stop a page on another port of the same host).

use axum::extract::ws::{CloseFrame, Message};
use axum::extract::{ConnectInfo, Extension, Json, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Semaphore, watch};
use ts_rs::TS;

/// The session cookie's name.
pub const COOKIE: &str = "homelab_session";
/// A session no request has used for this long has ended.
pub const SESSION_IDLE: Duration = Duration::from_secs(12 * 3600);
/// No session outlives this, however busy.
pub const SESSION_MAX: Duration = Duration::from_secs(7 * 24 * 3600);
/// The WebSocket close code sent when the session behind a socket ends.
pub const SESSION_ENDED: u16 = 4401;
/// Wrong passwords allowed from one address, or for one user, before each
/// further try must wait; the wait doubles per failure up to [`MAX_WAIT`].
pub const FREE_TRIES: u32 = 5;
pub const MAX_WAIT: Duration = Duration::from_secs(300);
/// A run of failures is forgotten after this long without another.
const FORGET_FAILURES: Duration = Duration::from_secs(15 * 60);
/// Password checks that may run at once (each costs ~19 MiB and a few tens of ms).
const CONCURRENT_CHECKS: usize = 4;
const MIN_PASSWORD: usize = 8;
const MAX_PASSWORD: usize = 1024;
/// The public paths: everything else under `/api` needs a session.
const PUBLIC: [&str; 3] = ["/api/auth/session", "/api/auth/login", "/api/auth/logout"];

// ---------------------------------------------------------------- wire types

/// How a user can prove who they are. Only passwords today; an OIDC
/// provider (e.g. Authelia) would be another variant with its name and the
/// URL that starts its login, and the login page would offer it as a button.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum AuthMethod {
    Password,
}

/// Who is logged in, and until when at the latest (Unix ms) if idle.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct AuthSession {
    pub user: String,
    pub method: AuthMethod,
    #[ts(type = "number")]
    pub expires_at: u64,
}

/// `GET /api/auth/session`: the session, if any, and the ways to log in.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct AuthStatus {
    pub session: Option<AuthSession>,
    pub methods: Vec<AuthMethod>,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[ts(export)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

// ---------------------------------------------------------------- users file

/// The users file: Authelia's file users database, as far as we read it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsersFile {
    #[serde(default)]
    pub users: BTreeMap<String, UserEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserEntry {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub displayname: String,
    /// An Argon2 PHC string (`$argon2id$v=19$m=…`).
    pub password: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disabled: bool,
    /// What Authelia also keeps (email, groups), kept as it was.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml_ng::Value>,
}

impl UsersFile {
    /// The file at `path`, or `None` if there is none.
    pub fn read(path: &Path) -> io::Result<Option<UsersFile>> {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if text.trim().is_empty() {
            return Ok(Some(UsersFile::default()));
        }
        let file: UsersFile = serde_yaml_ng::from_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", path.display())))?;
        Ok(Some(file))
    }

    /// Replaces the file at `path` in one step, readable by its owner only.
    pub fn write(&self, path: &Path) -> io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(".{}.tmp", path.file_name().and_then(|n| n.to_str()).unwrap_or("users")));
        let body = format!(
            "# Who may log in to the homelab server. The same shape as Authelia's file users database;\n\
             # manage it with `./lab.sh user passwd|delete|list`.\n{}",
            serde_yaml_ng::to_string(self).map_err(io::Error::other)?
        );
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    }
}

/// A user name: 1–64 of letters, digits and `._@-`, not starting with a dot or dash.
pub fn valid_username(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || "._@-".contains(c))
        && !name.starts_with(['.', '-'])
}

/// An Argon2id hash of `password` with a fresh salt, as a PHC string.
pub fn hash_password(password: &str) -> String {
    use argon2::password_hash::PasswordHasher;
    argon2::Argon2::default().hash_password(password.as_bytes()).expect("argon2 hashes any password").to_string()
}

/// Whether `password` matches the PHC string `phc` (any Argon2 variant and parameters).
pub fn verify_password(phc: &str, password: &str) -> bool {
    use argon2::password_hash::PasswordVerifier;
    use argon2::password_hash::phc::PasswordHash;
    PasswordHash::new(phc).is_ok_and(|h| argon2::Argon2::default().verify_password(password.as_bytes(), &h).is_ok())
}

/// Checked against when the user does not exist, so that a wrong name costs
/// as long as a wrong password and the timing does not tell them apart.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| hash_password("no such user"))
}

/// A password for a new account: 24 characters from an alphabet without
/// look-alikes (about 137 bits).
pub fn generate_password() -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..24).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char).collect()
}

fn new_token() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

type Users = Arc<BTreeMap<String, UserEntry>>;

/// The users file, re-read whenever its modification time or size changes.
struct UserDb {
    path: PathBuf,
    cache: Mutex<(Option<(SystemTime, u64)>, Users)>,
}

impl UserDb {
    /// The users as the file now says. An unreadable or invalid file means
    /// nobody: logins fail and sessions end until it is fixed (it is logged).
    fn current(&self) -> Users {
        let stamp = std::fs::metadata(&self.path).ok().map(|m| (m.modified().unwrap_or(UNIX_EPOCH), m.len()));
        let mut cache = self.cache.lock().expect("users lock");
        if cache.0.is_some() && cache.0 == stamp {
            return cache.1.clone();
        }
        let users = match UsersFile::read(&self.path) {
            Ok(Some(f)) => f.users,
            Ok(None) => {
                eprintln!("auth: {} is missing: nobody can log in", self.path.display());
                BTreeMap::new()
            }
            Err(e) => {
                eprintln!("auth: cannot read the users file, so nobody can log in: {e}");
                BTreeMap::new()
            }
        };
        *cache = (stamp, Arc::new(users));
        cache.1.clone()
    }
}

// ---------------------------------------------------------------- sessions

/// Who a session belongs to and how they logged in.
#[derive(Debug, Clone, PartialEq)]
pub struct Identity {
    pub user: String,
    pub method: AuthMethod,
}

struct Session {
    identity: Identity,
    /// The password hash the user logged in against: a new password ends the session.
    credential: String,
    created: Instant,
    last_seen: Instant,
    /// Dropped when the session ends, which wakes every [`SessionGuard`].
    end: watch::Sender<()>,
}

impl Session {
    fn deadline(&self) -> Instant {
        (self.last_seen + SESSION_IDLE).min(self.created + SESSION_MAX)
    }
}

/// The session behind a request, put in its extensions by [`guard`]. Long
/// streams (WebSockets) wait on [`SessionGuard::ended`] to stop when it ends.
#[derive(Clone)]
pub struct SessionGuard {
    pub identity: Identity,
    end: watch::Receiver<()>,
}

impl SessionGuard {
    /// Resolves once the session has ended (logout, expiry, user removed or
    /// re-passworded).
    pub async fn ended(&mut self) {
        while self.end.changed().await.is_ok() {}
    }

    /// The close frame for a socket whose session ended.
    pub fn close_frame() -> Message {
        Message::Close(Some(CloseFrame { code: SESSION_ENDED, reason: "session ended: log in again".into() }))
    }
}

/// Failed logins per key (an address or a user name).
#[derive(Default)]
struct Throttle {
    tries: HashMap<String, (u32, Instant)>,
}

impl Throttle {
    /// How long `key` must still wait before another try, if at all.
    fn wait(&mut self, key: &str, now: Instant) -> Option<Duration> {
        self.tries.retain(|_, (_, last)| now.duration_since(*last) < FORGET_FAILURES);
        let &(count, last) = self.tries.get(key)?;
        if count < FREE_TRIES {
            return None;
        }
        let delay = Duration::from_secs(1u64 << (count - FREE_TRIES).min(16)).min(MAX_WAIT);
        (last + delay).checked_duration_since(now).filter(|d| !d.is_zero())
    }

    /// Counts a try before its password is checked, so that tries racing
    /// each other cannot all slip in under the limit; success clears it.
    fn attempt(&mut self, key: &str, now: Instant) {
        let e = self.tries.entry(key.to_string()).or_insert((0, now));
        e.0 += 1;
        e.1 = now;
    }

    fn clear(&mut self, key: &str) {
        self.tries.remove(key);
    }
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The users file.
    pub users: PathBuf,
    /// Origins (`https://homelab.example`) besides the server's own that may
    /// make changes and open sockets: for a reverse proxy that rewrites `Host`.
    pub allowed_origins: Vec<String>,
    /// Always mark the cookie `Secure` (it is anyway behind a proxy that says
    /// `X-Forwarded-Proto: https`).
    pub secure_cookie: bool,
}

impl AuthConfig {
    /// `HOMELAB_USERS` (default `$LAB_STATE/users.yml`, i.e.
    /// `/var/lib/homelab-lab/users.yml`), `ALLOWED_ORIGINS` (comma-separated)
    /// and `COOKIE_SECURE=1`.
    pub fn from_env() -> Self {
        let state = std::env::var("LAB_STATE").unwrap_or_else(|_| "/var/lib/homelab-lab".into());
        Self {
            users: std::env::var("HOMELAB_USERS").map_or_else(|_| Path::new(&state).join("users.yml"), PathBuf::from),
            allowed_origins: std::env::var("ALLOWED_ORIGINS")
                .unwrap_or_default()
                .split(',')
                .map(|o| o.trim().trim_end_matches('/').to_string())
                .filter(|o| !o.is_empty())
                .collect(),
            secure_cookie: std::env::var("COOKIE_SECURE").is_ok_and(|v| v == "1" || v == "true"),
        }
    }
}

/// Logins and sessions; cheap to clone.
#[derive(Clone)]
pub struct Auth {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: AuthConfig,
    users: UserDb,
    sessions: Mutex<HashMap<String, Session>>,
    throttle: Mutex<Throttle>,
    checks: Semaphore,
}

/// Why a login did not happen.
#[derive(Debug, PartialEq)]
pub enum LoginError {
    /// Wrong name or password, or a disabled user: the same answer for all three.
    Invalid,
    /// Too many recent failures from this address or for this user.
    Throttled(Duration),
}

impl Auth {
    pub fn new(cfg: AuthConfig) -> Self {
        let users = UserDb { path: cfg.users.clone(), cache: Mutex::new((None, Arc::new(BTreeMap::new()))) };
        Self {
            inner: Arc::new(Inner { cfg, users, sessions: Mutex::new(HashMap::new()), throttle: Mutex::new(Throttle::default()), checks: Semaphore::new(CONCURRENT_CHECKS) }),
        }
    }

    pub fn users_file(&self) -> &Path {
        &self.inner.cfg.users
    }

    /// Creates the users file on first start: one user, `admin`, with a
    /// generated password written beside it (`initial-admin-password`, mode
    /// 0600). Returns that file's path when it did so.
    pub fn bootstrap(&self) -> io::Result<Option<PathBuf>> {
        let path = self.users_file();
        if let Some(file) = UsersFile::read(path)? {
            if !file.users.values().any(|u| !u.disabled) {
                eprintln!("auth: {} lists no enabled user, so nobody can log in: add one with `./lab.sh user passwd <name>`", path.display());
            }
            warn_if_readable(path);
            return Ok(None);
        }
        let password = generate_password();
        let mut file = UsersFile::default();
        file.users.insert("admin".into(), UserEntry { displayname: "Administrator".into(), password: hash_password(&password), disabled: false, extra: BTreeMap::new() });
        file.write(path)?;
        let note = path.with_file_name("initial-admin-password");
        write_private(&note, &format!("{password}\n"))?;
        Ok(Some(note))
    }

    /// Ends sessions that expired or whose user changed, once a minute, so
    /// their sockets close even if no request comes.
    pub fn spawn_sweeper(&self) {
        let auth = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                auth.sweep(Instant::now());
            }
        });
    }

    fn sweep(&self, now: Instant) {
        let users = self.inner.users.current();
        let mut sessions = self.inner.sessions.lock().expect("sessions lock");
        sessions.retain(|_, s| s.deadline() > now && still_valid(&users, s));
    }

    /// Checks a password and opens a session: its cookie token and status.
    pub async fn login(&self, username: &str, password: &str, from: Option<SocketAddr>) -> Result<(String, AuthSession), LoginError> {
        if !valid_username(username) || password.is_empty() || password.len() > MAX_PASSWORD {
            return Err(LoginError::Invalid);
        }
        let keys = [format!("user:{username}"), format!("addr:{}", from.map_or("-".into(), |a| a.ip().to_string()))];
        {
            let now = Instant::now();
            let mut t = self.inner.throttle.lock().expect("throttle lock");
            if let Some(wait) = keys.iter().filter_map(|k| t.wait(k, now)).max() {
                return Err(LoginError::Throttled(wait));
            }
            keys.iter().for_each(|k| t.attempt(k, now));
        }
        let users = self.inner.users.current();
        let entry = users.get(username).cloned();
        let phc = entry.as_ref().map_or_else(|| dummy_hash().to_string(), |e| e.password.clone());
        let ok = {
            let _permit = self.inner.checks.acquire().await.expect("the semaphore is never closed");
            let pw = password.to_string();
            tokio::task::spawn_blocking(move || verify_password(&phc, &pw)).await.unwrap_or(false)
        };
        let Some(entry) = entry.filter(|e| ok && !e.disabled) else {
            return Err(LoginError::Invalid);
        };
        {
            let mut t = self.inner.throttle.lock().expect("throttle lock");
            keys.iter().for_each(|k| t.clear(k));
        }
        let identity = Identity { user: username.to_string(), method: AuthMethod::Password };
        Ok(self.open_session(identity, entry.password))
    }

    /// Opens a session for someone already proven to be `identity` (the hook
    /// an OIDC callback would use too).
    pub fn open_session(&self, identity: Identity, credential: String) -> (String, AuthSession) {
        let token = new_token();
        let now = Instant::now();
        let (end, _) = watch::channel(());
        let session = Session { identity, credential, created: now, last_seen: now, end };
        let status = status_of(&session, now);
        self.inner.sessions.lock().expect("sessions lock").insert(token.clone(), session);
        (token, status)
    }

    /// Ends the session `token` names, if any; whose it was.
    pub fn logout(&self, token: &str) -> Option<String> {
        self.inner.sessions.lock().expect("sessions lock").remove(token).map(|s| s.identity.user)
    }

    /// The live session `token` names, refreshed as used just now.
    fn touch(&self, token: &str) -> Option<(SessionGuard, AuthSession)> {
        let users = self.inner.users.current();
        let now = Instant::now();
        let mut sessions = self.inner.sessions.lock().expect("sessions lock");
        let s = sessions.get_mut(token)?;
        if s.deadline() <= now || !still_valid(&users, s) {
            sessions.remove(token);
            return None;
        }
        s.last_seen = now;
        Some((SessionGuard { identity: s.identity.clone(), end: s.end.subscribe() }, status_of(s, now)))
    }

    /// Whether a browser request from `Origin` may change things or open a
    /// socket: its own site, or one configured. Clients that send no
    /// `Origin` (curl, scripts) are not browsers being misused, so they may.
    pub fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        let Some(origin) = headers.get(header::ORIGIN) else {
            return true;
        };
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        let origin = origin.trim_end_matches('/');
        if self.inner.cfg.allowed_origins.iter().any(|o| o.eq_ignore_ascii_case(origin)) {
            return true;
        }
        let Some((_, authority)) = origin.split_once("://") else {
            return false;
        };
        let first = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).and_then(|v| v.split(',').next()).map(str::trim);
        [first(header::HOST.as_str()), first("x-forwarded-host")].into_iter().flatten().any(|h| h.eq_ignore_ascii_case(authority))
    }

    fn cookie(&self, headers: &HeaderMap, token: &str, max_age: Duration) -> HeaderValue {
        let https = self.inner.cfg.secure_cookie || headers.get("x-forwarded-proto").is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"https"));
        let secure = if https { "; Secure" } else { "" };
        HeaderValue::from_str(&format!("{COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{secure}", max_age.as_secs())).expect("a cookie of token characters")
    }
}

fn still_valid(users: &BTreeMap<String, UserEntry>, s: &Session) -> bool {
    match s.identity.method {
        AuthMethod::Password => users.get(&s.identity.user).is_some_and(|u| !u.disabled && u.password == s.credential),
    }
}

fn status_of(s: &Session, now: Instant) -> AuthSession {
    let left = s.deadline().saturating_duration_since(now);
    let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default() + left;
    AuthSession { user: s.identity.user.clone(), method: s.identity.method, expires_at: wall.as_millis() as u64 }
}

fn write_private(path: &Path, text: &str) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    f.write_all(text.as_bytes())
}

fn warn_if_readable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o077 != 0) {
        eprintln!("auth: warning: {} is readable by others (chmod 600 it)", path.display());
    }
}

/// The session token in the request's `Cookie` headers, if any.
pub fn token_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == COOKIE)
        .map(|(_, v)| v.to_string())
        .filter(|v| !v.is_empty())
}

// ---------------------------------------------------------------- HTTP

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn is_websocket(req: &Request) -> bool {
    req.headers().get(header::UPGRADE).is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"))
}

/// The middleware on `/api`: Origin check for changes and sockets, then a
/// session for everything but [`PUBLIC`]. Changes and sockets are logged
/// with the user who made or opened them.
pub async fn guard(State(auth): State<Auth>, mut req: Request, next: Next) -> Response {
    let safe = matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    let socket = is_websocket(&req);
    if (!safe || socket) && !auth.origin_allowed(req.headers()) {
        return error(StatusCode::FORBIDDEN, "cross-site request refused");
    }
    if PUBLIC.contains(&req.uri().path()) {
        return next.run(req).await;
    }
    let Some((session, _)) = token_of(req.headers()).and_then(|t| auth.touch(&t)) else {
        return error(StatusCode::UNAUTHORIZED, "login required");
    };
    if !safe || socket {
        let what = req.uri().path_and_query().map_or(req.uri().path(), |p| p.as_str());
        eprintln!("audit: {} {} {what}", session.identity.user, req.method());
    }
    req.extensions_mut().insert(session);
    next.run(req).await
}

fn methods() -> Vec<AuthMethod> {
    vec![AuthMethod::Password]
}

/// `GET /api/auth/session`: always 200, with the session or `null`.
pub async fn session(State(auth): State<Auth>, headers: HeaderMap) -> Json<AuthStatus> {
    let session = token_of(&headers).and_then(|t| auth.touch(&t)).map(|(_, s)| s);
    Json(AuthStatus { session, methods: methods() })
}

/// `POST /api/auth/login` `{username, password}`: 200 with the status and
/// the cookie; 401 for a wrong name or password (the same for both); 429
/// with `Retry-After` after too many failures.
pub async fn login(State(auth): State<Auth>, peer: Option<Extension<ConnectInfo<SocketAddr>>>, headers: HeaderMap, Json(req): Json<LoginRequest>) -> Response {
    let from = peer.map(|Extension(ConnectInfo(a))| a);
    let who = from.map_or("?".into(), |a| a.ip().to_string());
    match auth.login(req.username.trim(), &req.password, from).await {
        Ok((token, session)) => {
            eprintln!("auth: {} logged in from {who}", session.user);
            let cookie = auth.cookie(&headers, &token, SESSION_MAX);
            ([(header::SET_COOKIE, cookie)], Json(AuthStatus { session: Some(session), methods: methods() })).into_response()
        }
        Err(LoginError::Invalid) => {
            eprintln!("auth: failed login as {:?} from {who}", req.username.chars().take(64).collect::<String>());
            error(StatusCode::UNAUTHORIZED, "wrong user name or password")
        }
        Err(LoginError::Throttled(wait)) => {
            let secs = wait.as_secs().max(1);
            let mut res = error(StatusCode::TOO_MANY_REQUESTS, &format!("too many failed logins: try again in {secs} s"));
            res.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(secs));
            res
        }
    }
}

/// `POST /api/auth/logout`: ends the session (and its sockets) and clears the cookie.
pub async fn logout(State(auth): State<Auth>, headers: HeaderMap) -> Response {
    if let Some(user) = token_of(&headers).and_then(|t| auth.logout(&t)) {
        eprintln!("auth: {user} logged out");
    }
    let cookie = auth.cookie(&headers, "", Duration::ZERO);
    ([(header::SET_COOKIE, cookie)], Json(json!({ "ok": true }))).into_response()
}

// ---------------------------------------------------------------- CLI

/// `server user passwd <name> | delete <name> | list`: manages the users
/// file. `passwd` creates the user or sets a new password, read from the
/// terminal (twice) or one line of stdin.
pub fn cli(args: &[String]) -> Result<(), String> {
    let cfg = AuthConfig::from_env();
    let path = &cfg.users;
    let load = || UsersFile::read(path).map(Option::unwrap_or_default).map_err(|e| e.to_string());
    let usage = "usage: server user passwd <name> | delete <name> | list";
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["list"] => {
            for (name, u) in load()?.users {
                println!("{name}{}", if u.disabled { " (disabled)" } else { "" });
            }
            Ok(())
        }
        ["passwd", name] => {
            if !valid_username(name) {
                return Err(format!("{name:?} is not a valid user name (letters, digits and ._@-, at most 64)"));
            }
            let mut file = load()?;
            let password = read_new_password()?;
            let entry = file.users.entry(name.to_string()).or_insert_with(|| UserEntry { displayname: name.to_string(), password: String::new(), disabled: false, extra: BTreeMap::new() });
            let created = entry.password.is_empty();
            entry.password = hash_password(&password);
            file.write(path).map_err(|e| e.to_string())?;
            println!("{} {name} in {}{}", if created { "created" } else { "new password for" }, path.display(), if created { "" } else { " (their sessions end)" });
            Ok(())
        }
        ["delete", name] => {
            let mut file = load()?;
            if file.users.remove(*name).is_none() {
                return Err(format!("no user {name:?} in {}", path.display()));
            }
            file.write(path).map_err(|e| e.to_string())?;
            println!("deleted {name} (their sessions end)");
            Ok(())
        }
        _ => Err(usage.into()),
    }
}

fn read_new_password() -> Result<String, String> {
    use std::io::IsTerminal;
    let password = if std::io::stdin().is_terminal() {
        let first = rpassword::prompt_password("new password: ").map_err(|e| e.to_string())?;
        let again = rpassword::prompt_password("again: ").map_err(|e| e.to_string())?;
        if first != again {
            return Err("the passwords differ".into());
        }
        first
    } else {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).map_err(|e| e.to_string())?;
        line.trim_end_matches(['\r', '\n']).to_string()
    };
    if password.chars().count() < MIN_PASSWORD || password.len() > MAX_PASSWORD {
        return Err(format!("a password needs {MIN_PASSWORD} to {MAX_PASSWORD} characters"));
    }
    Ok(password)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::{get, post};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    pub fn scratch(name: &str) -> PathBuf {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("homelab-auth-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An `Auth` whose users file holds `users` (name, password).
    pub fn auth_with(name: &str, users: &[(&str, &str)]) -> Auth {
        let path = scratch(name).join("users.yml");
        let mut file = UsersFile::default();
        for (u, p) in users {
            file.users.insert(u.to_string(), UserEntry { displayname: String::new(), password: hash_password(p), disabled: false, extra: BTreeMap::new() });
        }
        file.write(&path).unwrap();
        Auth::new(AuthConfig { users: path, allowed_origins: vec!["https://homelab.example".into()], secure_cookie: false })
    }

    /// An `Auth` with one user, `tester`, and a live session of theirs: its cookie token.
    pub fn logged_in() -> (Auth, String) {
        let auth = auth_with("session", &[("tester", "tester-password")]);
        let credential = auth.inner.users.current()["tester"].password.clone();
        let (token, _) = auth.open_session(Identity { user: "tester".into(), method: AuthMethod::Password }, credential);
        (auth, token)
    }

    fn app(auth: Auth) -> Router {
        Router::new()
            .route("/api/auth/session", get(session))
            .route("/api/auth/login", post(login))
            .route("/api/auth/logout", post(logout))
            .with_state(auth.clone())
            .route("/api/thing", get(|Extension(s): Extension<SessionGuard>| async move { s.identity.user }).post(|| async { "changed" }))
            .layer(axum::middleware::from_fn_with_state(auth, guard))
    }

    async fn call(app: &Router, method: &str, path: &str, headers: &[(&str, &str)], body: Option<serde_json::Value>) -> (StatusCode, HeaderMap, String) {
        let mut req = Request::builder().method(method).uri(path);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = match body {
            Some(b) => req.header("content-type", "application/json").body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let (status, headers) = (res.status(), res.headers().clone());
        (status, headers, String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap())
    }

    fn cookie_of(headers: &HeaderMap) -> String {
        headers.get(header::SET_COOKIE).unwrap().to_str().unwrap().split(';').next().unwrap().to_string()
    }

    #[test]
    fn passwords_hash_to_argon2id_and_verify() {
        let phc = hash_password("correct horse");
        assert!(phc.starts_with("$argon2id$v=19$"), "{phc}");
        assert!(verify_password(&phc, "correct horse"));
        assert!(!verify_password(&phc, "correct horsE"));
        assert!(!verify_password("not a hash", "x"));
        assert_ne!(hash_password("same"), hash_password("same"), "salted");
        let generated = generate_password();
        assert_eq!(generated.len(), 24);
        assert_ne!(generated, generate_password());
    }

    #[test]
    fn the_users_file_is_authelias_shape_and_keeps_what_it_does_not_use() {
        let path = scratch("shape").join("users.yml");
        std::fs::write(
            &path,
            "users:\n  john:\n    displayname: John Doe\n    password: \"$argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHQ$aGFzaGhhc2hoYXNo\"\n    email: john@example.com\n    groups: [admins, dev]\n  old:\n    password: x\n    disabled: true\n",
        )
        .unwrap();
        let file = UsersFile::read(&path).unwrap().unwrap();
        assert_eq!(file.users["john"].displayname, "John Doe");
        assert!(file.users["old"].disabled);
        file.write(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("email: john@example.com") && text.contains("- admins"), "{text}");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(UsersFile::read(&path.with_file_name("none.yml")).unwrap().is_none());
    }

    #[test]
    fn user_names_are_plain() {
        for ok in ["admin", "john.doe", "a@b.c", "x_1-2"] {
            assert!(valid_username(ok), "{ok}");
        }
        for bad in ["", ".hidden", "-x", "a b", "a/b", "ä", &"a".repeat(65)] {
            assert!(!valid_username(bad), "{bad}");
        }
    }

    #[test]
    fn the_first_start_creates_an_admin_with_a_generated_password() {
        let dir = scratch("bootstrap");
        let auth = Auth::new(AuthConfig { users: dir.join("users.yml"), allowed_origins: vec![], secure_cookie: false });
        let note = auth.bootstrap().unwrap().expect("created");
        let password = std::fs::read_to_string(&note).unwrap().trim().to_string();
        let file = UsersFile::read(auth.users_file()).unwrap().unwrap();
        assert!(verify_password(&file.users["admin"].password, &password));
        assert_eq!(auth.bootstrap().unwrap(), None, "only the first time");
    }

    #[test]
    fn failures_are_throttled_with_a_doubling_wait_and_success_clears_them() {
        let mut t = Throttle::default();
        let t0 = Instant::now();
        for _ in 0..FREE_TRIES {
            assert_eq!(t.wait("k", t0), None);
            t.attempt("k", t0);
        }
        assert_eq!(t.wait("k", t0), Some(Duration::from_secs(1)));
        assert_eq!(t.wait("k", t0 + Duration::from_secs(1)), None);
        t.attempt("k", t0 + Duration::from_secs(1));
        assert_eq!(t.wait("k", t0 + Duration::from_secs(1)), Some(Duration::from_secs(2)));
        for _ in 0..20 {
            t.attempt("k", t0 + Duration::from_secs(1));
        }
        assert_eq!(t.wait("k", t0 + Duration::from_secs(1)), Some(MAX_WAIT));
        assert_eq!(t.wait("other", t0), None);
        t.clear("k");
        assert_eq!(t.wait("k", t0), None);
        t.attempt("old", t0);
        for _ in 0..10 {
            t.attempt("old", t0);
        }
        assert_eq!(t.wait("old", t0 + FORGET_FAILURES), None, "forgotten");
    }

    #[test]
    fn origins_must_be_the_servers_own_or_configured() {
        let auth = auth_with("origin", &[]);
        let h = |pairs: &[(&str, &str)]| {
            let mut m = HeaderMap::new();
            for (k, v) in pairs {
                m.insert(axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
            }
            m
        };
        assert!(auth.origin_allowed(&h(&[("host", "127.0.0.1:8080")])), "no Origin: not a browser");
        assert!(auth.origin_allowed(&h(&[("host", "127.0.0.1:8080"), ("origin", "http://127.0.0.1:8080")])));
        assert!(!auth.origin_allowed(&h(&[("host", "127.0.0.1:8080"), ("origin", "http://127.0.0.1:3000")])), "another port is another origin");
        assert!(!auth.origin_allowed(&h(&[("host", "127.0.0.1:8080"), ("origin", "null")])));
        assert!(auth.origin_allowed(&h(&[("host", "10.0.0.1:8080"), ("origin", "https://homelab.example")])), "configured");
        assert!(auth.origin_allowed(&h(&[("host", "backend:8080"), ("x-forwarded-host", "lab.home"), ("origin", "https://lab.home")])));
    }

    #[tokio::test]
    async fn the_api_needs_a_session_and_logout_ends_it() {
        let auth = auth_with("flow", &[("alice", "wonderland1")]);
        let app = app(auth.clone());
        assert_eq!(call(&app, "GET", "/api/thing", &[], None).await.0, StatusCode::UNAUTHORIZED);
        let (status, _, body) = call(&app, "GET", "/api/auth/session", &[], None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({ "session": null, "methods": [{ "kind": "password" }] }));

        let (status, _, body) = call(&app, "POST", "/api/auth/login", &[], Some(json!({ "username": "alice", "password": "nope" }))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        let (status, _, body2) = call(&app, "POST", "/api/auth/login", &[], Some(json!({ "username": "bob", "password": "nope" }))).await;
        assert_eq!((status, &body2), (StatusCode::UNAUTHORIZED, &body), "an unknown user reads as a wrong password");

        let (status, headers, body) = call(&app, "POST", "/api/auth/login", &[], Some(json!({ "username": "alice", "password": "wonderland1" }))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let set = headers.get(header::SET_COOKIE).unwrap().to_str().unwrap().to_string();
        assert!(set.contains("HttpOnly") && set.contains("SameSite=Strict") && !set.contains("Secure"), "{set}");
        let cookie = cookie_of(&headers);
        let (status, _, user) = call(&app, "GET", "/api/thing", &[("cookie", &cookie)], None).await;
        assert_eq!((status, user.as_str()), (StatusCode::OK, "alice"));
        let (_, _, body) = call(&app, "GET", "/api/auth/session", &[("cookie", &format!("other=1; {cookie}"))], None).await;
        assert!(body.contains("\"user\":\"alice\""), "{body}");

        // A socket's guard wakes when the session ends.
        let (mut guard, _) = auth.touch(cookie.split_once('=').unwrap().1).unwrap();
        let ended = tokio::spawn(async move { guard.ended().await });
        let (status, headers, _) = call(&app, "POST", "/api/auth/logout", &[("cookie", &cookie)], None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers.get(header::SET_COOKIE).unwrap().to_str().unwrap().contains("Max-Age=0"));
        tokio::time::timeout(Duration::from_secs(2), ended).await.expect("the socket's guard woke").unwrap();
        assert_eq!(call(&app, "GET", "/api/thing", &[("cookie", &cookie)], None).await.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cross_site_changes_and_sockets_are_refused_even_with_a_session() {
        let auth = auth_with("csrf", &[("alice", "wonderland1")]);
        let app = app(auth.clone());
        let (token, _) = auth.open_session(Identity { user: "alice".into(), method: AuthMethod::Password }, auth.inner.users.current()["alice"].password.clone());
        let cookie = format!("{COOKIE}={token}");
        let evil = [("cookie", cookie.as_str()), ("host", "127.0.0.1:8080"), ("origin", "http://127.0.0.1:9999")];
        assert_eq!(call(&app, "POST", "/api/thing", &evil, None).await.0, StatusCode::FORBIDDEN);
        let ws = [evil[0], evil[1], evil[2], ("upgrade", "websocket")];
        assert_eq!(call(&app, "GET", "/api/thing", &ws, None).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(&app, "GET", "/api/thing", &evil, None).await.0, StatusCode::OK, "a plain read: the browser keeps its answer from the other site");
        let own = [("cookie", cookie.as_str()), ("host", "127.0.0.1:8080"), ("origin", "http://127.0.0.1:8080")];
        assert_eq!(call(&app, "POST", "/api/thing", &own, None).await.0, StatusCode::OK);
        let login = call(&app, "POST", "/api/auth/login", &evil[1..], Some(json!({ "username": "alice", "password": "wonderland1" }))).await;
        assert_eq!(login.0, StatusCode::FORBIDDEN, "no logging a victim in from another site");
    }

    #[tokio::test]
    async fn a_new_password_or_a_removed_user_ends_their_sessions() {
        let auth = auth_with("change", &[("alice", "wonderland1"), ("bob", "builder123")]);
        let (alice, _) = auth.login("alice", "wonderland1", None).await.unwrap();
        let (bob, _) = auth.login("bob", "builder123", None).await.unwrap();
        assert!(auth.touch(&alice).is_some() && auth.touch(&bob).is_some());
        let path = auth.users_file().to_path_buf();
        let mut file = UsersFile::read(&path).unwrap().unwrap();
        file.users.get_mut("alice").unwrap().password = hash_password("new-password");
        file.users.remove("bob");
        std::thread::sleep(Duration::from_millis(20));
        file.write(&path).unwrap();
        assert!(auth.touch(&alice).is_none(), "re-passworded");
        assert!(auth.touch(&bob).is_none(), "removed");
        assert_eq!(auth.login("alice", "wonderland1", None).await.unwrap_err(), LoginError::Invalid);
        assert!(auth.login("alice", "new-password", None).await.is_ok());
        let mut file = UsersFile::read(&path).unwrap().unwrap();
        file.users.get_mut("alice").unwrap().disabled = true;
        file.write(&path).unwrap();
        assert_eq!(auth.login("alice", "new-password", None).await.unwrap_err(), LoginError::Invalid, "disabled");
    }

    #[tokio::test]
    async fn repeated_failures_are_answered_429_with_retry_after() {
        let auth = auth_with("throttle", &[("alice", "wonderland1")]);
        let app = app(auth);
        let bad = json!({ "username": "alice", "password": "guess" });
        for _ in 0..FREE_TRIES {
            assert_eq!(call(&app, "POST", "/api/auth/login", &[], Some(bad.clone())).await.0, StatusCode::UNAUTHORIZED);
        }
        let (status, headers, body) = call(&app, "POST", "/api/auth/login", &[], Some(json!({ "username": "alice", "password": "wonderland1" }))).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "even the right password waits: {body}");
        assert_eq!(headers.get(header::RETRY_AFTER).unwrap(), "1");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let (status, _, body) = call(&app, "POST", "/api/auth/login", &[], Some(json!({ "username": "alice", "password": "wonderland1" }))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    #[test]
    fn sessions_expire_when_idle_or_too_old() {
        let auth = auth_with("expiry", &[("alice", "wonderland1")]);
        let cred = auth.inner.users.current()["alice"].password.clone();
        let (token, _) = auth.open_session(Identity { user: "alice".into(), method: AuthMethod::Password }, cred);
        let now = Instant::now();
        auth.sweep(now);
        assert!(auth.touch(&token).is_some());
        auth.sweep(now + SESSION_IDLE + Duration::from_secs(1));
        assert!(auth.touch(&token).is_none(), "idle too long");
    }
}
