# Homelab Cluster Manager — Login

Date: 2026-11-12
Status: Built (user name and password). Verified on the VM lab (ms01-01..03 with K3s pre-installed,
router 10.0.0.1): e2e K15, plus K10, K13 and K14 running through a session.
Builds on: `2026-11-02-ui-workload-actions-design.md` (the changes that made a login necessary).

## 1. Intent

The UI's server can apply YAML, delete workloads, open shells in containers and on the hosts,
hand out the admin kubeconfig and power off machines. Until now anyone who could reach its port
could do all of that. This puts a login in front of the whole API: user name and password now,
shaped so an OIDC provider (Authelia first) can be added without changing the session layer, the
guard or the UI's handling of sessions.

### Not in scope

- Roles. Every user can do everything the UI does; a read-only role is a follow-up (§8).
- TLS. The server still speaks plain HTTP on loopback by default; reaching it from elsewhere
  means an SSH tunnel or a TLS reverse proxy.
- Sessions surviving a server restart (they live in memory; a restart logs everyone out).

## 2. Users

A YAML file, by default `$LAB_STATE/users.yml` (`/var/lib/homelab-lab/users.yml`, mode 0600,
`HOMELAB_USERS` moves it), in the shape of **Authelia's file users database**:

```yaml
users:
  admin:
    displayname: Administrator
    password: $argon2id$v=19$m=19456,t=2,p=1$…
    disabled: false        # optional
    email: …               # optional, kept but unused (as are groups)
```

Passwords are Argon2id PHC strings (the `argon2` crate's defaults, OWASP's minimum: 19 MiB, 2
passes). Any Argon2 hash verifies, so a file written by `authelia crypto hash generate argon2`
works here, and this file can seed Authelia's own when the cluster moves to OIDC.

- **First start:** with no file, the server creates it with one user, `admin`, and a generated
  24-character password written to `$LAB_STATE/initial-admin-password` (0600). The path is
  logged, the password is not. `./lab.sh password` prints it. Giving `admin` a new password
  (or deleting it) with `./lab.sh user` removes that file, so it never hands out a stale one.
- **Managing users:** `./lab.sh user passwd <name>` (create, or set a new password: read twice
  from the terminal, or one line of stdin for scripts), `./lab.sh user delete <name>`,
  `./lab.sh user list`. They rewrite the file atomically, so a running server needs no restart.
- **Changes take effect at once.** The server re-reads the file when its modification time or
  size changes. A session belongs to the password hash it logged in against: a new password,
  removing a user or `disabled: true` ends that user's sessions on their next request, and the
  once-a-minute sweep ends their WebSockets even if they make none.
- **A broken file fails closed:** unreadable or invalid YAML means nobody, logged loudly.

## 3. Sessions

`POST /api/auth/login {username, password}` checks the password and sets
`homelab_session=<32 random bytes, base64url>; Path=/; HttpOnly; SameSite=Strict; Max-Age=7d`,
plus `Secure` behind a proxy that says `X-Forwarded-Proto: https` (or with `COOKIE_SECURE=1`).
The server keeps its sessions under a BLAKE2s hash of the token, never the token itself.

- A session ends after **12 h unused** or **7 days** however busy, on logout
  (`POST /api/auth/logout`), or when its user changes (§2).
- `GET /api/auth/session` always answers 200: `{session: {user, method, expires_at} | null,
  methods: [{kind: "password"}]}`. `methods` is what the login page offers.
- A wrong user name and a wrong password are the same 401 with the same message, and an unknown
  user is checked against a dummy hash so the timing does not tell them apart either.
- **Throttling:** each try is counted, before its check, against the user name and the client
  address. After 5 failures the next try waits 1 s, then 2, 4, … up to 5 min (429 with
  `Retry-After`); success clears the count, and 15 min without a failure forgets it. At most 4
  password checks run at once. Behind a reverse proxy every client shares the proxy's address,
  so the per-user count is the one that matters there. Someone guessing at `admin` can keep the
  real admin waiting up to 5 min between tries; for a homelab that is the right trade over
  unlimited guesses.

## 4. The guard

One axum middleware on every `/api` route:

1. **Origin.** A state-changing request (not GET/HEAD/OPTIONS) or a WebSocket upgrade whose
   `Origin` is not the server's own (`Host`, or `X-Forwarded-Host`, or one of
   `ALLOWED_ORIGINS`) is 403, logged in or not. `SameSite=Strict` alone is not enough: a page on
   another *port* of the same host is the same site, and WebSockets are not covered by CORS.
   Requests without `Origin` (curl, scripts, the e2e) are not a browser being misused and pass.
   This also covers the login itself (no logging a victim into the attacker's account).
2. **Session.** Every path but `/api/auth/{session,login,logout}` needs a live session, else
   401 `{"error": "login required"}`. The session (who, and how they logged in) goes into the
   request's extensions.
3. **Audit.** Each change and each socket opened is logged with its user:
   `audit: admin POST /api/kube`, `audit: admin GET /api/kube/exec/ws?namespace=…`.

The built UI's static files are served without a session: the page has to load to show its
login, and holds nothing secret.

**Sockets end with their session.** The live view, log follow, container shell and host
terminal each wait on their session's end alongside their own traffic, and close with code
**4401** ("session ended") on logout, expiry or a user change. The shell's container exec and
the host's SSH process are torn down as on any other close.

## 5. The UI

- `Root` asks `GET /api/auth/session` first: the login page if nobody is logged in, else the
  workspace and its live view. If that first request gets no answer (server down or
  restarting, a proxy error), it says the server is not answering and asks again every 5 s,
  with a "Try now" button, instead of staying on "connecting…".
- **A session that ends under a running UI** (a 401 from any API call, a 4401 close, or a drop
  after which `/api/auth/session` says nobody) does not throw the page away: a login dialog covers
  the workspace, with the user name filled in, so a half-written YAML or an open drawer survives.
  Logging in again reconnects the live view (a fresh snapshot). "Log in as someone else" goes to
  the login page.
- The top bar shows who is logged in, with **Log out**.
- Open log follows and terminals say "logged out" when their socket closes with 4401.

## 6. Adding OIDC (Authelia)

What exists for it:

- `Identity { user, method }` on every session, and `Auth::open_session(identity, credential)`,
  which any login method calls once it has proved who someone is.
- `AuthMethod`, the list the login page renders. OIDC adds a variant, e.g.
  `Oidc { name: "Authelia", start: "/api/auth/oidc/start" }`, shown as a button.
- `still_valid` per method: password sessions follow the users file; OIDC sessions would follow
  the ID token's expiry (and optionally a refresh).

What to build:

1. Config: issuer URL, client ID and secret, redirect URL (`https://<ui>/api/auth/oidc/callback`),
   scopes `openid profile groups`, and which claim names the user (`preferred_username`).
   Discovery from `<issuer>/.well-known/openid-configuration`.
2. `GET /api/auth/oidc/start`: authorization-code flow with PKCE (S256), `state` and `nonce` in a
   short-lived, HttpOnly cookie; redirect to Authelia.
3. `GET /api/auth/oidc/callback`: check `state`, exchange the code, verify the ID token (signature
   against the JWKS, `iss`, `aud`, `exp`, `nonce`), then `open_session` and redirect to `/`.
   This is a top-level navigation coming *from* Authelia: the `state` cookie must be
   `SameSite=Lax` (Strict would not be sent); the session cookie stays Strict.
4. Optionally map Authelia groups to roles (§8) and keep the password login as a break-glass for
   when Authelia is down (it may well run on this cluster).

An alternative, Authelia's **forward auth** (the proxy authenticates and passes `Remote-User`),
is simpler to wire but means trusting a header, which is only safe when the server is reachable
solely through that proxy. OIDC keeps the server in charge of its own sessions, so it is the
planned route.

## 7. Tests

- Rust (`crates/server/src/auth.rs`): hashing and verification; the users file round-trips
  Authelia's fields and is written 0600; first start creates `admin`; throttle timings; origin
  rules; the API needs a session and logout ends it (and wakes a socket's guard); cross-site
  changes and sockets are 403 with a valid session, and so is a cross-site login; a new
  password, a removed or a disabled user ends sessions; 429 with `Retry-After`; idle expiry.
  The existing API tests run as a logged-in user.
- UI (`auth.test.ts`): the login state machine (login page, dialog over the workspace, reconnect
  only on a new login) and error messages.
- e2e K15 on the lab: the UI page loads without a session; API calls (metrics, kubeconfig, a lab
  fault, a workload change, an op, the console, a forged cookie) are 401 and the VM's power does
  not change; the live view's socket does not open; a wrong password is 401; a cross-origin
  change and WebSocket with a valid cookie are 403; a second session's socket closes with 4401
  on logout and its cookie is refused while the first works on; the 6th wrong guess is 429 and the
  right password works after `Retry-After`. The rest of the e2e logs in once and carries the
  cookie on every request and socket.

## 8. Next

- Roles: `viewer` (reads, metrics, logs) and `admin` (changes, shells, kubeconfig, lab), from a
  `groups` entry in the users file — Authelia's field, and later its OIDC `groups` claim.
- OIDC with Authelia (§6).
- Persisting sessions across restarts, if restarts logging everyone out becomes a nuisance.
