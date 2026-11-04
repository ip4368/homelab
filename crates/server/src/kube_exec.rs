//! A shell in a container, for the UI's terminal: the browser's WebSocket
//! bridged to Kubernetes' exec WebSocket (docs/plans/2026-11-04-workload-insight-logs-terminal-design.md §6).
//! The browser never sees Kubernetes' channels: it sends and gets terminal
//! bytes in binary frames, and JSON in text frames for the rest, so a VM's
//! serial console can use the same terminal.

use crate::kube::{Access, SendError};
use crate::kube_action::{valid_name, valid_namespace, PROTECTED_NAMESPACES};
use axum::extract::ws::{Message as BrowserMsg, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_tungstenite::tungstenite::Message as KubeMsg;

/// Exec's subprotocols, newest first: v5 adds closing stdin; K3s v1.36 speaks both.
pub const EXEC_PROTOCOLS: [&str; 2] = ["v5.channel.k8s.io", "v4.channel.k8s.io"];
/// The most sessions (terminals and consoles) open at once.
pub const MAX_SESSIONS: usize = 16;
/// bash where the image has it, else sh.
const SHELL: &str = "command -v bash >/dev/null && exec bash || exec sh";

/// `GET /api/kube/exec`'s query.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ExecQuery {
    pub namespace: String,
    pub pod: String,
    /// Needed when the pod has several containers.
    pub container: Option<String>,
}

impl ExecQuery {
    /// Why the server will not open it: a name that is not a name, or a
    /// protected namespace (a shell there is a write in all but name).
    pub fn refusal(&self) -> Option<String> {
        if !valid_namespace(&self.namespace) || !valid_name(&self.pod) || self.container.as_deref().is_some_and(|c| !valid_namespace(c)) {
            return Some(format!("not a pod or container name: {}/{} {}", self.namespace, self.pod, self.container.as_deref().unwrap_or("")));
        }
        if PROTECTED_NAMESPACES.contains(&self.namespace.as_str()) {
            return Some(format!("{} is kept by homelabd: no shell from here; use kubectl exec if you must", self.namespace));
        }
        None
    }

    /// Its path in the Kubernetes API: a shell with a terminal (stderr comes on stdout then).
    pub fn path(&self) -> String {
        let container = self.container.as_ref().map(|c| format!("&container={c}")).unwrap_or_default();
        let command: String = ["sh", "-c", SHELL].iter().map(|a| format!("&command={}", url_encode(a))).collect();
        format!("/api/v1/namespaces/{}/pods/{}/exec?stdin=true&stdout=true&tty=true{container}{command}", self.namespace, self.pod)
    }

    pub fn describe(&self) -> String {
        format!("{}/{}{}", self.namespace, self.pod, self.container.as_ref().map(|c| format!("/{c}")).unwrap_or_default())
    }
}

/// Percent-encodes everything but unreserved characters.
fn url_encode(s: &str) -> String {
    s.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") }).collect()
}

/// What the browser sends.
#[derive(Debug, Clone, PartialEq)]
pub enum FromBrowser {
    Bytes(Vec<u8>),
    Resize { cols: u16, rows: u16 },
}

/// What the browser is sent.
#[derive(Debug, Clone, PartialEq)]
pub enum ToBrowser {
    Bytes(Vec<u8>),
    /// The command ended with this code; the session ends after it.
    Exit(i32),
    /// It could not start, or Kubernetes ended it, in its words; the session ends after it.
    Error(String),
}

impl ToBrowser {
    fn message(&self) -> BrowserMsg {
        match self {
            ToBrowser::Bytes(b) => BrowserMsg::Binary(b.clone().into()),
            ToBrowser::Exit(code) => BrowserMsg::Text(json!({ "exit": { "code": code } }).to_string().into()),
            ToBrowser::Error(e) => BrowserMsg::Text(json!({ "error": e }).to_string().into()),
        }
    }

    fn ends(&self) -> bool {
        !matches!(self, ToBrowser::Bytes(_))
    }
}

/// A browser frame: bytes typed, or `{"resize":{"cols":…,"rows":…}}`.
pub fn from_browser(msg: &BrowserMsg) -> Option<FromBrowser> {
    match msg {
        BrowserMsg::Binary(b) => Some(FromBrowser::Bytes(b.to_vec())),
        BrowserMsg::Text(t) => {
            let v: Value = serde_json::from_str(t.as_str()).ok()?;
            let r = &v["resize"];
            Some(FromBrowser::Resize { cols: r["cols"].as_u64()? as u16, rows: r["rows"].as_u64()? as u16 })
        }
        _ => None,
    }
}

/// Exec's frame for what the browser sent: stdin on channel 0, a resize on 4.
pub fn exec_frame(input: &FromBrowser) -> Vec<u8> {
    match input {
        FromBrowser::Bytes(b) => [&[0u8][..], b].concat(),
        FromBrowser::Resize { cols, rows } => [&[4u8][..], json!({ "Width": cols, "Height": rows }).to_string().as_bytes()].concat(),
    }
}

/// What an exec frame means for the browser: output on 1 (and 2), the end on 3.
/// Empty frames (each channel opens with one) mean nothing.
pub fn from_exec(frame: &[u8]) -> Option<ToBrowser> {
    let (&channel, data) = frame.split_first()?;
    match channel {
        1 | 2 if !data.is_empty() => Some(ToBrowser::Bytes(data.to_vec())),
        3 => Some(exec_status(&serde_json::from_slice(data).unwrap_or(Value::Null))),
        _ => None,
    }
}

/// The end of a command, from the `Status` exec sends on channel 3.
pub fn exec_status(status: &Value) -> ToBrowser {
    if status["status"] == "Success" {
        return ToBrowser::Exit(0);
    }
    if status["reason"] == "NonZeroExitCode" {
        let code = status["details"]["causes"].as_array().into_iter().flatten().find(|c| c["reason"] == "ExitCode").and_then(|c| c["message"].as_str()?.parse().ok());
        if let Some(code) = code {
            return ToBrowser::Exit(code);
        }
    }
    ToBrowser::Error(status["message"].as_str().filter(|m| !m.is_empty()).unwrap_or("the command failed").to_string())
}

static SESSIONS: AtomicUsize = AtomicUsize::new(0);

/// A place among the [`MAX_SESSIONS`], given back when dropped.
pub struct Session(());

impl Session {
    pub fn start() -> Option<Session> {
        SESSIONS.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| (n < MAX_SESSIONS).then_some(n + 1)).ok().map(|_| Session(()))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        SESSIONS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How a protocol's frames translate: what the browser sent to the pod's
/// (or VM's) frame, and back.
pub struct Translate {
    pub to_kube: fn(&FromBrowser) -> Option<Vec<u8>>,
    pub from_kube: fn(&[u8]) -> Option<ToBrowser>,
}

pub const EXEC: Translate = Translate { to_kube: |i| Some(exec_frame(i)), from_kube: from_exec };

/// How long to wait for the browser's side of the close handshake.
const CLOSE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Sends `e` to the browser and closes it.
pub async fn refuse(browser: WebSocket, e: &str) {
    let (mut tx, rx) = browser.split();
    let _ = tx.send(ToBrowser::Error(e.to_string()).message()).await;
    close_browser(tx, rx).await;
}

/// Closes the browser's socket, then reads until the browser closes too:
/// closing with what it sent still unread would reset the connection, and
/// the reset can lose the last message on its way (the error, the exit code).
async fn close_browser(mut tx: SplitSink<WebSocket, BrowserMsg>, mut rx: SplitStream<WebSocket>) {
    let _ = tx.close().await;
    let _ = tokio::time::timeout(CLOSE_WAIT, async {
        while let Some(Ok(m)) = rx.next().await {
            if matches!(m, BrowserMsg::Close(_)) {
                break;
            }
        }
    })
    .await;
}

/// Opens Kubernetes' socket at `path` and bridges `browser` to it until
/// either side ends; `what` names it in the server's log.
pub async fn run(browser: WebSocket, access: &Access, path: &str, protocols: &[&str], t: &Translate, what: &str) {
    let Some(_session) = Session::start() else {
        return refuse(browser, &format!("{MAX_SESSIONS} terminals are open already: close one first")).await;
    };
    let kube = match access.connect_ws(path, protocols).await {
        Ok(k) => k,
        Err(e) => {
            let why = match e {
                SendError::Refused { message, .. } | SendError::Unreachable(message) => message,
            };
            return refuse(browser, &why).await;
        }
    };
    eprintln!("terminal: {what} opened");
    bridge(browser, kube, t).await;
    eprintln!("terminal: {what} closed");
}

async fn bridge<S>(browser: WebSocket, kube: tokio_tungstenite::WebSocketStream<S>, t: &Translate)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut b_tx, mut b_rx) = browser.split();
    let (mut k_tx, mut k_rx) = kube.split();
    loop {
        tokio::select! {
            m = b_rx.next() => match m {
                Some(Ok(BrowserMsg::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(m)) => {
                    if let Some(frame) = from_browser(&m).and_then(|i| (t.to_kube)(&i))
                        && k_tx.send(KubeMsg::Binary(frame.into())).await.is_err()
                    {
                        break;
                    }
                }
            },
            m = k_rx.next() => match m {
                Some(Ok(KubeMsg::Binary(frame))) => {
                    if let Some(out) = (t.from_kube)(&frame)
                        && (b_tx.send(out.message()).await.is_err() || out.ends())
                    {
                        break;
                    }
                }
                Some(Ok(KubeMsg::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
    let _ = k_tx.close().await;
    close_browser(b_tx, b_rx).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(namespace: &str, pod: &str, container: Option<&str>) -> ExecQuery {
        ExecQuery { namespace: namespace.into(), pod: pod.into(), container: container.map(String::from) }
    }

    #[test]
    fn a_shell_is_bash_or_sh_with_a_terminal() {
        assert_eq!(
            q("demo", "web-1", Some("nginx")).path(),
            "/api/v1/namespaces/demo/pods/web-1/exec?stdin=true&stdout=true&tty=true&container=nginx&command=sh&command=-c&command=command%20-v%20bash%20%3E%2Fdev%2Fnull%20%26%26%20exec%20bash%20%7C%7C%20exec%20sh"
        );
        assert_eq!(q("demo", "web-1", None).describe(), "demo/web-1");
    }

    #[test]
    fn protected_namespaces_and_bad_names_are_refused() {
        assert_eq!(q("demo", "web-1", Some("nginx")).refusal(), None);
        assert!(q("kube-system", "coredns-1", None).refusal().unwrap().contains("kubectl"));
        assert!(q("longhorn-system", "x", None).refusal().is_some());
        assert!(q("demo", "../x", None).refusal().is_some());
        assert!(q("demo", "web-1", Some("a&command=rm")).refusal().is_some());
    }

    #[test]
    fn browser_frames_become_stdin_and_resizes() {
        let typed = from_browser(&BrowserMsg::Binary(b"ls\r".to_vec().into())).unwrap();
        assert_eq!(exec_frame(&typed), b"\x00ls\r");
        let resize = from_browser(&BrowserMsg::Text(r#"{"resize":{"cols":120,"rows":40}}"#.into())).unwrap();
        assert_eq!(resize, FromBrowser::Resize { cols: 120, rows: 40 });
        assert_eq!(exec_frame(&resize), br#"{"Height":40,"Width":120}"#.iter().fold(vec![4u8], |mut v, b| { v.push(*b); v }));
        assert_eq!(from_browser(&BrowserMsg::Text("hello".into())), None);
    }

    #[test]
    fn exec_frames_become_output_and_an_end() {
        assert_eq!(from_exec(b"\x01hi"), Some(ToBrowser::Bytes(b"hi".to_vec())));
        assert_eq!(from_exec(b"\x02oops"), Some(ToBrowser::Bytes(b"oops".to_vec())));
        assert_eq!(from_exec(b"\x01"), None, "each channel opens with an empty frame");
        assert_eq!(from_exec(b""), None);
        assert_eq!(from_exec(br#"{"status":"Success"}"#.iter().fold(vec![3u8], |mut v, b| { v.push(*b); v }).as_slice()), Some(ToBrowser::Exit(0)));
        let code = json!({ "status": "Failure", "reason": "NonZeroExitCode", "message": "command terminated with non-zero exit code: 3", "details": { "causes": [{ "reason": "ExitCode", "message": "3" }] } });
        assert_eq!(exec_status(&code), ToBrowser::Exit(3));
        let missing = json!({ "status": "Failure", "message": "exec: \"sh\": executable file not found in $PATH" });
        assert_eq!(exec_status(&missing), ToBrowser::Error("exec: \"sh\": executable file not found in $PATH".into()));
    }

    #[test]
    fn the_browser_hears_bytes_and_json() {
        assert_eq!(ToBrowser::Exit(3).message(), BrowserMsg::Text(r#"{"exit":{"code":3}}"#.into()));
        assert_eq!(ToBrowser::Error("no".into()).message(), BrowserMsg::Text(r#"{"error":"no"}"#.into()));
        assert!(ToBrowser::Exit(0).ends() && !ToBrowser::Bytes(vec![]).ends());
    }

    #[test]
    fn sessions_are_capped() {
        let held: Vec<Session> = std::iter::from_fn(Session::start).take(MAX_SESSIONS + 1).collect();
        assert_eq!(held.len(), MAX_SESSIONS);
        drop(held);
        assert!(Session::start().is_some());
    }
}
