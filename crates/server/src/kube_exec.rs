//! A shell in a container, as `kubectl exec -it` opens one: the API
//! server's `pods/exec` over WebSocket (`v4.channel.k8s.io`), bridged to
//! the browser by the HTTP API. Exec is a write — the protected namespaces
//! are refused, as for scale/restart/delete
//! (docs/plans/2026-11-04-workload-debug-design.md §5).

use crate::kube::Access;
use crate::kube_action::{valid_name, valid_namespace, ActionError, PROTECTED_NAMESPACES};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// The shells the UI offers; an image without the chosen one gets
/// Kubernetes' own error back over the status channel.
pub const SHELLS: [&str; 3] = ["/bin/sh", "/bin/bash", "/bin/ash"];
pub const DEFAULT_SHELL: &str = "/bin/sh";

pub const CH_STDIN: u8 = 0;
pub const CH_STDOUT: u8 = 1;
pub const CH_STDERR: u8 = 2;
pub const CH_STATUS: u8 = 3;
pub const CH_RESIZE: u8 = 4;
/// Per-stream close, protocol v5 only; v4 closes by closing the socket.
#[allow(dead_code)]
pub const CH_CLOSE: u8 = 255;

/// The most one session may run.
pub const MAX_SESSION: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Where to open a shell, as the UI asks.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ExecQuery {
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub shell: Option<String>,
}

impl ExecQuery {
    pub fn shell(&self) -> &str {
        self.shell.as_deref().unwrap_or(DEFAULT_SHELL)
    }
}

/// Why the server will not open this shell, if it will not.
pub fn refusal(q: &ExecQuery) -> Option<String> {
    if !valid_namespace(&q.namespace) || !valid_name(&q.pod) {
        return Some(format!("{:?}/{:?} is not a Kubernetes namespace and pod", q.namespace, q.pod));
    }
    if let Some(c) = &q.container {
        if !valid_namespace(c) {
            return Some(format!("{c:?} is not a container name"));
        }
    }
    if PROTECTED_NAMESPACES.contains(&q.namespace.as_str()) {
        return Some(format!("{} belongs to K3s and Longhorn, which homelabd installs and supervises: use kubectl exec there", q.namespace));
    }
    if !SHELLS.contains(&q.shell()) {
        return Some(format!("{:?} is not a shell this page runs ({}); use kubectl exec for other commands", q.shell(), SHELLS.join(", ")));
    }
    None
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out += &format!("%{b:02X}"),
        }
    }
    out
}

/// The `pods/exec` path for `q`, with a TTY (stdout and stderr are one stream).
pub fn exec_path(q: &ExecQuery) -> String {
    let mut p = format!(
        "/api/v1/namespaces/{}/pods/{}/exec?command={}&stdin=true&stdout=true&stderr=true&tty=true",
        q.namespace,
        q.pod,
        urlencode(q.shell())
    );
    if let Some(c) = &q.container {
        p += &format!("&container={c}");
    }
    p
}

/// `server` + `path` as a WebSocket URL.
pub fn ws_url(server: &str, path: &str) -> String {
    if let Some(rest) = server.strip_prefix("https://") {
        format!("wss://{rest}{path}")
    } else if let Some(rest) = server.strip_prefix("http://") {
        format!("ws://{rest}{path}")
    } else {
        format!("{server}{path}")
    }
}

/// One channel-prefixed frame.
pub fn frame(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(payload.len() + 1);
    v.push(channel);
    v.extend_from_slice(payload);
    v
}

/// A frame's channel and payload; none for an empty frame.
pub fn unframe(bytes: &[u8]) -> Option<(u8, &[u8])> {
    Some((*bytes.first()?, &bytes[1..]))
}

/// A terminal-size frame, as the API reads it on the resize channel.
pub fn resize_frame(width: u16, height: u16) -> Vec<u8> {
    frame(CH_RESIZE, format!("{{\"Width\":{width},\"Height\":{height}}}").as_bytes())
}

/// The status channel's answer in words: how the command ended.
pub fn status_text(payload: &[u8]) -> String {
    let v: Value = serde_json::from_slice(payload).unwrap_or(Value::Null);
    if v["status"].as_str() == Some("Success") {
        return "the command exited".into();
    }
    let message = v["message"].as_str().unwrap_or("the command ended");
    let causes = v["details"]["causes"].as_array().into_iter().flatten().filter_map(|c| c["message"].as_str()).collect::<Vec<_>>().join("; ");
    if causes.is_empty() { message.to_string() } else { format!("{message}: {causes}") }
}

/// A TLS client config trusting `access`'s CA and presenting its client certificate.
fn tls_config(access: &Access) -> Result<Arc<rustls::ClientConfig>, ActionError> {
    let un = |what: &str, e: String| ActionError::Unavailable(format!("the kubeconfig's {what}: {e}"));
    let mut roots = rustls::RootCertStore::empty();
    let mut ca_reader = std::io::BufReader::new(access.ca_pem());
    for cert in rustls_pemfile::certs(&mut ca_reader) {
        let cert = cert.map_err(|e| un("CA", e.to_string()))?;
        roots.add(cert).map_err(|e| un("CA", e.to_string()))?;
    }
    let mut id_reader = std::io::BufReader::new(access.identity_pem());
    let certs: Vec<_> = rustls_pemfile::certs(&mut id_reader).collect::<Result<_, _>>().map_err(|e| un("client certificate", e.to_string()))?;
    let mut key_reader = std::io::BufReader::new(access.identity_pem());
    let key = rustls_pemfile::private_key(&mut key_reader).map_err(|e| un("client key", e.to_string()))?.ok_or_else(|| un("client key", "none found".into()))?;
    // Named explicitly: reqwest's rustls also links a provider, so the
    // process-wide default is ambiguous and the plain builder panics.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| un("TLS versions", e.to_string()))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| un("client certificate", e.to_string()))?;
    Ok(Arc::new(config))
}

/// Opens the exec WebSocket to the API server for `q`.
pub async fn connect(access: &Access, q: &ExecQuery) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ActionError> {
    if let Some(why) = refusal(q) {
        return Err(ActionError::Refused(why));
    }
    let url = ws_url(access.server(), &exec_path(q));
    let mut request = url.into_client_request().map_err(|e| ActionError::Unavailable(format!("the exec URL: {e}")))?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "v4.channel.k8s.io".parse().expect("a header value"));
    let connector = Connector::Rustls(tls_config(access)?);
    let (stream, _) = tokio_tungstenite::connect_async_tls_with_config(request, None, false, Some(connector))
        .await
        .map_err(|e| ActionError::Unavailable(format!("the Kubernetes API: {e}")))?;
    Ok(stream)
}

/// One message from the API server's exec stream, decoded by channel.
pub enum Upstream {
    /// Terminal output (stdout, and stderr — one stream under a TTY).
    Output(Vec<u8>),
    /// The command ended; why, in words.
    Ended(String),
}

/// Decodes one upstream frame; `None` for frames the browser need not see.
pub fn decode(bytes: &[u8]) -> Option<Upstream> {
    let (channel, payload) = unframe(bytes)?;
    match channel {
        CH_STDOUT | CH_STDERR => Some(Upstream::Output(payload.to_vec())),
        CH_STATUS => Some(Upstream::Ended(status_text(payload))),
        _ => None,
    }
}

/// The sink half's type, for the API's bridge loop.
pub type UpstreamStream = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
pub type UpstreamSink = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tokio_tungstenite::tungstenite::Message>;

pub fn split(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> (UpstreamSink, UpstreamStream) {
    stream.split()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q() -> ExecQuery {
        ExecQuery { namespace: "demo".into(), pod: "web-1".into(), container: None, shell: None }
    }

    #[test]
    fn paths_and_urls_are_built_for_a_tty_shell() {
        assert_eq!(exec_path(&q()), "/api/v1/namespaces/demo/pods/web-1/exec?command=%2Fbin%2Fsh&stdin=true&stdout=true&stderr=true&tty=true");
        let with = ExecQuery { container: Some("app".into()), shell: Some("/bin/bash".into()), ..q() };
        assert!(exec_path(&with).ends_with("&container=app"));
        assert!(exec_path(&with).contains("command=%2Fbin%2Fbash"));
        assert_eq!(ws_url("https://10.0.3.253:6443", "/x"), "wss://10.0.3.253:6443/x");
        assert_eq!(ws_url("http://k3s:6443", "/x"), "ws://k3s:6443/x");
    }

    #[test]
    fn system_namespaces_other_shells_and_bad_names_are_refused() {
        assert_eq!(refusal(&q()), None);
        for ns in PROTECTED_NAMESPACES {
            assert!(refusal(&ExecQuery { namespace: ns.into(), ..q() }).is_some_and(|w| w.contains("kubectl")), "{ns}");
        }
        assert!(refusal(&ExecQuery { shell: Some("rm -rf /".into()), ..q() }).is_some());
        assert!(refusal(&ExecQuery { shell: Some("/bin/zsh".into()), ..q() }).is_some());
        assert!(refusal(&ExecQuery { pod: "../x".into(), ..q() }).is_some());
        assert!(refusal(&ExecQuery { container: Some("a b".into()), ..q() }).is_some());
    }

    #[test]
    fn frames_carry_their_channel_and_status_says_how_it_ended() {
        assert_eq!(unframe(&frame(CH_STDIN, b"ls\n")), Some((0, &b"ls\n"[..])));
        assert_eq!(unframe(&[]), None);
        assert_eq!(resize_frame(80, 24), frame(CH_RESIZE, br#"{"Width":80,"Height":24}"#));
        match decode(&frame(CH_STDOUT, b"out")) {
            Some(Upstream::Output(b)) => assert_eq!(b, b"out"),
            _ => panic!("stdout is output"),
        }
        assert!(decode(&frame(CH_RESIZE, b"{}")).is_none());
        assert_eq!(status_text(br#"{"status":"Success"}"#), "the command exited");
        let failed = status_text(br#"{"status":"Failure","message":"command terminated with non-zero exit code","details":{"causes":[{"reason":"ExitCode","message":"3"}]}}"#);
        assert_eq!(failed, "command terminated with non-zero exit code: 3");
    }
}
