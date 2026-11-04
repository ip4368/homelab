//! A pod's logs, as `kubectl logs` reads them: a tail over HTTP and a
//! `follow` stream the API relays over a WebSocket. Read-only, so every
//! namespace may be read — debugging K3s and Longhorn themselves is a core
//! case (docs/plans/2026-11-04-workload-debug-design.md §4).

use crate::kube::Access;
use crate::kube_action::{valid_name, valid_namespace, ActionError};
use serde::Deserialize;

pub const DEFAULT_TAIL: u32 = 200;
pub const MAX_TAIL: u32 = 2000;
/// The most log text one tail answer carries; the newest bytes are kept.
pub const MAX_BYTES: usize = 4 << 20;

/// Which logs, as the UI asks for them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LogQuery {
    pub namespace: String,
    pub pod: String,
    /// None lets Kubernetes pick (it refuses a multi-container pod).
    pub container: Option<String>,
    pub tail: Option<u32>,
    /// The previous, terminated container instance.
    #[serde(default)]
    pub previous: bool,
    pub since_seconds: Option<u64>,
    #[serde(default)]
    pub timestamps: bool,
}

/// Why the server will not fetch these logs, if it will not.
pub fn refusal(q: &LogQuery) -> Option<String> {
    if !valid_namespace(&q.namespace) || !valid_name(&q.pod) {
        return Some(format!("{:?}/{:?} is not a Kubernetes namespace and pod", q.namespace, q.pod));
    }
    if let Some(c) = &q.container {
        // A container name is a DNS label, as a namespace is.
        if !valid_namespace(c) {
            return Some(format!("{c:?} is not a container name"));
        }
    }
    None
}

/// The log API's path for `q`; `tailLines` capped so one answer stays small.
pub fn log_path(q: &LogQuery, follow: bool) -> String {
    let tail = q.tail.unwrap_or(DEFAULT_TAIL).clamp(1, MAX_TAIL);
    let mut p = format!("/api/v1/namespaces/{}/pods/{}/log?tailLines={tail}", q.namespace, q.pod);
    if let Some(c) = &q.container {
        p += &format!("&container={c}");
    }
    if follow {
        p += "&follow=true";
    }
    if q.previous {
        p += "&previous=true";
    }
    if q.timestamps {
        p += "&timestamps=true";
    }
    if let Some(s) = q.since_seconds {
        p += &format!("&sinceSeconds={s}");
    }
    p
}

/// The newest `max` bytes of `text`, with a cut first line dropped.
pub fn keep_tail(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut start = text.len() - max;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    // Already at a line start (or the cut moved past a split character
    // onto one): nothing partial to drop.
    if start == 0 || text.as_bytes()[start - 1] == b'\n' && start == text.len() - max {
        return &text[start..];
    }
    match text[start..].find('\n') {
        Some(nl) => &text[start + nl + 1..],
        None => &text[start..],
    }
}

/// The tail the query asks for, as text.
pub async fn fetch(access: &Access, q: &LogQuery) -> Result<String, ActionError> {
    if let Some(why) = refusal(q) {
        return Err(ActionError::Refused(why));
    }
    let res = access.get(&log_path(q, false)).await?;
    let text = res.text().await.map_err(|e| ActionError::Unavailable(format!("the Kubernetes API: {e}")))?;
    Ok(keep_tail(&text, MAX_BYTES).to_string())
}

/// The live `follow` response for the query, to relay chunk by chunk.
pub async fn follow(access: &Access, q: &LogQuery) -> Result<reqwest::Response, ActionError> {
    if let Some(why) = refusal(q) {
        return Err(ActionError::Refused(why));
    }
    Ok(access.get(&log_path(q, true)).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q() -> LogQuery {
        LogQuery { namespace: "demo".into(), pod: "web-1".into(), container: None, tail: None, previous: false, since_seconds: None, timestamps: false }
    }

    #[test]
    fn paths_cap_the_tail_and_carry_the_options() {
        assert_eq!(log_path(&q(), false), "/api/v1/namespaces/demo/pods/web-1/log?tailLines=200");
        let full = LogQuery { container: Some("app".into()), tail: Some(99_999), previous: true, since_seconds: Some(60), timestamps: true, ..q() };
        assert_eq!(log_path(&full, true), "/api/v1/namespaces/demo/pods/web-1/log?tailLines=2000&container=app&follow=true&previous=true&timestamps=true&sinceSeconds=60");
        let zero = LogQuery { tail: Some(0), ..q() };
        assert!(log_path(&zero, false).contains("tailLines=1"));
    }

    #[test]
    fn bad_names_are_refused_but_system_namespaces_may_be_read() {
        assert_eq!(refusal(&q()), None);
        assert_eq!(refusal(&LogQuery { namespace: "longhorn-system".into(), ..q() }), None, "logs are read-only");
        for bad in [
            LogQuery { namespace: "../x".into(), ..q() },
            LogQuery { pod: "web/log".into(), ..q() },
            LogQuery { pod: "..".into(), ..q() },
            LogQuery { container: Some("a b".into()), ..q() },
            LogQuery { container: Some("a.b".into()), ..q() },
        ] {
            assert!(refusal(&bad).is_some(), "{bad:?}");
        }
    }

    #[test]
    fn tails_keep_the_newest_whole_lines() {
        assert_eq!(keep_tail("a\nb\n", 100), "a\nb\n");
        assert_eq!(keep_tail("one\ntwo\nthree\n", 9), "three\n");
        assert_eq!(keep_tail("no newline at all", 4), " all", "no line to lose: the raw tail stands");
        assert_eq!(keep_tail("héllo\nwörld\n", 7), "wörld\n", "no split in a character");
    }
}
