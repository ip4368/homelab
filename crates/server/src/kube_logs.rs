//! A container's log, streamed from the kubelet through the API server as
//! it arrives (docs/plans/2026-11-04-workload-insight-logs-terminal-design.md §5).
//! A read, so in every namespace: the logs of K3s's and Longhorn's own pods
//! are what you need when homelabd says something is wrong with them.

use crate::kube::{Access, SendError};
use crate::kube_action::{valid_name, valid_namespace, ActionError};
use serde::Deserialize;

/// Lines from the end, when the query does not say.
pub const DEFAULT_TAIL: u32 = 500;

/// `GET /api/kube/logs`'s query.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LogQuery {
    pub namespace: String,
    pub pod: String,
    /// Needed when the pod has several containers.
    pub container: Option<String>,
    /// The log of the run before the last restart.
    #[serde(default)]
    pub previous: bool,
    /// Lines from the end; 0 for the whole log. [`DEFAULT_TAIL`] if not given.
    pub tail: Option<u32>,
    /// Only lines from the last this many seconds.
    pub since: Option<u32>,
    /// Keep the stream open for new lines (default); without, the log so far, as a download.
    pub follow: Option<bool>,
}

impl LogQuery {
    pub fn follows(&self) -> bool {
        self.follow.unwrap_or(true)
    }

    /// Why it cannot be asked for: names that are not Kubernetes names would change the URL.
    pub fn refusal(&self) -> Option<String> {
        if !valid_namespace(&self.namespace) || !valid_name(&self.pod) || self.container.as_deref().is_some_and(|c| !valid_namespace(c)) {
            return Some(format!("not a pod or container name: {}/{} {}", self.namespace, self.pod, self.container.as_deref().unwrap_or("")));
        }
        None
    }

    /// Its path in the Kubernetes API. Lines always carry their timestamps:
    /// the viewer hides them unless asked, and merges pods' logs by them.
    pub fn path(&self) -> String {
        let mut q = vec!["timestamps=true".to_string()];
        if let Some(c) = &self.container {
            q.push(format!("container={c}"));
        }
        if self.follows() {
            q.push("follow=true".into());
        }
        if self.previous {
            q.push("previous=true".into());
        }
        match self.tail.unwrap_or(DEFAULT_TAIL) {
            0 => {}
            n => q.push(format!("tailLines={n}")),
        }
        if let Some(s) = self.since {
            q.push(format!("sinceSeconds={s}"));
        }
        format!("/api/v1/namespaces/{}/pods/{}/log?{}", self.namespace, self.pod, q.join("&"))
    }

    /// A file name for the download: `web-6d4-x2_nginx.log`.
    pub fn file_name(&self) -> String {
        let container = self.container.as_deref().map(|c| format!("_{c}")).unwrap_or_default();
        let previous = if self.previous { "_previous" } else { "" };
        format!("{}{container}{previous}.log", self.pod)
    }
}

/// The log's stream, once Kubernetes has started it; or why not, in its words
/// (no such pod, no previous container, a container name needed). The caller
/// has checked [`LogQuery::refusal`].
pub async fn open(access: &Access, q: &LogQuery) -> Result<reqwest::Response, ActionError> {
    access.stream(&q.path()).await.map_err(|e| match e {
        SendError::Refused { status, message, .. } => ActionError::Kube { status, message },
        SendError::Unreachable(m) => ActionError::Unavailable(m),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(container: Option<&str>) -> LogQuery {
        LogQuery { namespace: "demo".into(), pod: "web-6d4-x2".into(), container: container.map(String::from), previous: false, tail: None, since: None, follow: None }
    }

    #[test]
    fn a_query_follows_the_last_lines_with_timestamps_by_default() {
        assert_eq!(q(None).path(), "/api/v1/namespaces/demo/pods/web-6d4-x2/log?timestamps=true&follow=true&tailLines=500");
        let crashed = LogQuery { previous: true, tail: Some(0), follow: Some(false), since: Some(600), ..q(Some("nginx")) };
        assert_eq!(crashed.path(), "/api/v1/namespaces/demo/pods/web-6d4-x2/log?timestamps=true&container=nginx&previous=true&sinceSeconds=600");
        assert_eq!(crashed.file_name(), "web-6d4-x2_nginx_previous.log");
        assert_eq!(q(None).file_name(), "web-6d4-x2.log");
    }

    #[test]
    fn names_that_would_change_the_url_are_refused() {
        assert_eq!(q(Some("nginx")).refusal(), None);
        for bad in [LogQuery { pod: "../../secrets".into(), ..q(None) }, LogQuery { namespace: "demo&x=1".into(), ..q(None) }, q(Some("a?b"))] {
            assert!(bad.refusal().is_some(), "{bad:?}");
        }
    }

    #[test]
    fn a_query_reads_as_the_ui_sends_it() {
        let parsed: LogQuery = serde_json::from_value(serde_json::json!({ "namespace": "demo", "pod": "p", "previous": true, "tail": 50 })).unwrap();
        assert_eq!((parsed.previous, parsed.tail, parsed.follows()), (true, Some(50), true));
    }
}
