//! The HTTP API: user operations (through the control plane), the lab's
//! hardware faults, and the WebSocket.

use crate::driver::{DriverHandle, LabError};
use crate::kube_action::{ActionError, KubeAction, WorkloadRef};
use crate::kube_apply::ApplyRequest;
use crate::kube_exec::ExecQuery;
use crate::kube_log::LogQuery;
use crate::ws::ws_handler;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Json, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::Router;
use control::metrics::{MetricEntity, METRICS};
use control::proto::{ApiOp, ApiResult};
use lab::manager::LabFault;
use serde::Deserialize;
use serde_json::json;

pub fn app(driver: DriverHandle) -> Router {
    Router::new()
        .route("/api/ops", post(submit_op))
        .route("/api/metrics", get(get_metrics))
        .route("/api/lab/fault", post(lab_fault))
        .route("/api/lab/kubeconfig", get(lab_kubeconfig))
        .route("/api/lab/seed", post(lab_seed))
        .route("/api/kube", post(kube_action))
        .route("/api/kube/apply", post(kube_apply))
        .route("/api/kube/manifest", get(kube_manifest))
        .route("/api/kube/metrics", get(kube_metrics))
        .route("/api/kube/logs", get(kube_logs))
        .route("/api/kube/logs/ws", get(kube_logs_ws))
        .route("/api/kube/exec/ws", get(kube_exec_ws))
        .route("/api/lab/console", get(lab_console))
        .route("/api/lab/journal", get(lab_journal))
        .route("/api/lab/terminal/ws", get(lab_terminal_ws))
        .route("/api/ws", get(ws_handler))
        .route("/api", any(unknown_api))
        .route("/api/{*path}", any(unknown_api))
        .with_state(driver)
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn unavailable() -> Response {
    error(StatusCode::SERVICE_UNAVAILABLE, "control plane unavailable")
}

/// Keeps unknown API paths from falling through to the UI's `index.html`.
async fn unknown_api() -> Response {
    error(StatusCode::NOT_FOUND, "no such endpoint")
}

/// Runs `op` through the control plane: its `Ok` value, or the response saying why not.
async fn run_op(driver: &DriverHandle, op: ApiOp) -> Result<serde_json::Value, Response> {
    match driver.op(op).await {
        ApiResult::Ok(value) => Ok(value),
        ApiResult::Err(message) => Err(error(StatusCode::BAD_REQUEST, &message)),
        ApiResult::NotLeader | ApiResult::Unavailable => Err(unavailable()),
    }
}

/// 200 with the [`ApiResult`] for `Ok`, 400 for a rejected operation, 503 when the control plane could not answer.
async fn submit_op(State(driver): State<DriverHandle>, Json(op): Json<ApiOp>) -> Response {
    match run_op(&driver, op).await {
        Ok(value) => Json(ApiResult::Ok(value)).into_response(),
        Err(response) => response,
    }
}

#[derive(Deserialize)]
struct MetricsQuery {
    entity: Option<String>,
    /// Comma-separated metric names.
    m: Option<String>,
}

/// `?entity=host:<id>|fleet|role:<Role>|switch:<id>&m=cpu_pct,ram_pct`:
/// 200 with `{since, series: {metric: [[t, v], ...]}}` from the leader's
/// memory; 400 for an unknown entity or metric; 503 when the control plane
/// could not answer.
async fn get_metrics(State(driver): State<DriverHandle>, Query(q): Query<MetricsQuery>) -> Response {
    let entity = match q.entity.as_deref().map(str::parse::<MetricEntity>) {
        Some(Ok(entity)) => entity,
        Some(Err(message)) => return error(StatusCode::BAD_REQUEST, &message),
        None => return error(StatusCode::BAD_REQUEST, "entity is required"),
    };
    let metrics: Vec<String> = q.m.unwrap_or_default().split(',').filter(|m| !m.is_empty()).map(String::from).collect();
    if metrics.is_empty() {
        return error(StatusCode::BAD_REQUEST, &format!("m is required: some of {}", METRICS.join(", ")));
    }
    if let Some(bad) = metrics.iter().find(|m| !METRICS.contains(&m.as_str())) {
        return error(StatusCode::BAD_REQUEST, &format!("unknown metric {bad:?}: expected some of {}", METRICS.join(", ")));
    }
    match run_op(&driver, ApiOp::GetSeries { entity, metrics }).await {
        Ok(value) => Json(value).into_response(),
        Err(response) => response,
    }
}

/// 200 `{"ok": true}` (with `"vm"` naming a machine `AddVm` racked), 400
/// if the lab refused, 500 if its code panicked, 503 without a lab or a
/// join token.
async fn lab_fault(State(driver): State<DriverHandle>, Json(fault): Json<LabFault>) -> Response {
    match driver.fault(fault).await {
        Ok(None) => Json(json!({ "ok": true })).into_response(),
        Ok(Some(vm)) => Json(json!({ "ok": true, "vm": vm })).into_response(),
        Err(e @ (LabError::NoLab | LabError::NoToken | LabError::NoKubeServer)) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        Err(LabError::Failed(message)) => error(StatusCode::BAD_REQUEST, &message),
        Err(LabError::Panicked(message)) => error(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

/// 200 with an admin kubeconfig for the cluster's Kubernetes (pointed at
/// the VIP), as `sudo homelabd kubeconfig` prints it; 503 while the control
/// plane's leader runs no K3s server, or the control plane does not answer.
async fn lab_kubeconfig(State(driver): State<DriverHandle>) -> Response {
    match driver.kubeconfig().await {
        Ok(k) => ([("content-type", "application/yaml")], k).into_response(),
        Err(e @ (LabError::NoLab | LabError::NoToken | LabError::NoKubeServer)) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        Err(LabError::Failed(message)) => error(StatusCode::SERVICE_UNAVAILABLE, &message),
        Err(LabError::Panicked(message)) => error(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

/// 200 `{"ok": true, "output": <kubectl's>}` once the demo workloads are
/// applied; 503 while no Kubernetes server runs, or without a lab.
async fn lab_seed(State(driver): State<DriverHandle>) -> Response {
    match driver.seed_workloads().await {
        Ok(output) => Json(json!({ "ok": true, "output": output })).into_response(),
        Err(e @ (LabError::NoLab | LabError::NoToken | LabError::NoKubeServer)) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        Err(LabError::Failed(message)) => error(StatusCode::SERVICE_UNAVAILABLE, &message),
        Err(LabError::Panicked(message)) => error(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

/// 400 if the server refuses, Kubernetes' own 4xx if it does (404 no such
/// workload, 422 invalid), 503 without a kubeconfig or an API.
fn action_error(e: ActionError) -> Response {
    match e {
        ActionError::Refused(message) => error(StatusCode::BAD_REQUEST, &message),
        ActionError::Kube { status, message } => error(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST), &message),
        ActionError::Unavailable(message) => error(StatusCode::SERVICE_UNAVAILABLE, &message),
    }
}

/// 200 `{"ok": true}` once Kubernetes has taken the change; else as [`action_error`].
async fn kube_action(State(driver): State<DriverHandle>, Json(action): Json<KubeAction>) -> Response {
    match driver.kube_action(action).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => action_error(e),
    }
}

/// 200 with an [`ApplyReport`](crate::kube_apply::ApplyReport): what became
/// of each object, Kubernetes' refusals and conflicts among them. 400 if the
/// YAML does not parse or the server refuses an object (then nothing is
/// applied); else as [`action_error`].
async fn kube_apply(State(driver): State<DriverHandle>, Json(req): Json<ApplyRequest>) -> Response {
    match driver.kube_apply(req).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => action_error(e),
    }
}

/// `?namespace=&kind=&name=`: 200 with the workload as YAML to edit; else as [`action_error`].
async fn kube_manifest(State(driver): State<DriverHandle>, Query(w): Query<WorkloadRef>) -> Response {
    match driver.kube_manifest(w).await {
        Ok(yaml) => ([("content-type", "application/yaml")], yaml).into_response(),
        Err(e) => action_error(e),
    }
}

#[derive(Deserialize)]
struct KubeMetricsQuery {
    /// A pod as `<namespace>/<name>`.
    pod: Option<String>,
    /// A workload as `<namespace>/<Kind>/<name>`.
    workload: Option<String>,
    m: Option<String>,
}

/// `?pod=ns/name|workload=ns/Kind/name&m=cpu_milli,net_rx_bps`: 200 with
/// `{since, series}` from this server's sampled history; 400 for an unknown
/// entity or metric, or with neither or both of them.
async fn kube_metrics(State(driver): State<DriverHandle>, Query(q): Query<KubeMetricsQuery>) -> Response {
    let entity = match (q.pod, q.workload) {
        (Some(p), None) => p,
        (None, Some(w)) => w,
        _ => return error(StatusCode::BAD_REQUEST, "exactly one of pod and workload is required"),
    };
    let metrics: Vec<String> = q.m.unwrap_or_default().split(',').filter(|m| !m.is_empty()).map(String::from).collect();
    match driver.kube_series(&entity, &metrics) {
        Ok(value) => Json(value).into_response(),
        Err(message) => error(StatusCode::BAD_REQUEST, &message),
    }
}

/// A container's log tail as text; else as [`action_error`].
async fn kube_logs(State(driver): State<DriverHandle>, Query(q): Query<LogQuery>) -> Response {
    if let Some(why) = crate::kube_log::refusal(&q) {
        return error(StatusCode::BAD_REQUEST, &why);
    }
    match driver.kube_logs(q).await {
        Ok(text) => ([("content-type", "text/plain; charset=utf-8")], text).into_response(),
        Err(e) => action_error(e),
    }
}

/// The follow stream: text frames as log chunks arrive, an hour at most.
async fn kube_logs_ws(State(driver): State<DriverHandle>, Query(q): Query<LogQuery>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| serve_log_follow(socket, driver, q))
}

async fn serve_log_follow(mut socket: WebSocket, driver: DriverHandle, q: LogQuery) {
    let mut res = match driver.kube_log_follow(q).await {
        Ok(r) => r,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({ "error": e.to_string() }).to_string().into())).await;
            return;
        }
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
    loop {
        tokio::select! {
            chunk = res.chunk() => match chunk {
                Ok(Some(bytes)) => {
                    if socket.send(Message::Text(String::from_utf8_lossy(&bytes).into_owned().into())).await.is_err() {
                        return;
                    }
                }
                Ok(None) | Err(_) => return,
            },
            incoming = socket.recv() => match incoming {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            },
            () = tokio::time::sleep_until(deadline) => return,
        }
    }
}

/// A shell in a container: binary frames are keystrokes (browser → server)
/// and terminal output (server → browser); text frames carry
/// `{"resize":{width,height}}` in and `{"status":…}` / `{"error":…}` out.
async fn kube_exec_ws(State(driver): State<DriverHandle>, Query(q): Query<ExecQuery>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| serve_exec(socket, driver, q))
}

async fn serve_exec(mut socket: WebSocket, driver: DriverHandle, q: ExecQuery) {
    use futures_util::{SinkExt, StreamExt};
    let upstream = match driver.kube_exec(q).await {
        Ok(s) => s,
        Err(e) => {
            let _ = socket.send(Message::Text(json!({ "error": e.to_string() }).to_string().into())).await;
            return;
        }
    };
    let (mut up_sink, mut up_stream) = crate::kube_exec::split(upstream);
    let deadline = tokio::time::Instant::now() + crate::kube_exec::MAX_SESSION;
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Binary(data))) => {
                    let framed = crate::kube_exec::frame(crate::kube_exec::CH_STDIN, &data);
                    if up_sink.send(tokio_tungstenite::tungstenite::Message::Binary(framed.into())).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
                    if let (Some(w), Some(h)) = (v["resize"]["width"].as_u64(), v["resize"]["height"].as_u64()) {
                        let framed = crate::kube_exec::resize_frame(w.min(1000) as u16, h.min(1000) as u16);
                        if up_sink.send(tokio_tungstenite::tungstenite::Message::Binary(framed.into())).await.is_err() {
                            return;
                        }
                    }
                }
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            },
            next = up_stream.next() => match next {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => match crate::kube_exec::decode(&bytes) {
                    Some(crate::kube_exec::Upstream::Output(out)) => {
                        if socket.send(Message::Binary(out.into())).await.is_err() {
                            return;
                        }
                    }
                    Some(crate::kube_exec::Upstream::Ended(why)) => {
                        let _ = socket.send(Message::Text(json!({ "status": why }).to_string().into())).await;
                        return;
                    }
                    None => {}
                },
                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
            () = tokio::time::sleep_until(deadline) => {
                let _ = socket.send(Message::Text(json!({ "status": "session ended: 15 minutes is the most one shell may run" }).to_string().into())).await;
                return;
            }
        }
    }
}

fn lab_error(e: LabError) -> Response {
    match e {
        e @ (LabError::NoLab | LabError::NoToken | LabError::NoKubeServer) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        LabError::Failed(message) => error(StatusCode::BAD_REQUEST, &message),
        LabError::Panicked(message) => error(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

#[derive(Deserialize)]
struct VmQuery {
    vm: String,
}

#[derive(Deserialize)]
struct JournalQuery {
    vm: String,
    unit: String,
    lines: Option<u32>,
}

/// `?vm=`: the VM's serial console log; 503 without a lab, 400 for a bad name.
async fn lab_console(State(driver): State<DriverHandle>, Query(q): Query<VmQuery>) -> Response {
    if !crate::lab_debug::valid_vm(&q.vm) {
        return error(StatusCode::BAD_REQUEST, &format!("{:?} is not a lab VM name", q.vm));
    }
    match driver.lab_console(q.vm).await {
        Ok(text) => ([("content-type", "text/plain; charset=utf-8")], text).into_response(),
        Err(e) => lab_error(e),
    }
}

/// `?vm=&unit=&lines=`: the VM's journal for one of homelabd/k3s/k3s-agent.
async fn lab_journal(State(driver): State<DriverHandle>, Query(q): Query<JournalQuery>) -> Response {
    if !crate::lab_debug::valid_vm(&q.vm) {
        return error(StatusCode::BAD_REQUEST, &format!("{:?} is not a lab VM name", q.vm));
    }
    match driver.lab_journal(q.vm, q.unit, q.lines).await {
        Ok(text) => ([("content-type", "text/plain; charset=utf-8")], text).into_response(),
        Err(e) => lab_error(e),
    }
}

/// An interactive SSH login to a lab VM: binary frames both ways, text
/// frames carry `{"resize":{width,height}}` in and `{"error":…}` out.
async fn lab_terminal_ws(State(driver): State<DriverHandle>, Query(q): Query<VmQuery>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| serve_lab_terminal(socket, driver, q.vm))
}

async fn serve_lab_terminal(mut socket: WebSocket, driver: DriverHandle, vm: String) {
    use portable_pty::{native_pty_system, CommandBuilder, PtySize};
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};
    macro_rules! fail {
        ($why:expr) => {{
            let _ = socket.send(Message::Text(json!({ "error": $why }).to_string().into())).await;
            return;
        }};
    }
    if !crate::lab_debug::valid_vm(&vm) {
        fail!(format!("{vm:?} is not a lab VM name"));
    }
    let (key, ip) = match driver.lab_ssh_target(vm.clone()).await {
        Ok(t) => t,
        Err(e) => {
            fail!(e.to_string());
        }
    };
    eprintln!("lab terminal: ssh to {vm} at {ip}");
    let pair = match native_pty_system().openpty(PtySize { rows: 24, cols: 80, pixel_width: 0, pixel_height: 0 }) {
        Ok(p) => p,
        Err(e) => {
            fail!(format!("cannot open a terminal: {e}"));
        }
    };
    let mut cmd = CommandBuilder::new("ssh");
    cmd.args(crate::lab_debug::ssh_args(&key, ip));
    let child = match pair.slave.spawn_command(cmd) {
        Ok(c) => Arc::new(Mutex::new(c)),
        Err(e) => {
            fail!(format!("cannot start ssh: {e}"));
        }
    };
    drop(pair.slave);
    let master = Arc::new(Mutex::new(pair.master));
    let reader_result = master.lock().expect("pty lock").try_clone_reader();
    let mut reader = match reader_result {
        Ok(r) => r,
        Err(e) => {
            fail!(format!("cannot read the terminal: {e}"));
        }
    };
    let writer_result = master.lock().expect("pty lock").take_writer();
    let writer = match writer_result {
        Ok(w) => Arc::new(Mutex::new(w)),
        Err(e) => {
            fail!(format!("cannot write the terminal: {e}"));
        }
    };
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if out_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        return;
                    }
                }
            }
        }
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Binary(data))) => {
                    let w = writer.clone();
                    let _ = tokio::task::spawn_blocking(move || w.lock().expect("pty writer").write_all(&data)).await;
                }
                Some(Ok(Message::Text(text))) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
                    if let (Some(cols), Some(rows)) = (v["resize"]["width"].as_u64(), v["resize"]["height"].as_u64()) {
                        let _ = master.lock().expect("pty lock").resize(PtySize { rows: rows.min(1000) as u16, cols: cols.min(1000) as u16, pixel_width: 0, pixel_height: 0 });
                    }
                }
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            },
            out = out_rx.recv() => match out {
                Some(bytes) => {
                    if socket.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    let _ = child.lock().expect("ssh child").kill();
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::driver::Published;
    use axum::body::Body;
    use axum::http::Request;
    use control::agent::HostAgent;
    use control::bootstrap::{seeded_durable, SeedHost};
    use control::replica::CpReplica;
    use control::runtime::{Addr, Svc};
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use model::{HostId, Inventory, ProfileId};
    use node::runtime::{wall_ms, NodeConfig, NodeHandle};
    use node::testing::MemOs;
    use node::transport::AddressBook;
    use serde_json::Value;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    /// A real one-host cluster on loopback (its replica and agent on the
    /// real runtime, over a memory-only machine) and a driver whose client
    /// reaches it as through the VIP. No lab.
    pub fn cluster() -> (DriverHandle, NodeHandle) {
        let id = HostId::from("ms01-01");
        let voters = [id.clone()].into();
        let seed = SeedHost { id: id.clone(), inventory: Inventory::from_profile(ProfileId::Ms01, 0), location: "lab".into() };
        let durable = seeded_durable(&[seed], &voters, "test-token", wall_ms());
        let cfg = NodeConfig { me: Addr::Host(id.clone()), listen: Some("127.0.0.1:0".parse().unwrap()), port: 0, book: AddressBook::default() };
        let host = NodeHandle::spawn_with(cfg, Some(Box::new(MemOs::new(id.clone(), Some(durable)))), move |n| {
            n.start(Svc::Cp, Box::new(CpReplica::new(id, None, voters)));
            n.start(Svc::Agent, Box::new(HostAgent::new(None)));
        })
        .unwrap();
        let client = lab::client::spawn_at(Ipv4Addr::LOCALHOST, host.listening.unwrap().port()).unwrap();
        (DriverHandle::start(client, None), host)
    }

    pub async fn wait_for_leader(driver: &DriverHandle) -> Arc<Published> {
        let mut rx = driver.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let p = rx.borrow_and_update().clone();
                if p.view.leader.is_some() {
                    return p;
                }
                rx.changed().await.unwrap();
            }
        })
        .await
        .expect("a leader within 10 s")
    }

    async fn post_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::post(path).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    async fn get_json(app: &Router, path: &str) -> (StatusCode, Value) {
        let res = app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn op(op: ApiOp) -> Value {
        serde_json::to_value(op).unwrap()
    }

    #[tokio::test]
    async fn an_op_goes_through_the_real_control_plane() {
        let (driver, _host) = cluster();
        wait_for_leader(&driver).await;
        let app = app(driver);
        let (status, body) = post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["Ok"].as_str().map(str::len), Some(16));
        let (status, body) = post_json(&app, "/api/ops", op(ApiOp::DeleteRule { id: "nope".into() })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "a rejected op: {body}");
    }

    #[tokio::test]
    async fn metrics_come_from_the_leaders_history_and_are_validated() {
        let (driver, _host) = cluster();
        wait_for_leader(&driver).await;
        let app = app(driver);
        let (status, body) = get_json(&app, "/api/metrics?entity=host:ms01-01&m=cpu_pct").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["series"]["cpu_pct"].is_array(), "{body}");
        assert_eq!(get_json(&app, "/api/metrics?entity=host:ms01-01&m=bogus").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/metrics?m=cpu_pct").await.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn without_a_control_plane_ops_are_unavailable() {
        let client = lab::client::spawn_at(Ipv4Addr::LOCALHOST, 1).unwrap();
        let app = app(DriverHandle::start(client, None));
        let reply = tokio::time::timeout(Duration::from_secs(8), post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)));
        assert_eq!(reply.await.expect("answered within the client's give-up time").0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn lab_faults_need_a_lab_and_are_validated() {
        let (driver, _host) = cluster();
        let app = app(driver);
        let (status, body) = post_json(&app, "/api/lab/fault", json!({"Freeze": {"vm": "ms01-01"}})).await;
        assert_eq!((status, body["error"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("no lab attached")));
        assert_eq!(post_json(&app, "/api/lab/fault", json!({"Explode": {}})).await.0, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn the_websocket_opens_with_a_snapshot() {
        let (driver, _host) = cluster();
        wait_for_leader(&driver).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app(driver)).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/api/ws")).await.unwrap();
        let first = ws.next().await.unwrap().unwrap();
        let msg: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
        assert_eq!(msg["type"], "snapshot");
        assert_eq!(msg["view"]["leader"], "ms01-01");
        assert!(msg["clock"]["now"].as_u64().unwrap() > 1_700_000_000_000);
    }

    #[tokio::test]
    async fn workload_actions_are_checked_before_kubernetes_is_asked() {
        let (driver, _host) = cluster();
        wait_for_leader(&driver).await;
        let app = app(driver);
        let workload = |ns: &str, kind: &str| json!({ "namespace": ns, "kind": kind, "name": "x" });
        let (status, body) = post_json(&app, "/api/kube", json!({ "Delete": { "workload": workload("longhorn-system", "DaemonSet") } })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["error"].as_str().unwrap().contains("kubectl"), "{body}");
        let (status, body) = post_json(&app, "/api/kube", json!({ "Scale": { "workload": workload("demo", "DaemonSet"), "replicas": 2 } })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(post_json(&app, "/api/kube", json!({ "Restart": { "workload": workload("demo", "Pod") } })).await.0, StatusCode::UNPROCESSABLE_ENTITY);
        // This one-host cluster runs no K3s server, so it hands out no kubeconfig.
        let (status, body) = post_json(&app, "/api/kube", json!({ "Restart": { "workload": workload("demo", "Deployment") } })).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let apply = json!({ "yaml": "kind: Namespace", "namespace": "", "dry_run": true, "force": false });
        assert_eq!(post_json(&app, "/api/kube/apply", apply).await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get_json(&app, "/api/kube/manifest?namespace=demo&kind=Deployment&name=web").await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get_json(&app, "/api/kube/manifest?namespace=demo&kind=Pod&name=web").await.0, StatusCode::BAD_REQUEST, "not a workload kind");
    }

    #[tokio::test]
    async fn unknown_api_paths_are_404_not_the_ui() {
        let (driver, _host) = cluster();
        let app = app(driver);
        assert_eq!(get_json(&app, "/api/sim/speed").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get_json(&app, "/api").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn debug_endpoints_validate_before_any_cluster_or_lab_is_asked() {
        let (driver, _host) = cluster();
        wait_for_leader(&driver).await;
        let app = app(driver);
        // Logs: a bad pod name is refused here; a good one reaches for a
        // kubeconfig this cluster cannot hand out (503).
        assert_eq!(get_json(&app, "/api/kube/logs?namespace=demo&pod=..%2F..").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/kube/logs?namespace=demo&pod=web-1&container=a%20b").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/kube/logs?namespace=demo&pod=web-1").await.0, StatusCode::SERVICE_UNAVAILABLE);
        // Metrics: exactly one entity, known metrics, and history to show.
        assert_eq!(get_json(&app, "/api/kube/metrics?m=cpu_milli").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/kube/metrics?pod=demo/web-1&workload=demo/Deployment/web&m=cpu_milli").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/kube/metrics?pod=demo/web-1&m=bogus").await.0, StatusCode::BAD_REQUEST);
        let (status, body) = get_json(&app, "/api/kube/metrics?pod=demo/web-1&m=cpu_milli").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("no history"), "{body}");
        // Lab debug: no lab attached, and names checked first.
        assert_eq!(get_json(&app, "/api/lab/console?vm=..").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get_json(&app, "/api/lab/console?vm=ms01-01").await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get_json(&app, "/api/lab/journal?vm=ms01-01&unit=ssh").await.0, StatusCode::BAD_REQUEST, "the unit allow-list comes first");
        assert_eq!(get_json(&app, "/api/lab/journal?vm=ms01-01&unit=homelabd").await.0, StatusCode::SERVICE_UNAVAILABLE);
    }
}
