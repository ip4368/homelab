//! The HTTP API: user operations (through the control plane), the lab's
//! hardware faults, and the WebSocket.

use crate::driver::{DriverHandle, LabError};
use crate::ws::ws_handler;
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
/// the VIP), as `sudo homelabd kubeconfig` prints it on a K3s server; 503
/// while no server runs, or without a lab.
async fn lab_kubeconfig(State(driver): State<DriverHandle>) -> Response {
    match driver.kubeconfig().await {
        Ok(k) => ([("content-type", "application/yaml")], k).into_response(),
        Err(e @ (LabError::NoLab | LabError::NoToken | LabError::NoKubeServer)) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        Err(LabError::Failed(message)) => error(StatusCode::SERVICE_UNAVAILABLE, &message),
        Err(LabError::Panicked(message)) => error(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
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
        let seed = SeedHost { id: id.clone(), inventory: Inventory::from_profile(ProfileId::Ms01, 0), location: "lab".into(), running: None };
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
    async fn unknown_api_paths_are_404_not_the_ui() {
        let (driver, _host) = cluster();
        let app = app(driver);
        assert_eq!(get_json(&app, "/api/sim/speed").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get_json(&app, "/api").await.0, StatusCode::NOT_FOUND);
    }
}
