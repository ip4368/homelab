//! The HTTP API: user operations, simulation controls and the WebSocket.

use crate::driver::{DriverGone, DriverHandle, OpOutcome, SPEEDS, ScenarioError};
use crate::ws::ws_handler;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::Router;
use control::proto::{ApiOp, ApiResult};
use serde::Deserialize;
use serde_json::json;
use sim::faults::Fault;
use sim::presets::Preset;
use sim::scenario::Scenario;
use std::time::Duration;

/// How long a request waits for the driver before giving up on the control plane.
/// The client's own 5 s sim timeout normally answers first.
const WALL_TIMEOUT: Duration = Duration::from_secs(10);

pub fn app(driver: DriverHandle) -> Router {
    Router::new()
        .route("/api/ops", post(submit_op))
        .route("/api/sim/speed", post(set_speed))
        .route("/api/sim/fault", post(inject_fault))
        .route("/api/sim/scenario", post(run_scenario))
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

fn paused() -> Response {
    error(StatusCode::CONFLICT, "simulation paused")
}

impl IntoResponse for DriverGone {
    fn into_response(self) -> Response {
        error(StatusCode::SERVICE_UNAVAILABLE, "simulation stopped")
    }
}

/// Keeps unknown API paths from falling through to the UI's `index.html`.
async fn unknown_api() -> Response {
    error(StatusCode::NOT_FOUND, "no such endpoint")
}

fn ok() -> Response {
    Json(json!({ "ok": true })).into_response()
}

/// 200 with the [`ApiResult`] for `Ok`, 400 for a rejected operation, 503 when the control plane could not answer.
async fn submit_op(State(driver): State<DriverHandle>, Json(op): Json<ApiOp>) -> Response {
    match tokio::time::timeout(WALL_TIMEOUT, driver.op(op)).await {
        Err(_) => unavailable(),
        Ok(Err(gone)) => gone.into_response(),
        Ok(Ok(OpOutcome::Paused)) => paused(),
        Ok(Ok(OpOutcome::Done(result))) => match result {
            ApiResult::Ok(_) => Json(result).into_response(),
            ApiResult::Err(message) => error(StatusCode::BAD_REQUEST, &message),
            ApiResult::NotLeader | ApiResult::Unavailable => unavailable(),
        },
    }
}

/// Takes any JSON so that a wrong type is a 400 like a wrong value, not axum's 422.
async fn set_speed(State(driver): State<DriverHandle>, Json(body): Json<serde_json::Value>) -> Response {
    let speed = body["speed"].as_u64().and_then(|s| u32::try_from(s).ok()).filter(|s| SPEEDS.contains(s));
    let Some(speed) = speed else {
        return error(StatusCode::BAD_REQUEST, &format!("speed must be one of {SPEEDS:?}"));
    };
    match driver.speed(speed).await {
        Ok(()) => ok(),
        Err(gone) => gone.into_response(),
    }
}

async fn inject_fault(State(driver): State<DriverHandle>, Json(fault): Json<Fault>) -> Response {
    match driver.fault(fault).await {
        Ok(Ok(())) => ok(),
        Ok(Err(message)) => error(StatusCode::BAD_REQUEST, &message),
        Err(gone) => gone.into_response(),
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum PresetName {
    Default,
    Scale500,
}

/// A [`Scenario`], or `{"preset": ...}` to start over on a preset fleet.
#[derive(Deserialize)]
#[serde(untagged)]
enum ScenarioBody {
    Preset { preset: PresetName },
    Scenario(Scenario),
}

async fn run_scenario(State(driver): State<DriverHandle>, Json(body): Json<ScenarioBody>) -> Response {
    match body {
        ScenarioBody::Preset { preset } => {
            let preset = match preset {
                PresetName::Default => Preset::Default,
                PresetName::Scale500 => Preset::Scale500,
            };
            match driver.preset(preset).await {
                Ok(()) => ok(),
                Err(gone) => gone.into_response(),
            }
        }
        ScenarioBody::Scenario(scenario) => match driver.scenario(scenario).await {
            Ok(Ok(())) => ok(),
            Ok(Err(ScenarioError::Paused)) => paused(),
            Ok(Err(ScenarioError::Failed(message))) => error(StatusCode::BAD_REQUEST, &message),
            Err(gone) => gone.into_response(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::Published;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use control::proto::ApiOp;
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use sim::faults::Fault;
    use sim::presets::Preset;
    use std::time::Duration;
    use tower::ServiceExt;

    /// A Default-preset driver at 20x, so elections and timeouts take a fraction of the wall time.
    fn start() -> DriverHandle {
        DriverHandle::spawn(Preset::Default, 20)
    }

    /// Waits until the client has synced with an elected leader.
    async fn wait_for_leader(driver: &DriverHandle) -> std::sync::Arc<Published> {
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
        .expect("a leader is elected and the client syncs")
    }

    async fn post_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::post(path).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn op(op: ApiOp) -> Value {
        serde_json::to_value(op).unwrap()
    }

    #[tokio::test]
    async fn approve_op_roundtrip() {
        let driver = start();
        wait_for_leader(&driver).await;
        let app = app(driver);
        let (status, body) = post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["Ok"].as_str().is_some_and(|t| !t.is_empty()), "a token string: {body}");
    }

    #[tokio::test]
    async fn paused_returns_409() {
        let driver = start();
        wait_for_leader(&driver).await;
        let app = app(driver);
        assert_eq!(post_json(&app, "/api/sim/speed", json!({"speed": 0})).await.0, StatusCode::OK);
        let reply = tokio::time::timeout(Duration::from_secs(1), post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)));
        let (status, body) = reply.await.expect("answered at once, not after a sim timeout");
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body, json!({"error": "simulation paused"}));
    }

    #[tokio::test]
    async fn unavailable_returns_503() {
        let driver = start();
        let truth = wait_for_leader(&driver).await.truth.clone();
        let app = app(driver);
        let voters: Vec<_> = truth.hosts.iter().filter(|h| h.cp_role.is_some()).map(|h| h.id.clone()).collect();
        assert_eq!(voters.len(), 3);
        for host in &voters[..2] {
            let (status, body) = post_json(&app, "/api/sim/fault", serde_json::to_value(Fault::KillHost { host: host.clone() }).unwrap()).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let reply = tokio::time::timeout(Duration::from_secs(6), post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)));
        let (status, body) = reply.await.expect("the client gives up after 5 sim seconds");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, json!({"error": "control plane unavailable"}));
    }

    #[tokio::test]
    async fn ws_first_message_is_snapshot() {
        let driver = start();
        wait_for_leader(&driver).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app(driver)).into_future());

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/api/ws")).await.unwrap();
        let first = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let first: Value = serde_json::from_str(first.as_str()).unwrap();
        assert_eq!(first["type"], "snapshot");
        assert_eq!(first["view"]["hosts"].as_array().unwrap().len(), 60);
        assert_eq!(first["truth"]["hosts"].as_array().unwrap().len(), 60);
        assert_eq!(first["sim"]["speed"], 20);

        let second = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let second: Value = serde_json::from_str(second.as_str()).unwrap();
        assert_eq!(second["type"], "delta");
        assert!(second["sim"]["now"].as_u64() > first["sim"]["now"].as_u64(), "the clock ticks");
    }

    #[tokio::test]
    async fn only_the_offered_speeds_are_accepted() {
        let driver = start();
        let app = app(driver.clone());
        for speed in [0, 1, 5, 20, 60] {
            assert_eq!(post_json(&app, "/api/sim/speed", json!({ "speed": speed })).await.0, StatusCode::OK);
        }
        for speed in [json!(2), json!(100), json!(-1), json!(5.5), json!("20"), json!(null), json!(4_294_967_297u64)] {
            let (status, _) = post_json(&app, "/api/sim/speed", json!({ "speed": speed })).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "speed {speed}");
        }
        assert_eq!(post_json(&app, "/api/sim/speed", json!({})).await.0, StatusCode::BAD_REQUEST);
        let mut rx = driver.subscribe();
        let sim = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                rx.changed().await.unwrap();
                let sim = rx.borrow().sim;
                if sim.speed == 60 {
                    return sim;
                }
            }
        })
        .await
        .expect("the last accepted speed is published");
        assert!(!sim.paused);
    }

    #[tokio::test]
    async fn faults_and_scenarios_are_validated_and_paused_scenarios_conflict() {
        let driver = start();
        wait_for_leader(&driver).await;
        let app = app(driver);
        let kill = json!({"KillHost": {"host": "no-such-host"}});
        assert_eq!(post_json(&app, "/api/sim/fault", kill).await.0, StatusCode::BAD_REQUEST);

        let add_hosts = json!({"AddHosts": {"profile": "Pi5", "count": 1000, "misconfigure": null}});
        let (status, body) = post_json(&app, "/api/sim/scenario", add_hosts.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "more hosts than fit: {body}");

        post_json(&app, "/api/sim/speed", json!({"speed": 0})).await;
        let (status, _) = post_json(&app, "/api/sim/scenario", add_hosts).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn a_preset_body_restarts_on_that_fleet() {
        let driver = start();
        wait_for_leader(&driver).await;
        let app = app(driver.clone());
        let (status, _) = post_json(&app, "/api/sim/scenario", json!({"preset": "scale500"})).await;
        assert_eq!(status, StatusCode::OK);
        let mut rx = driver.subscribe();
        let hosts = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let n = rx.borrow_and_update().truth.hosts.len();
                if n == 500 {
                    return n;
                }
                rx.changed().await.unwrap();
            }
        })
        .await
        .expect("the 500-host fleet is published");
        assert_eq!(hosts, 500);
        assert_eq!(post_json(&app, "/api/sim/scenario", json!({"preset": "huge"})).await.0, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn ops_in_flight_when_the_sim_pauses_get_409_at_once() {
        let driver = start();
        let truth = wait_for_leader(&driver).await.truth.clone();
        let app = app(driver);
        // Without a quorum the op stays pending; at 1x its 5 sim second timeout is far off.
        for h in truth.hosts.iter().filter(|h| h.cp_role.is_some()).take(2) {
            post_json(&app, "/api/sim/fault", serde_json::to_value(Fault::KillHost { host: h.id.clone() }).unwrap()).await;
        }
        post_json(&app, "/api/sim/speed", json!({"speed": 1})).await;
        let pending = tokio::spawn({
            let app = app.clone();
            async move { post_json(&app, "/api/ops", op(ApiOp::CreateJoinToken)).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!pending.is_finished(), "the op is waiting on the control plane");
        post_json(&app, "/api/sim/speed", json!({"speed": 0})).await;
        let (status, body) = tokio::time::timeout(Duration::from_secs(1), pending).await.expect("answered at once").unwrap();
        assert_eq!((status, body), (StatusCode::CONFLICT, json!({"error": "simulation paused"})));
    }

    #[tokio::test]
    async fn unknown_api_paths_are_404_not_the_ui() {
        let app = app(start());
        for path in ["/api", "/api/nope", "/api/sim/nope"] {
            let res = app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }
}
