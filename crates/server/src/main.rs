//! The homelab cluster manager's server: one process running the simulated
//! fleet, its HTTP API and WebSocket, and the built UI.

mod api;
mod driver;
mod ws;

use axum::Router;
use axum::routing::get;
use driver::DriverHandle;
use sim::presets::Preset;
use std::path::{Path, PathBuf};
use tower_http::services::{ServeDir, ServeFile};

const DEFAULT_PORT: u16 = 8080;

/// The port to listen on: `$PORT`, else 8080. The server is a local tool, so it only listens on loopback.
fn listen_addr(port: Option<&str>) -> Result<(std::net::Ipv4Addr, u16), String> {
    let port = match port {
        None => DEFAULT_PORT,
        Some(p) => p.parse().map_err(|_| format!("PORT must be a port number, got {p:?}"))?,
    };
    Ok((std::net::Ipv4Addr::LOCALHOST, port))
}

/// Where `npm run build` leaves the UI: `ui/dist` at the workspace root.
fn dist_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui/dist")
}

/// Adds the built UI to `router`. Unknown paths get `index.html` so client-side
/// routes survive a reload. Without a build, `/` explains what to do.
fn with_ui(router: Router, dist: &Path) -> Router {
    let index = dist.join("index.html");
    if index.is_file() {
        return router.fallback_service(ServeDir::new(dist).fallback(ServeFile::new(index)));
    }
    let hint = format!("The UI is not built: run ./run.sh (expected {}).\n", index.display());
    router.route("/", get(move || async move { hint }))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let driver = DriverHandle::spawn(Preset::Default, 1);
    let app = with_ui(api::app(driver), &dist_dir());
    let addr = listen_addr(std::env::var("PORT").ok().as_deref())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("homelab cluster manager on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn get_text(app: &Router, path: &str) -> (StatusCode, String) {
        let res = app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
        let status = res.status();
        (status, String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap())
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("homelab-server-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_port_comes_from_the_environment() {
        let lo = std::net::Ipv4Addr::LOCALHOST;
        assert_eq!(listen_addr(None), Ok((lo, 8080)));
        assert_eq!(listen_addr(Some("9123")), Ok((lo, 9123)));
        assert!(listen_addr(Some("http")).is_err());
        assert!(listen_addr(Some("70000")).is_err());
    }

    #[tokio::test]
    async fn a_missing_ui_build_gets_a_hint_instead_of_a_crash() {
        let missing = scratch_dir("missing").join("dist");
        let (status, text) = get_text(&with_ui(Router::new(), &missing), "/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(text.contains("not built"), "{text}");
    }

    #[tokio::test]
    async fn the_built_ui_is_served_with_index_html_as_the_fallback() {
        let dist = scratch_dir("built");
        std::fs::write(dist.join("index.html"), "<h1>ui</h1>").unwrap();
        std::fs::write(dist.join("app.js"), "console.log(1)").unwrap();
        let app = with_ui(Router::new(), &dist);
        assert_eq!(get_text(&app, "/app.js").await, (StatusCode::OK, "console.log(1)".into()));
        assert_eq!(get_text(&app, "/").await, (StatusCode::OK, "<h1>ui</h1>".into()));
        assert_eq!(get_text(&app, "/hosts/ms01-01").await, (StatusCode::OK, "<h1>ui</h1>".into()));
    }
}
