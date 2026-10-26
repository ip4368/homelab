//! The homelab cluster manager's server: the UI's client to the control
//! plane of the real VM lab, the lab itself, the HTTP API and WebSocket, and
//! the built UI. Runs as root (the lab needs it).

mod api;
mod driver;
mod ws;

use axum::Router;
use axum::routing::get;
use driver::DriverHandle;
use lab::manager::{Lab, LabConfig};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use tower_http::services::{ServeDir, ServeFile};

const DEFAULT_PORT: u16 = 8080;

/// Where to listen: `$BIND` (an IP address, default loopback) and `$PORT`
/// (default 8080). Loopback by default because the API is unauthenticated
/// and can power off the lab's VMs.
fn listen_addr(bind: Option<&str>, port: Option<&str>) -> Result<SocketAddr, String> {
    let ip = match bind {
        None => IpAddr::V4(Ipv4Addr::LOCALHOST),
        Some(b) => b.parse().map_err(|_| format!("BIND must be an IP address (e.g. 0.0.0.0), got {b:?}"))?,
    };
    let port = match port {
        None => DEFAULT_PORT,
        Some(p) => p.parse().map_err(|_| format!("PORT must be a port number, got {p:?}"))?,
    };
    Ok(SocketAddr::new(ip, port))
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
    let lab = Lab::start(LabConfig::from_env()).await?;
    let client = lab::client::spawn()?;
    let driver = DriverHandle::start(client, Some(lab.clone()));
    let up = driver.clone();
    tokio::spawn(async move {
        if let Err(e) = up.bring_up().await {
            eprintln!("lab: bring-up: {e}");
        }
    });
    let stop = lab.clone();
    tokio::spawn(async move {
        // Ctrl-C or a plain `kill`: stop the lab's daemons either way.
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        stop.stop_services();
        std::process::exit(0);
    });
    let app = with_ui(api::app(driver), &dist_dir());
    let addr = listen_addr(std::env::var("BIND").ok().as_deref(), std::env::var("PORT").ok().as_deref())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("homelab cluster manager on http://{}", listener.local_addr()?);
    if !addr.ip().is_loopback() {
        eprintln!("warning: listening beyond loopback; anyone who can reach {addr} can control the lab (there is no authentication)");
    }
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
    fn the_address_comes_from_the_environment() {
        let lo = |port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        assert_eq!(listen_addr(None, None), Ok(lo(8080)));
        assert_eq!(listen_addr(None, Some("9123")), Ok(lo(9123)));
        assert_eq!(listen_addr(Some("0.0.0.0"), Some("18080")), Ok("0.0.0.0:18080".parse().unwrap()));
        assert_eq!(listen_addr(Some("::"), None), Ok("[::]:8080".parse().unwrap()));
        assert!(listen_addr(None, Some("http")).is_err());
        assert!(listen_addr(None, Some("70000")).is_err());
        assert!(listen_addr(Some("devvm"), None).is_err(), "an address, not a host name");
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
