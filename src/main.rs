use std::net::SocketAddr;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use trenchers_arena_backend::{app, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env().context("invalid configuration")?;
    let app = app(&config).context("could not build the upstream HTTP client")?;
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("could not listen on {addr}"))?;
    tracing::info!(
        %addr,
        platform_api = %config.platform_api_url,
        upstream_timeout_ms = config.upstream_timeout.as_millis(),
        version = env!("CARGO_PKG_VERSION"),
        "trenchers-arena-backend listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;
    tracing::info!("stopped");
    Ok(())
}

/// Resolves on Ctrl-C or SIGTERM, so in-flight requests finish before exit.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutting down");
}
