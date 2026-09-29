//! Backend for the Arena mobile app: an HTTP gateway in front of the
//! platform API and the indexer API.

pub mod config;

use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

pub use config::Config;

/// Builds the gateway's axum router.
pub fn app(_config: &Config) -> Router {
    Router::new().route("/health", get(health))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}
