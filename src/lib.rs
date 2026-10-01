//! Backend for the Arena mobile app: an HTTP gateway in front of the
//! platform API and the indexer API.

pub mod config;
pub mod error;
pub mod routes;
pub mod validate;

pub use config::Config;

/// Builds the gateway's axum router.
pub fn app(_config: &Config) -> axum::Router {
    routes::router()
}
