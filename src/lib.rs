//! Backend for the Arena mobile app: an HTTP gateway that forwards
//! the routes in [`routes::ROUTES`] to the platform API and the Robinhood
//! Chain read API.

pub mod config;
pub mod error;
pub mod proxy;
pub mod routes;
pub mod stream;
pub mod validate;

pub use config::{Config, StreamLimits};

/// Builds the gateway's axum router.
pub fn app(config: &Config) -> Result<axum::Router, reqwest::Error> {
    Ok(routes::router(proxy::Proxy::new(config)?))
}
