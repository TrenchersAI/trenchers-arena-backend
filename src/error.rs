//! Errors returned by the gateway itself. They use the platform API's error
//! body, `{ "error", "code", "category" }`, so the app handles both the same
//! way. Upstream error responses are passed through as they are.

use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::routes::Upstream;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// The method and path are not on the allowlist.
    #[error("route not found: {method} {path}")]
    RouteNotFound { method: Method, path: String },

    /// A path parameter failed its check, so nothing was forwarded.
    #[error("invalid path parameter `{name}`: expected {expected}")]
    InvalidPathParam {
        name: &'static str,
        expected: &'static str,
    },

    #[error("request body exceeds {limit} bytes")]
    PayloadTooLarge { limit: usize },

    #[error("request body could not be read")]
    UnreadableBody,

    #[error("request body was not received in time")]
    BodyTimeout,

    /// The upstream did not finish answering within the upstream timeout.
    #[error("{upstream} did not answer in time")]
    UpstreamTimeout {
        upstream: Upstream,
        #[source]
        source: reqwest::Error,
    },

    /// The upstream could not be reached, or the exchange broke off.
    #[error("{upstream} is unavailable")]
    UpstreamUnavailable {
        upstream: Upstream,
        #[source]
        source: reqwest::Error,
    },

    #[error("{upstream} response exceeds {limit} bytes")]
    UpstreamTooLarge { upstream: Upstream, limit: usize },
}

impl GatewayError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::RouteNotFound { .. } => StatusCode::NOT_FOUND,
            Self::InvalidPathParam { .. } | Self::UnreadableBody => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::BodyTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::UpstreamTimeout { .. }
            | Self::UpstreamUnavailable { .. }
            | Self::UpstreamTooLarge { .. } => StatusCode::BAD_GATEWAY,
        }
    }

    /// Short name for logs. Unlike the message, it never contains request data.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RouteNotFound { .. } => "route_not_found",
            Self::InvalidPathParam { .. } => "invalid_path_param",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::UnreadableBody => "unreadable_body",
            Self::BodyTimeout => "body_timeout",
            Self::UpstreamTimeout { .. } => "upstream_timeout",
            Self::UpstreamUnavailable { .. } => "upstream_unavailable",
            Self::UpstreamTooLarge { .. } => "upstream_too_large",
        }
    }
}

/// Same mapping as the platform API: 500 is `internal`, other 5xx are
/// `upstream`, 4xx are `validation`.
pub fn category(status: StatusCode) -> &'static str {
    match status {
        StatusCode::INTERNAL_SERVER_ERROR => "internal",
        s if s.is_server_error() => "upstream",
        _ => "validation",
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    code: u16,
    category: &'static str,
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorBody {
            // Transport details in `source` go to the logs, not to the client.
            error: self.to_string(),
            code: status.as_u16(),
            category: category(status),
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    async fn body_of(error: GatewayError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("collect body");
        (status, serde_json::from_slice(&bytes).expect("JSON body"))
    }

    /// A real `reqwest::Error`, needed to build the upstream variants.
    async fn connection_refused() -> reqwest::Error {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("address");
        drop(listener);
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client builds")
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect_err("nothing listens there")
    }

    #[tokio::test]
    async fn should_answer_in_the_api_error_shape() {
        let (status, body) = body_of(GatewayError::RouteNotFound {
            method: Method::GET,
            path: "/admin/settings".into(),
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            body,
            json!({
                "error": "route not found: GET /admin/settings",
                "code": 404,
                "category": "validation",
            })
        );
    }

    #[tokio::test]
    async fn should_name_the_parameter_but_not_echo_its_value() {
        let (status, body) = body_of(GatewayError::InvalidPathParam {
            name: "id",
            expected: "a UUID",
        })
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "invalid path parameter `id`: expected a UUID"
        );
        assert_eq!(body["category"], "validation");
    }

    #[tokio::test]
    async fn should_answer_upstream_failures_with_502_upstream_and_no_transport_detail() {
        for error in [
            GatewayError::UpstreamUnavailable {
                upstream: Upstream::Platform,
                source: connection_refused().await,
            },
            GatewayError::UpstreamTimeout {
                upstream: Upstream::Indexer,
                source: connection_refused().await,
            },
            GatewayError::UpstreamTooLarge {
                upstream: Upstream::Platform,
                limit: 10,
            },
        ] {
            let (status, body) = body_of(error).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert_eq!(body["code"], 502);
            assert_eq!(body["category"], "upstream");
            let message = body["error"].as_str().expect("message");
            assert!(
                message.starts_with("platform-api ") || message.starts_with("indexer-api "),
                "the message names the upstream: {message}"
            );
            assert!(
                !message.contains("127.0.0.1"),
                "leaked transport detail: {message}"
            );
        }
    }

    #[test]
    fn should_map_statuses_to_categories_like_the_api() {
        assert_eq!(category(StatusCode::INTERNAL_SERVER_ERROR), "internal");
        for status in [
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert_eq!(category(status), "upstream", "{status}");
        }
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::NOT_FOUND,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::PAYLOAD_TOO_LARGE,
        ] {
            assert_eq!(category(status), "validation", "{status}");
        }
    }

    #[test]
    fn should_give_every_gateway_error_its_status() {
        assert_eq!(
            GatewayError::PayloadTooLarge { limit: 1 }.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            GatewayError::UnreadableBody.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            GatewayError::BodyTimeout.status(),
            StatusCode::REQUEST_TIMEOUT
        );
    }
}
