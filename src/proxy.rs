//! Forwards an allowlisted request to its upstream and passes the response
//! back unchanged.

use std::error::Error as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, HttpBody};
use axum::extract::Request;
use axum::http::header::{self, HeaderMap, HeaderName};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use reqwest::Url;
use tokio::sync::Semaphore;

use crate::config::{Config, StreamLimits};
use crate::error::GatewayError;
use crate::routes::{Delivery, Route, Upstream};
use crate::{stream, validate};

/// Same request body limit as the platform API.
pub const MAX_REQUEST_BODY_BYTES: usize = 1_048_576;

/// Limit on a buffered upstream response, to bound memory use.
pub const MAX_RESPONSE_BODY_BYTES: usize = 32 * 1_048_576;

pub const USER_AGENT: &str = concat!("trenchers-arena-backend/", env!("CARGO_PKG_VERSION"));

/// Request headers copied upstream. Everything else is dropped, including
/// cookies and client-supplied `X-Forwarded-*` / `CF-*` headers, which the
/// platform API would treat as the client's address.
///
/// `Authorization` goes to the platform API only. The hood API serves public
/// market data and never reads it, so a session token has no reason to go
/// there.
///
/// reqwest adds `Host`, `User-Agent` and `Content-Length`, plus `Accept: */*`
/// when the client sent no `Accept`.
pub static FORWARDED_REQUEST_HEADERS: [HeaderName; 4] = [
    header::AUTHORIZATION,
    header::CONTENT_TYPE,
    header::ACCEPT,
    HeaderName::from_static("idempotency-key"),
];

/// Response headers passed back to the client. hyper adds `Content-Length`
/// and `Date`. `Retry-After` tells the app when to retry a 429 or 503.
pub static RETURNED_RESPONSE_HEADERS: [HeaderName; 3] = [
    header::CONTENT_TYPE,
    header::CACHE_CONTROL,
    header::RETRY_AFTER,
];

#[derive(Clone)]
pub struct Proxy {
    client: reqwest::Client,
    platform: Url,
    hood: Option<Url>,
    /// `UPSTREAM_TIMEOUT_MS`: the request body read, and every upstream call
    /// whose route sets no longer timeout.
    timeout: Duration,
    streams: StreamLimits,
    /// The AI limit: one permit per open stream, held by its body.
    stream_permits: Arc<Semaphore>,
}

impl Proxy {
    pub fn new(config: &Config) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            // Following a redirect would send the caller's token to whatever
            // host `Location` points at.
            .redirect(reqwest::redirect::Policy::none())
            // Ignore HTTP(S)_PROXY: requests carrying tokens go straight to
            // the upstream.
            .no_proxy()
            .build()?;
        Ok(Self {
            client,
            platform: config.platform_api_url.clone(),
            hood: config.hood_api_url.clone(),
            timeout: config.upstream_timeout,
            streams: config.streams,
            stream_permits: Arc::new(Semaphore::new(config.streams.concurrency)),
        })
    }

    /// The upstream timeout for `route`: its own when that is longer than the
    /// default, so raising `UPSTREAM_TIMEOUT_MS` never shortens a route.
    pub fn timeout_for(&self, route: &Route) -> Duration {
        route
            .timeout
            .map_or(self.timeout, |timeout| timeout.max(self.timeout))
    }

    /// Forwards `request` and logs one line for it, without headers, bodies or
    /// query strings.
    pub async fn forward(&self, route: &'static Route, request: Request) -> Response {
        let started = Instant::now();
        let method = request.method().clone();
        let outcome = self.exchange(route, request).await;
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        match outcome {
            Ok(response) => {
                tracing::info!(
                    %method,
                    route = route.path,
                    upstream = %route.upstream,
                    upstream_status = response.status().as_u16(),
                    latency_ms,
                    "forwarded"
                );
                response
            }
            Err(error) => {
                let status = error.status().as_u16();
                if error.status().is_server_error() {
                    tracing::warn!(
                        %method,
                        route = route.path,
                        upstream = %route.upstream,
                        status,
                        kind = error.kind(),
                        error = %chain(&error),
                        latency_ms,
                        "upstream call failed"
                    );
                } else {
                    tracing::info!(
                        %method,
                        route = route.path,
                        status,
                        kind = error.kind(),
                        latency_ms,
                        "refused"
                    );
                }
                error.into_response()
            }
        }
    }

    async fn exchange(
        &self,
        route: &'static Route,
        request: Request,
    ) -> Result<Response, GatewayError> {
        let (parts, body) = request.into_parts();
        let not_found = || GatewayError::RouteNotFound {
            method: parts.method.clone(),
            path: parts.uri.path().to_owned(),
        };
        // axum routes HEAD to GET handlers; only the listed method is allowed.
        if parts.method != route.verb.method() {
            return Err(not_found());
        }
        let params = route.params(parts.uri.path()).ok_or_else(not_found)?;
        for (name, raw) in &params {
            validate::path_param(name, raw)?;
        }
        let path = route.upstream_path_for(&params).ok_or_else(not_found)?;
        let mut url = match route.upstream {
            Upstream::Platform => self.platform.clone(),
            Upstream::Hood => self
                .hood
                .clone()
                .ok_or(GatewayError::UpstreamNotConfigured {
                    upstream: Upstream::Hood,
                })?,
        };
        let body = self.read_body(&parts.headers, body).await?;

        // set_query may percent-encode a few bytes (a raw `'`, for example);
        // they decode to the same value.
        url.set_path(&path);
        url.set_query(parts.uri.query());

        let streaming = route.delivery == Delivery::Stream;
        // Taken without waiting: a queued stream would use up the app's wait
        // for the first byte. A stream's body holds it until the stream ends.
        let permit = if streaming {
            let permit = Arc::clone(&self.stream_permits)
                .try_acquire_owned()
                .map_err(|_| GatewayError::StreamsBusy)?;
            Some(permit)
        } else {
            None
        };
        // Covers connecting, the response headers and the body. A stream's
        // headers get a shorter limit below.
        let timeout = if streaming {
            self.streams.max_duration
        } else {
            self.timeout_for(route)
        };

        let mut upstream = self
            .client
            .request(parts.method.clone(), url)
            .timeout(timeout);
        for name in &FORWARDED_REQUEST_HEADERS {
            if name == header::AUTHORIZATION && route.upstream == Upstream::Hood {
                continue;
            }
            for value in parts.headers.get_all(name) {
                let mut value = value.clone();
                value.set_sensitive(name == header::AUTHORIZATION);
                upstream = upstream.header(name, value);
            }
        }
        if body.is_empty() && parts.method != Method::GET {
            // hyper sends no Content-Length for an empty body; send 0 like
            // mobile HTTP clients do.
            upstream = upstream.header(header::CONTENT_LENGTH, 0);
        }
        let send = upstream.body(body).send();
        let sent = if streaming {
            tokio::time::timeout(self.streams.headers_timeout, send)
                .await
                .map_err(|_| GatewayError::UpstreamHeadersTimeout {
                    upstream: route.upstream,
                })?
        } else {
            send.await
        };
        let mut response = sent.map_err(|source| upstream_error(route.upstream, source))?;

        // Anything but an event stream (a 429, a JSON refusal) is read whole,
        // still holding the permit.
        let _permit = match permit {
            Some(permit) if stream::is_event_stream(&response) => {
                let idle = self.streams.idle_timeout;
                return Ok(stream::respond(route, response, permit, idle));
            }
            permit => permit,
        };

        let status = response.status();
        let mut headers = HeaderMap::new();
        for name in &RETURNED_RESPONSE_HEADERS {
            for value in response.headers().get_all(name) {
                headers.append(name.clone(), value.clone());
            }
        }
        let bytes = read_upstream_body(&mut response, route.upstream).await?;
        let mut out = Response::new(Body::from(bytes));
        *out.status_mut() = status;
        *out.headers_mut() = headers;
        Ok(out)
    }

    /// Reads the request body up to the size limit, within the default timeout
    /// even on routes with a longer one, so a slow client can't hold a
    /// concurrency permit for long. Nothing has gone upstream yet, so the 408
    /// is always safe to retry.
    async fn read_body(&self, headers: &HeaderMap, body: Body) -> Result<Vec<u8>, GatewayError> {
        let declared = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if declared.is_some_and(|len| len > MAX_REQUEST_BODY_BYTES as u64) {
            return Err(GatewayError::PayloadTooLarge {
                limit: MAX_REQUEST_BODY_BYTES,
            });
        }
        tokio::time::timeout(self.timeout, collect_capped(body))
            .await
            .map_err(|_| GatewayError::BodyTimeout)?
    }
}

async fn collect_capped(body: Body) -> Result<Vec<u8>, GatewayError> {
    let mut body = std::pin::pin!(body);
    let mut bytes = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = frame.map_err(|_| GatewayError::UnreadableBody)?;
        // Trailers are not forwarded.
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len() + data.len() > MAX_REQUEST_BODY_BYTES {
            return Err(GatewayError::PayloadTooLarge {
                limit: MAX_REQUEST_BODY_BYTES,
            });
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}

async fn read_upstream_body(
    response: &mut reqwest::Response,
    upstream: Upstream,
) -> Result<Vec<u8>, GatewayError> {
    let too_large = || GatewayError::UpstreamTooLarge {
        upstream,
        limit: MAX_RESPONSE_BODY_BYTES,
    };
    let declared = response.content_length().unwrap_or(0);
    if declared > MAX_RESPONSE_BODY_BYTES as u64 {
        return Err(too_large());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(declared).unwrap_or(0));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| upstream_error(upstream, source))?
    {
        if bytes.len() + chunk.len() > MAX_RESPONSE_BODY_BYTES {
            return Err(too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn upstream_error(upstream: Upstream, source: reqwest::Error) -> GatewayError {
    // Drop the URL so ids and query strings stay out of the logs.
    let source = source.without_url();
    if source.is_timeout() {
        GatewayError::UpstreamTimeout { upstream, source }
    } else {
        GatewayError::UpstreamUnavailable { upstream, source }
    }
}

/// Formats an error and its sources as `error: cause: cause`.
fn chain(error: &GatewayError) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::http::StatusCode;
    use wiremock::matchers::any;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::routes::{Delivery, Limit, Verb};

    /// A money route with a short timeout, so tests can run past it.
    static SLOW_TRADE: Route = Route {
        verb: Verb::Post,
        path: "/slow",
        upstream: Upstream::Platform,
        upstream_path: "/slow",
        limit: Some(Limit::Trade),
        timeout: Some(Duration::from_secs(1)),
        delivery: Delivery::Buffered,
    };

    fn proxy(platform: &str, default: Duration) -> Proxy {
        let vars = HashMap::from([
            ("PLATFORM_API_URL", platform.to_owned()),
            ("UPSTREAM_TIMEOUT_MS", default.as_millis().to_string()),
        ]);
        let config = Config::from_lookup(|name| vars.get(name).cloned()).expect("valid config");
        Proxy::new(&config).expect("the client builds")
    }

    #[test]
    fn should_never_use_a_route_timeout_shorter_than_the_default() {
        let route = |timeout| Route {
            timeout,
            ..SLOW_TRADE
        };
        let secs = Duration::from_secs;
        let cases = [
            (None, secs(15), secs(15)),
            (Some(secs(35)), secs(15), secs(35)),
            (Some(secs(35)), secs(60), secs(60)),
        ];
        for (timeout, default, expected) in cases {
            let proxy = proxy("http://127.0.0.1:1", default);
            assert_eq!(
                proxy.timeout_for(&route(timeout)),
                expected,
                "route {timeout:?}, default {default:?}"
            );
        }
    }

    /// With a 100ms default, a 1s route still waits 400ms for the upstream,
    /// and still gives up after its own timeout.
    #[tokio::test]
    async fn should_wait_for_the_route_timeout_and_no_longer() {
        let upstream = MockServer::start().await;
        let proxy = proxy(&upstream.uri(), Duration::from_millis(100));
        let send = || {
            proxy.forward(
                &SLOW_TRADE,
                Request::post("/slow").body(Body::empty()).expect("request"),
            )
        };

        Mock::given(any())
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(r#"{"ok":true}"#, "application/json")
                    .set_delay(Duration::from_millis(400)),
            )
            .mount(&upstream)
            .await;
        let response = send().await;
        assert_eq!(response.status(), StatusCode::OK);

        upstream.reset().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
            .mount(&upstream)
            .await;
        let started = Instant::now();
        let response = send().await;
        let waited = started.elapsed();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(
            waited >= Duration::from_secs(1) && waited < Duration::from_millis(2500),
            "gave up after {waited:?}"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON body");
        assert_eq!(
            body,
            serde_json::json!({
                "error": "platform-api did not answer in time",
                "code": 502,
                "category": "upstream",
            })
        );
    }
}
