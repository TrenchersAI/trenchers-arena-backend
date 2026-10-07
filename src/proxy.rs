//! Forwards an allowlisted request to its upstream and passes the response
//! back unchanged.

use std::error::Error as _;
use std::time::{Duration, Instant};

use axum::body::{Body, HttpBody};
use axum::extract::Request;
use axum::http::header::{self, HeaderMap, HeaderName};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use reqwest::Url;

use crate::config::Config;
use crate::error::GatewayError;
use crate::routes::{Route, Upstream};
use crate::validate;

/// Same request body limit as the platform API.
pub const MAX_REQUEST_BODY_BYTES: usize = 1_048_576;

/// Limit on a buffered upstream response, to bound memory use.
pub const MAX_RESPONSE_BODY_BYTES: usize = 32 * 1_048_576;

pub const USER_AGENT: &str = concat!("trenchers-arena-backend/", env!("CARGO_PKG_VERSION"));

/// Request headers copied upstream. Everything else is dropped, including
/// cookies and client-supplied `X-Forwarded-*` / `CF-*` headers, which the
/// platform API would treat as the client's address.
///
/// `Authorization` goes to the platform API only. The indexer serves public
/// charts and never reads it, so a session token has no reason to go there.
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
/// and `Date`.
pub static RETURNED_RESPONSE_HEADERS: [HeaderName; 2] =
    [header::CONTENT_TYPE, header::CACHE_CONTROL];

#[derive(Clone)]
pub struct Proxy {
    client: reqwest::Client,
    platform: Url,
    indexer: Url,
    timeout: Duration,
}

impl Proxy {
    pub fn new(config: &Config) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            // Covers connecting, the response headers and the body.
            .timeout(config.upstream_timeout)
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
            indexer: config.indexer_api_url.clone(),
            timeout: config.upstream_timeout,
        })
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
        let body = self.read_body(&parts.headers, body).await?;

        let mut url = match route.upstream {
            Upstream::Platform => self.platform.clone(),
            Upstream::Indexer => self.indexer.clone(),
        };
        // set_query may percent-encode a few bytes (a raw `'`, for example);
        // they decode to the same value.
        url.set_path(&path);
        url.set_query(parts.uri.query());

        let mut upstream = self.client.request(parts.method.clone(), url);
        for name in &FORWARDED_REQUEST_HEADERS {
            if name == header::AUTHORIZATION && route.upstream == Upstream::Indexer {
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
        let mut response = upstream
            .body(body)
            .send()
            .await
            .map_err(|source| upstream_error(route.upstream, source))?;

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

    /// Reads the request body up to the size limit. The upstream timeout also
    /// applies, so a slow client can't hold a concurrency permit indefinitely.
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
