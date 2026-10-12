//! Integration tests for the streamed route, `POST /bots/{id}/ai/insight/stream`:
//! the gateway on a local port in front of a small axum upstream whose test
//! decides each answer and feeds an event stream chunk by chunk.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method};
use axum::response::{IntoResponse, Response};
use axum::Router;
use http_body::Frame;
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use trenchers_arena_backend::stream::CUT_NOTICE;
use trenchers_arena_backend::{app, Config, StreamLimits};

const BOT: &str = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
const INSIGHT_BODY: &str = r#"{"message":"Give me a two-line briefing on this bot."}"#;
const TEXT: &str = "event: text\ndata: {\"delta\":\"Your bot \"}\n\n";
const DONE: &str = "event: done\ndata: {\"reply\":\"Your bot is up 3% today.\"}\n\n";
const KEEP_ALIVE: &str = ":\n\n";

/// Generous for the first byte, idle gap and length, so only the limit a
/// test lowers can fire.
fn limits() -> StreamLimits {
    StreamLimits {
        headers_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        max_duration: Duration::from_secs(30),
        concurrency: 4,
    }
}

// ---------------------------------------------------------------------------
// The upstream
// ---------------------------------------------------------------------------

/// One request that reached the upstream, waiting for the test's answer.
struct Exchange {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
    respond: oneshot::Sender<Response>,
}

impl Exchange {
    fn respond(self, response: Response) {
        let _ = self.respond.send(response);
    }
}

struct Upstream {
    url: String,
    exchanges: mpsc::UnboundedReceiver<Exchange>,
}

impl Upstream {
    async fn start() -> Self {
        let (tx, exchanges) = mpsc::unbounded_channel();
        let router = Router::new().fallback(handle).with_state(tx);
        Self {
            url: listen(router).await,
            exchanges,
        }
    }

    async fn next(&mut self) -> Exchange {
        tokio::time::timeout(Duration::from_secs(5), self.exchanges.recv())
            .await
            .expect("a request reaches the upstream")
            .expect("the upstream is running")
    }

    fn assert_no_request(&mut self) {
        assert!(
            self.exchanges.try_recv().is_err(),
            "the request must not reach the upstream"
        );
    }
}

async fn handle(State(tx): State<mpsc::UnboundedSender<Exchange>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1 << 20)
        .await
        .expect("read the forwarded body");
    let (respond, answer) = oneshot::channel();
    let _ = tx.send(Exchange {
        method: parts.method,
        path: parts.uri.path().to_owned(),
        headers: parts.headers,
        body,
        respond,
    });
    answer
        .await
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// The upstream's half of an event stream.
struct Feed {
    chunks: mpsc::UnboundedSender<Result<Bytes, io::Error>>,
    dropped: oneshot::Receiver<()>,
}

impl Feed {
    fn send(&self, chunk: &str) {
        self.chunks
            .send(Ok(Bytes::copy_from_slice(chunk.as_bytes())))
            .expect("the stream is still open");
    }

    /// Breaks the upstream connection off mid-body.
    fn fail(&self) {
        self.chunks
            .send(Err(io::Error::other("upstream broke off")))
            .expect("the stream is still open");
    }

    /// Sends keep-alives until the stream is gone, as the AI service does. A
    /// server only notices a closed connection when it writes to it.
    fn keep_alive(&self, every: Duration) {
        let chunks = self.chunks.clone();
        tokio::spawn(async move {
            while chunks
                .send(Ok(Bytes::from_static(KEEP_ALIVE.as_bytes())))
                .is_ok()
            {
                tokio::time::sleep(every).await;
            }
        });
    }

    /// Whether the upstream's response body was dropped (the gateway went
    /// away) within `within`.
    async fn dropped_within(&mut self, within: Duration) -> bool {
        tokio::time::timeout(within, &mut self.dropped)
            .await
            .is_ok()
    }
}

/// A 200 event stream with the AI service's headers, fed through `Feed`.
fn event_stream() -> (Feed, Response) {
    let (chunks, rx) = mpsc::unbounded_channel();
    let (dropped_tx, dropped) = oneshot::channel();
    let body = ChannelBody {
        rx,
        dropped: Some(dropped_tx),
    };
    let response = Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .header("set-cookie", "upstream=1")
        .body(Body::new(body))
        .expect("response");
    (Feed { chunks, dropped }, response)
}

struct ChannelBody {
    rx: mpsc::UnboundedReceiver<Result<Bytes, io::Error>>,
    dropped: Option<oneshot::Sender<()>>,
}

impl HttpBody for ChannelBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        self.rx
            .poll_recv(cx)
            .map(|chunk| chunk.map(|chunk| chunk.map(Frame::data)))
    }
}

impl Drop for ChannelBody {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

fn json_response(status: u16, body: &'static str, retry_after: Option<&str>) -> Response {
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "application/json");
    if let Some(retry_after) = retry_after {
        response = response.header("retry-after", retry_after);
    }
    response.body(Body::from(body)).expect("response")
}

// ---------------------------------------------------------------------------
// The gateway and the app
// ---------------------------------------------------------------------------

async fn listen(router: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("listener address");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serves");
    });
    format!("http://{addr}")
}

struct Gateway {
    url: String,
    http: reqwest::Client,
}

impl Gateway {
    async fn start(upstream: &Upstream, streams: StreamLimits) -> Self {
        let api = upstream.url.clone();
        let mut config = Config::from_lookup(|name| match name {
            "PLATFORM_API_URL" => Some(api.clone()),
            _ => None,
        })
        .expect("test config is valid");
        config.streams = streams;
        let router = app(&config).expect("the gateway builds");
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test client builds");
        Self {
            url: listen(router).await,
            http,
        }
    }

    /// Asks for an insight the way the app does, in the background, so the
    /// test can answer it upstream.
    fn insight(&self, bot: &str) -> tokio::task::JoinHandle<reqwest::Response> {
        let request = self
            .http
            .post(format!("{}/bots/{bot}/ai/insight/stream", self.url))
            .header("authorization", "Bearer session.jwt")
            .header("accept", "text/event-stream")
            .header("content-type", "application/json")
            .body(INSIGHT_BODY);
        tokio::spawn(async move { request.send().await.expect("the gateway answers") })
    }
}

async fn answered(pending: tokio::task::JoinHandle<reqwest::Response>) -> reqwest::Response {
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("the gateway answers in time")
        .expect("join")
}

async fn next_chunk(response: &mut reqwest::Response) -> String {
    let chunk = tokio::time::timeout(Duration::from_secs(2), response.chunk())
        .await
        .expect("a chunk arrives in time")
        .expect("the stream is readable")
        .expect("the stream has not ended");
    String::from_utf8(chunk.to_vec()).expect("UTF-8")
}

/// The rest of the body, until the gateway ends it.
async fn rest(mut response: reqwest::Response) -> String {
    let mut text = String::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
            .await
            .expect("the stream ends in time")
            .expect("the stream ends cleanly");
        match chunk {
            Some(chunk) => text.push_str(std::str::from_utf8(&chunk).expect("UTF-8")),
            None => return text,
        }
    }
}

async fn json(response: reqwest::Response) -> Value {
    serde_json::from_slice(&response.bytes().await.expect("body")).expect("JSON body")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_first_event_reaches_the_app_before_the_stream_ends() {
    let mut upstream = Upstream::start().await;
    let gateway = Gateway::start(&upstream, limits()).await;
    let pending = gateway.insight(BOT);

    let exchange = upstream.next().await;
    assert_eq!(exchange.method, Method::POST);
    assert_eq!(exchange.path, format!("/bots/{BOT}/ai/insight/stream"));
    assert_eq!(exchange.headers["authorization"], "Bearer session.jwt");
    assert_eq!(exchange.headers["accept"], "text/event-stream");
    assert_eq!(exchange.headers["content-type"], "application/json");
    assert!(
        exchange.headers.get("accept-encoding").is_none(),
        "the gateway never asks for a compressed stream"
    );
    assert_eq!(exchange.body, INSIGHT_BODY);
    let (feed, response) = event_stream();
    exchange.respond(response);

    let mut response = answered(pending).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["cache-control"], "no-cache");
    assert_eq!(headers["x-accel-buffering"], "no");
    assert!(headers.get("content-length").is_none(), "{headers:?}");
    assert!(headers.get("set-cookie").is_none(), "{headers:?}");

    // The upstream is still open, so this chunk was not buffered.
    feed.send(TEXT);
    assert_eq!(next_chunk(&mut response).await, TEXT);
    feed.send(KEEP_ALIVE);
    assert_eq!(next_chunk(&mut response).await, KEEP_ALIVE);

    feed.send(DONE);
    drop(feed);
    assert_eq!(rest(response).await, DONE);
}

#[tokio::test]
async fn an_idle_stream_ends_with_an_error_event_and_lets_the_upstream_go() {
    let mut upstream = Upstream::start().await;
    let streams = StreamLimits {
        idle_timeout: Duration::from_millis(300),
        ..limits()
    };
    let gateway = Gateway::start(&upstream, streams).await;
    let pending = gateway.insight(BOT);
    let (mut feed, response) = event_stream();
    upstream.next().await.respond(response);
    let mut response = answered(pending).await;

    feed.send(TEXT);
    assert_eq!(next_chunk(&mut response).await, TEXT);
    let started = Instant::now();
    // Then nothing: the upstream stalls with the stream open.
    let text = rest(response).await;
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(text.as_bytes(), CUT_NOTICE);
    assert!(
        feed.dropped_within(Duration::from_secs(2)).await,
        "the gateway lets go of the upstream"
    );
}

/// The app throws on any `error` event once the stream ends, so one after
/// `done` would turn a complete answer into a failure.
#[tokio::test]
async fn a_stream_that_stalls_after_done_ends_without_an_error_event() {
    let mut upstream = Upstream::start().await;
    let streams = StreamLimits {
        idle_timeout: Duration::from_millis(300),
        ..limits()
    };
    let gateway = Gateway::start(&upstream, streams).await;
    for (label, chunks) in [
        (
            "done",
            vec![
                TEXT,
                "event: do",
                "ne\ndata: {\"reply\":",
                "\"Up 3%.\"}\n\n",
            ],
        ),
        (
            "error",
            vec![
                TEXT,
                "event:",
                " error\r\n",
                "data: {\"message\":\"Busy.\"}\r\n\r\n",
            ],
        ),
        // The final event's line is complete; its data is not.
        ("done, data cut", vec![TEXT, "event: done\n", "data: {\"re"]),
    ] {
        let pending = gateway.insight(BOT);
        let (feed, response) = event_stream();
        upstream.next().await.respond(response);
        let response = answered(pending).await;
        for chunk in &chunks {
            feed.send(chunk);
        }
        // The upstream never closes the stream.
        let text = rest(response).await;
        assert_eq!(text, chunks.concat(), "{label}");
        drop(feed);
    }
}

#[tokio::test]
async fn a_stream_past_its_maximum_length_ends_with_an_error_event() {
    let mut upstream = Upstream::start().await;
    let streams = StreamLimits {
        max_duration: Duration::from_millis(600),
        ..limits()
    };
    let gateway = Gateway::start(&upstream, streams).await;
    let started = Instant::now();
    let pending = gateway.insight(BOT);
    let (feed, response) = event_stream();
    upstream.next().await.respond(response);
    let response = answered(pending).await;

    // Never idle, never done.
    feed.send(TEXT);
    feed.keep_alive(Duration::from_millis(50));
    let text = rest(response).await;
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(600) && took < Duration::from_secs(3),
        "ended after {took:?}"
    );
    assert!(text.starts_with(TEXT), "{text:?}");
    assert!(
        text.ends_with(std::str::from_utf8(CUT_NOTICE).expect("UTF-8")),
        "{text:?}"
    );
    drop(feed);
}

#[tokio::test]
async fn an_upstream_that_breaks_off_mid_stream_ends_with_an_error_event() {
    let mut upstream = Upstream::start().await;
    let gateway = Gateway::start(&upstream, limits()).await;
    let pending = gateway.insight(BOT);
    let (feed, response) = event_stream();
    upstream.next().await.respond(response);
    let mut response = answered(pending).await;

    feed.send(TEXT);
    assert_eq!(next_chunk(&mut response).await, TEXT);
    feed.fail();
    let text = rest(response).await;
    assert_eq!(text.as_bytes(), CUT_NOTICE);
}

#[tokio::test]
async fn refusals_and_json_answers_come_back_whole_and_unchanged() {
    let mut upstream = Upstream::start().await;
    let gateway = Gateway::start(&upstream, limits()).await;
    let cases: [(u16, &'static str, Option<&str>); 4] = [
        (
            429,
            r#"{"error":"AI rate limit reached","code":429,"category":"validation"}"#,
            Some("7"),
        ),
        (
            503,
            r#"{"error":"AI is unavailable","code":503,"category":"upstream"}"#,
            Some("2"),
        ),
        (
            401,
            r#"{"error":"AI access is not enabled","code":401,"category":"validation"}"#,
            None,
        ),
        (200, r#"{"reply":"Not streamed."}"#, None),
    ];
    for (status, body, retry_after) in cases {
        let pending = gateway.insight(BOT);
        upstream
            .next()
            .await
            .respond(json_response(status, body, retry_after));
        let response = answered(pending).await;
        assert_eq!(response.status().as_u16(), status);
        let headers = response.headers();
        assert_eq!(headers["content-type"], "application/json", "{status}");
        assert_eq!(
            headers
                .get("retry-after")
                .map(|value| value.to_str().expect("ASCII")),
            retry_after,
            "{status}"
        );
        assert!(headers.get("x-accel-buffering").is_none(), "{status}");
        assert_eq!(
            headers["content-length"],
            body.len().to_string(),
            "{status}: read whole"
        );
        assert_eq!(response.text().await.expect("body"), body, "{status}");
    }
}

#[tokio::test]
async fn a_stream_is_cancelled_upstream_when_the_app_goes_away() {
    let mut upstream = Upstream::start().await;
    let gateway = Gateway::start(&upstream, limits()).await;
    let pending = gateway.insight(BOT);
    let (mut feed, response) = event_stream();
    upstream.next().await.respond(response);
    let mut response = answered(pending).await;

    feed.send(TEXT);
    assert_eq!(next_chunk(&mut response).await, TEXT);
    drop(response);
    drop(gateway);

    feed.keep_alive(Duration::from_millis(20));
    assert!(
        feed.dropped_within(Duration::from_secs(3)).await,
        "the upstream stream must be dropped, not read to the end"
    );
}

#[tokio::test]
async fn late_response_headers_answer_502_in_the_error_shape() {
    let mut upstream = Upstream::start().await;
    let streams = StreamLimits {
        headers_timeout: Duration::from_millis(300),
        ..limits()
    };
    let gateway = Gateway::start(&upstream, streams).await;
    let started = Instant::now();
    let pending = gateway.insight(BOT);
    // Reaches the upstream, which never answers.
    let _exchange = upstream.next().await;

    let response = answered(pending).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(2), "answered after {took:?}");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        json(response).await,
        json!({
            "error": "platform-api did not answer in time",
            "code": 502,
            "category": "upstream",
        })
    );
}

/// Each open stream holds a permit until it ends, not just until its headers.
/// A request over the limit gets a 503 at once, and other routes don't share
/// the limit.
#[tokio::test]
async fn a_full_ai_limit_answers_503_at_once_until_a_stream_ends() {
    let mut upstream = Upstream::start().await;
    let streams = StreamLimits {
        concurrency: 1,
        ..limits()
    };
    let gateway = Gateway::start(&upstream, streams).await;
    let pending = gateway.insight(BOT);
    let (feed, response) = event_stream();
    upstream.next().await.respond(response);
    let mut open = answered(pending).await;
    feed.send(TEXT);
    assert_eq!(next_chunk(&mut open).await, TEXT);

    let started = Instant::now();
    let refused = answered(gateway.insight(BOT)).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(refused).await,
        json!({
            "error": "too many AI answers in progress, retry shortly",
            "code": 503,
            "category": "upstream",
        })
    );
    upstream.assert_no_request();

    // Another group still gets through.
    let me = {
        let request = gateway.http.get(format!("{}/me", gateway.url));
        tokio::spawn(async move { request.send().await.expect("the gateway answers") })
    };
    upstream
        .next()
        .await
        .respond(json_response(200, r#"{"id":"me"}"#, None));
    assert_eq!(answered(me).await.status(), StatusCode::OK);

    feed.send(DONE);
    drop(feed);
    assert_eq!(rest(open).await, DONE);

    // The permit goes back when the gateway drops the finished body, just
    // after the app reads its end.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut pending = gateway.insight(BOT);
        tokio::select! {
            exchange = upstream.next() => {
                exchange.respond(json_response(429, r#"{"error":"slow down"}"#, None));
                assert_eq!(answered(pending).await.status(), StatusCode::TOO_MANY_REQUESTS);
                break;
            }
            response = &mut pending => {
                let status = response.expect("join").status();
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert!(Instant::now() < deadline, "the permit was never released");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

#[tokio::test]
async fn a_bad_bot_id_or_method_is_refused_without_reaching_the_upstream() {
    let mut upstream = Upstream::start().await;
    let gateway = Gateway::start(&upstream, limits()).await;
    // `stats` is a static `/bots/...` route, but here it sits where the id goes.
    for bot in ["stats", "not-a-uuid"] {
        let response = answered(gateway.insight(bot)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bot}");
    }
    let response = gateway
        .http
        .get(format!("{}/bots/{BOT}/ai/insight/stream", gateway.url))
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    upstream.assert_no_request();
}
