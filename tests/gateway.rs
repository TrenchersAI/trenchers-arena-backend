//! Integration tests: the gateway on a local port in front of two wiremock
//! servers (platform API and indexer), checking what each one receives.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use trenchers_arena_backend::proxy::{MAX_REQUEST_BODY_BYTES, MAX_RESPONSE_BODY_BYTES};
use trenchers_arena_backend::routes::{Upstream, ROUTES};
use trenchers_arena_backend::{app, Config};

const BOT: &str = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
const OTHER_BOT: &str = "9f3c2a71-5d4e-4b8a-8c1f-2e6d7a9b0c34";
const ENTRANT: &str = "5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11";
const MINT: &str = "So11111111111111111111111111111111111111112";
const WALLET: &str = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
const ORDER: &str = "3c9e1f7a-8b2d-4e6f-a1c3-5d7e9f0b2a46";
const FUNDING: &str = "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2f";
const USER_AGENT: &str = concat!("trenchers-arena-backend/", env!("CARGO_PKG_VERSION"));

const PRIVY_BODY: &str = r#"{"privy_token":"privy.token.value","wallet_address":"9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"}"#;
const REFRESH_BODY: &str = r#"{"token":"session.token.value"}"#;
const CREATE_BOT_BODY: &str = r#"{"name":"Safe Flipper","config":{"sol_buy_amount_lamports":100000000,"exit_tiers":[{"mult_x":"2","pct_of_position":50,"slippage_override_bps":null}]},"paper_mode":true,"spend_limit_lamports":0,"required_acknowledged":["sol_buy_amount_lamports","exit_tiers"]}"#;
const CONFIG_BODY: &str = r#"{"config":{"max_hold_secs":600},"required_acknowledged":["max_hold_secs"],"change_note":"Updated in Arena"}"#;
const ALLOCATE_BODY: &str = r#"{"lamports":100000000}"#;
const SETTINGS_BODY: &str =
    r#"{"ui_prefs":{"tradingPresets":{"buy":[{"amount_sol":"0.1","slippage_bps":500}]}}}"#;
const TOKENS_META_BODY: &str = r#"{"mints":["So11111111111111111111111111111111111111112","EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]}"#;
const INSIGHT_BODY: &str = r#"{"message":"Give me a two-line briefing on this bot."}"#;
const DELEGATED_BODY: &str = r#"{"wallet_id":"cm7x2k9pq00a1b2c3d4e5f6g7"}"#;
const FUND_BODY: &str = r#"{"amount_lamports":250000000}"#;
const WITHDRAW_BODY: &str = r#"{"amount_lamports":"all"}"#;
const SWAP_BODY: &str = r#"{"input_mint":"So11111111111111111111111111111111111111112","output_mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","amount":"100000000","side":"buy","slippage_bps":1000,"mev_mode":"off","mev_protect":false,"auto_fee":false,"idempotency_key":"6f1e2d3c-4b5a-4968-8776-5a4b3c2d1e0f"}"#;
const SWAP_COMPLETE_BODY: &str = r#"{"intent_id":"8a7b6c5d-4e3f-4a2b-9c1d-0e9f8a7b6c5d","signature":"5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW","forwarded":false}"#;
const SELL_INIT_QUOTE_BODY: &str =
    r#"{"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","market_cap_usd":"1250000"}"#;
const ORDER_BODY: &str = r#"{"order_type":"limit","side":"buy","token_mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","amount":"100000000","slippage_bps":500,"trigger_price":"0.000042"}"#;
const SUBMIT_BODY: &str = r#"{"transaction":"AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="}"#;
const RPC_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"],{"searchTransactionHistory":true}]}"#;
const CLONE_KEY: &str = "clone-6a1f9c2e-entrant-5b0e8f0e";
const CLONE_BODY: &str =
    r#"{"name":"My Opus copy","idempotency_key":"clone-6a1f9c2e-entrant-5b0e8f0e"}"#;
const CLONE_RESPONSE: &str = r#"{"bot_id":"9f3c2a71-5d4e-4b8a-8c1f-2e6d7a9b0c34","provenance":{"entrant_id":"5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11","season_number":10,"template_name":"Safe Flipper","model_id":"0d6b7c55-3c1e-4f3a-9a8b-6e2f1c4d5a90","model_name":"claude-opus","model_label":"Opus","label":"Cloned · Opus · Safe Flipper · S10","cloned_at":"2026-10-07T12:00:00Z"}}"#;

/// An arena feed cursor as the app sends it, percent-encoded.
const FEED_CURSOR: &str =
    "2026-10-01T12%3A00%3A00.123456%2B00%3A00%7C5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11%7C0";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Gateway {
    base: String,
    api: MockServer,
    indexer: MockServer,
    http: reqwest::Client,
}

impl Gateway {
    async fn start() -> Self {
        Self::start_with_timeout(Duration::from_secs(5)).await
    }

    async fn start_with_timeout(timeout: Duration) -> Self {
        let api = MockServer::start().await;
        let indexer = MockServer::start().await;
        let base = serve(&api.uri(), &indexer.uri(), timeout).await;
        Self {
            base,
            api,
            indexer,
            http: test_client(),
        }
    }

    /// Both upstreams answer every request with `200 {"ok":true}`.
    async fn with_ok_upstreams() -> Self {
        let gateway = Self::start().await;
        for server in [&gateway.api, &gateway.indexer] {
            Mock::given(any())
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(r#"{"ok":true}"#, "application/json"),
                )
                .mount(server)
                .await;
        }
        gateway
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn send(&self, method: Method, path: &str) -> reqwest::Response {
        self.http
            .request(method, self.url(path))
            .send()
            .await
            .expect("the gateway answers")
    }

    /// The request a contract case describes, without an `Authorization`
    /// header.
    fn request(&self, case: &Case) -> reqwest::RequestBuilder {
        let request = self.http.request(case.method.clone(), self.url(&case.path));
        match case.body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(body),
            None => request,
        }
    }

    async fn received(&self, upstream: Upstream) -> Vec<Request> {
        let server = match upstream {
            Upstream::Platform => &self.api,
            Upstream::Indexer => &self.indexer,
        };
        server
            .received_requests()
            .await
            .expect("request recording is on")
    }

    async fn assert_no_upstream_calls(&self) {
        for upstream in [Upstream::Platform, Upstream::Indexer] {
            let received = self.received(upstream).await;
            assert!(
                received.is_empty(),
                "{upstream} must not be called, but received: {:?}",
                received
                    .iter()
                    .map(|r| format!("{} {}", r.method, r.url))
                    .collect::<Vec<_>>()
            );
        }
    }
}

/// Serves the gateway on an ephemeral port and returns its base URL.
async fn serve(api: &str, indexer: &str, timeout: Duration) -> String {
    let vars = HashMap::from([
        ("PLATFORM_API_URL", api.to_owned()),
        ("INDEXER_API_URL", indexer.to_owned()),
        ("UPSTREAM_TIMEOUT_MS", timeout.as_millis().to_string()),
    ]);
    let config = Config::from_lookup(|name| vars.get(name).cloned()).expect("test config is valid");
    let router = app(&config).expect("the gateway builds");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("listener address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("the gateway serves");
    });
    format!("http://{addr}")
}

fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("test client builds")
}

/// A URL on a port nothing listens on.
async fn closed_port_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("listener address");
    drop(listener);
    format!("http://{addr}")
}

/// Sends a raw HTTP/1.1 request, so the target isn't normalised the way a URL
/// parser would rewrite `..` or `%2e`. `head` must include `Connection: close`.
async fn raw_request(base: &str, head: &str, body: &[u8]) -> (u16, String) {
    let addr = base.trim_start_matches("http://");
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(head.as_bytes()).await.expect("write head");
    // The gateway may answer (and stop reading) before the body is complete.
    let _ = stream.write_all(body).await;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let text = String::from_utf8_lossy(&response).into_owned();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn raw_get(target: &str) -> String {
    format!("GET {target} HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
}

async fn json(response: reqwest::Response) -> Value {
    let bytes = response.bytes().await.expect("read body");
    serde_json::from_slice(&bytes).expect("JSON body")
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .get(name)
        .map(|value| value.to_str().expect("ASCII header"))
}

fn path_and_query(url: &reqwest::Url) -> String {
    match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    }
}

/// Errors the gateway produces itself carry the platform API error body:
/// `{ "error": <message>, "code": <status>, "category": <category> }`.
fn assert_error_shape(body: &Value, status: u16, category: &str) {
    let object = body.as_object().expect("the error body is a JSON object");
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        BTreeSet::from(["category", "code", "error"]),
        "error body keys: {body}"
    );
    assert!(
        object["error"].as_str().is_some_and(|m| !m.is_empty()),
        "error message: {body}"
    );
    assert_eq!(object["code"], status, "error code: {body}");
    assert_eq!(object["category"], category, "error category: {body}");
}

// ---------------------------------------------------------------------------
// The contract: one row per allowlisted route
// ---------------------------------------------------------------------------

/// One route: the request the app sends and what the upstream should get.
/// Written by hand instead of derived from `ROUTES`, so a wrong table entry
/// fails the test.
struct Case {
    method: Method,
    pattern: &'static str,
    path: String,
    body: Option<&'static str>,
    upstream: Upstream,
    upstream_path: String,
}

fn api(method: Method, pattern: &'static str, path: String, body: Option<&'static str>) -> Case {
    Case {
        method,
        pattern,
        upstream_path: path.clone(),
        path,
        body,
        upstream: Upstream::Platform,
    }
}

fn contract() -> Vec<Case> {
    use Method as M;
    vec![
        // Auth and account
        api(
            M::POST,
            "/auth/privy",
            "/auth/privy".into(),
            Some(PRIVY_BODY),
        ),
        api(
            M::POST,
            "/auth/refresh",
            "/auth/refresh".into(),
            Some(REFRESH_BODY),
        ),
        api(M::GET, "/me", "/me".into(), None),
        api(M::GET, "/paper-wallet", "/paper-wallet".into(), None),
        // Bots
        api(M::GET, "/bots", "/bots".into(), None),
        api(M::POST, "/bots", "/bots".into(), Some(CREATE_BOT_BODY)),
        api(M::GET, "/bots/defaults", "/bots/defaults".into(), None),
        api(
            M::GET,
            "/bots/stats",
            format!("/bots/stats?ids={BOT},{OTHER_BOT}&limit=50"),
            None,
        ),
        api(
            M::GET,
            "/bots/leaderboard",
            "/bots/leaderboard".into(),
            None,
        ),
        api(M::GET, "/bots/{id}", format!("/bots/{BOT}"), None),
        api(
            M::PUT,
            "/bots/{id}/config",
            format!("/bots/{BOT}/config"),
            Some(CONFIG_BODY),
        ),
        api(
            M::POST,
            "/bots/{id}/start",
            format!("/bots/{BOT}/start"),
            None,
        ),
        api(
            M::POST,
            "/bots/{id}/pause",
            format!("/bots/{BOT}/pause"),
            None,
        ),
        api(
            M::POST,
            "/bots/{id}/resume",
            format!("/bots/{BOT}/resume"),
            None,
        ),
        api(
            M::POST,
            "/bots/{id}/allocate",
            format!("/bots/{BOT}/allocate"),
            Some(ALLOCATE_BODY),
        ),
        api(
            M::GET,
            "/bots/{id}/stats",
            format!("/bots/{BOT}/stats"),
            None,
        ),
        api(
            M::GET,
            "/bots/{id}/positions",
            format!("/bots/{BOT}/positions"),
            None,
        ),
        api(
            M::GET,
            "/bots/{id}/trades",
            format!("/bots/{BOT}/trades?limit=50"),
            None,
        ),
        api(
            M::GET,
            "/bots/{id}/watching",
            format!("/bots/{BOT}/watching"),
            None,
        ),
        api(
            M::GET,
            "/bots/{id}/arena-clone",
            format!("/bots/{BOT}/arena-clone"),
            None,
        ),
        // Arena: public
        api(
            M::GET,
            "/arena/paper-bots",
            "/arena/paper-bots".into(),
            None,
        ),
        api(M::GET, "/arena/season", "/arena/season".into(), None),
        api(
            M::GET,
            "/arena/entrants/{id}",
            format!("/arena/entrants/{ENTRANT}"),
            None,
        ),
        api(
            M::GET,
            "/arena/feed",
            format!("/arena/feed?limit=30&before={FEED_CURSOR}"),
            None,
        ),
        api(M::GET, "/arena/seasons", "/arena/seasons".into(), None),
        api(
            M::GET,
            "/arena/seasons/{number}",
            "/arena/seasons/6".into(),
            None,
        ),
        // Arena: signed in
        api(
            M::GET,
            "/arena/entrants/{id}/positions",
            format!("/arena/entrants/{ENTRANT}/positions"),
            None,
        ),
        api(
            M::GET,
            "/arena/entrants/{id}/trades",
            format!("/arena/entrants/{ENTRANT}/trades?limit=500"),
            None,
        ),
        api(
            M::GET,
            "/arena/entrants/{id}/watching",
            format!("/arena/entrants/{ENTRANT}/watching"),
            None,
        ),
        api(
            M::GET,
            "/arena/entrants/{id}/config-history",
            format!("/arena/entrants/{ENTRANT}/config-history?limit=200"),
            None,
        ),
        api(
            M::GET,
            "/arena/entrants/{id}/transcript",
            format!("/arena/entrants/{ENTRANT}/transcript?runs=50"),
            None,
        ),
        api(
            M::POST,
            "/arena/entrants/{id}/clone",
            format!("/arena/entrants/{ENTRANT}/clone"),
            Some(CLONE_BODY),
        ),
        // Indexer: the one renamed route.
        Case {
            method: M::GET,
            pattern: "/tokens/{mint}/candles",
            path: format!("/tokens/{MINT}/candles?res=15&from=1759600000&to=1759686400"),
            body: None,
            upstream: Upstream::Indexer,
            upstream_path: format!("/terminal/{MINT}/candles?res=15&from=1759600000&to=1759686400"),
        },
        // Account settings
        api(M::GET, "/me/settings", "/me/settings".into(), None),
        api(
            M::PUT,
            "/me/settings",
            "/me/settings?return=minimal".into(),
            Some(SETTINGS_BODY),
        ),
        // Portfolio and trades
        api(M::GET, "/portfolio", "/portfolio".into(), None),
        api(
            M::GET,
            "/portfolio/holdings",
            "/portfolio/holdings".into(),
            None,
        ),
        api(
            M::GET,
            "/portfolio/realized",
            "/portfolio/realized".into(),
            None,
        ),
        api(
            M::GET,
            "/trades",
            format!("/trades?mint={MINT}&page_size=100"),
            None,
        ),
        api(
            M::GET,
            "/fees/recommendation",
            "/fees/recommendation".into(),
            None,
        ),
        api(
            M::GET,
            "/wallets/{address}/sol-balance",
            format!("/wallets/{WALLET}/sol-balance"),
            None,
        ),
        api(M::GET, "/bots/wallets", "/bots/wallets".into(), None),
        // Orders (reads)
        api(M::GET, "/orders", "/orders".into(), None),
        api(
            M::GET,
            "/orders/{id}/events",
            format!("/orders/{ORDER}/events"),
            None,
        ),
        // Public
        api(
            M::POST,
            "/public/tokens-meta",
            "/public/tokens-meta".into(),
            Some(TOKENS_META_BODY),
        ),
        // Orders (writes)
        api(M::DELETE, "/orders/{id}", format!("/orders/{ORDER}"), None),
        // AI, streamed. A JSON answer comes back whole, as here; streaming
        // is tested in `tests/streaming.rs`.
        api(
            M::POST,
            "/bots/{id}/ai/insight/stream",
            format!("/bots/{BOT}/ai/insight/stream"),
            Some(INSIGHT_BODY),
        ),
        // Live agent wallets
        api(
            M::POST,
            "/bots/live-confirm",
            "/bots/live-confirm".into(),
            Some("{}"),
        ),
        api(
            M::POST,
            "/bots/{id}/archive",
            format!("/bots/{BOT}/archive"),
            None,
        ),
        api(
            M::GET,
            "/bots/{id}/wallet",
            format!("/bots/{BOT}/wallet"),
            None,
        ),
        api(
            M::POST,
            "/bots/{id}/wallet",
            format!("/bots/{BOT}/wallet"),
            None,
        ),
        api(
            M::POST,
            "/bots/{id}/wallet/delegated",
            format!("/bots/{BOT}/wallet/delegated"),
            Some(DELEGATED_BODY),
        ),
        api(
            M::POST,
            "/bots/{id}/wallet/fund",
            format!("/bots/{BOT}/wallet/fund"),
            Some(FUND_BODY),
        ),
        api(
            M::POST,
            "/bots/{id}/wallet/withdraw",
            format!("/bots/{BOT}/wallet/withdraw"),
            Some(WITHDRAW_BODY),
        ),
        api(
            M::GET,
            "/bots/{id}/wallet/funding/{funding_id}",
            format!("/bots/{BOT}/wallet/funding/{FUNDING}"),
            None,
        ),
        // Trading
        api(
            M::POST,
            "/trade/swap",
            "/trade/swap".into(),
            Some(SWAP_BODY),
        ),
        api(
            M::POST,
            "/trade/swap/prepare",
            "/trade/swap/prepare".into(),
            Some(SWAP_BODY),
        ),
        api(
            M::POST,
            "/trade/swap/complete",
            "/trade/swap/complete".into(),
            Some(SWAP_COMPLETE_BODY),
        ),
        api(
            M::POST,
            "/trade/sell-init/quote",
            "/trade/sell-init/quote".into(),
            Some(SELL_INIT_QUOTE_BODY),
        ),
        api(M::POST, "/orders", "/orders".into(), Some(ORDER_BODY)),
        // Solana relay
        api(
            M::POST,
            "/solana/submit",
            "/solana/submit".into(),
            Some(SUBMIT_BODY),
        ),
        api(M::POST, "/solana/rpc", "/solana/rpc".into(), Some(RPC_BODY)),
        api(
            M::GET,
            "/solana/blockhash",
            "/solana/blockhash".into(),
            None,
        ),
    ]
}

#[test]
fn the_contract_covers_every_allowlisted_route_and_nothing_else() {
    let mut table: Vec<(String, &str, Upstream)> = ROUTES
        .iter()
        .map(|route| (route.verb.method().to_string(), route.path, route.upstream))
        .collect();
    let mut cases: Vec<(String, &str, Upstream)> = contract()
        .iter()
        .map(|case| (case.method.to_string(), case.pattern, case.upstream))
        .collect();
    table.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    cases.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    assert_eq!(table, cases);
}

#[tokio::test]
async fn every_route_reaches_its_upstream_with_method_path_query_and_body_intact() {
    let gateway = Gateway::with_ok_upstreams().await;
    for case in contract() {
        let label = format!("{} {}", case.method, case.path);
        let api_before = gateway.received(Upstream::Platform).await.len();
        let indexer_before = gateway.received(Upstream::Indexer).await.len();

        let mut request = gateway
            .http
            .request(case.method.clone(), gateway.url(&case.path))
            .header("authorization", "Bearer session.jwt")
            .header("accept", "application/json");
        if let Some(body) = case.body {
            request = request
                .header("content-type", "application/json")
                .body(body);
        }
        let response = request.send().await.expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::OK, "{label}");
        assert_eq!(
            response.text().await.expect("body"),
            r#"{"ok":true}"#,
            "{label}"
        );

        let api = gateway.received(Upstream::Platform).await;
        let indexer = gateway.received(Upstream::Indexer).await;
        let (hit, missed) = match case.upstream {
            Upstream::Platform => (&api[api_before..], &indexer[indexer_before..]),
            Upstream::Indexer => (&indexer[indexer_before..], &api[api_before..]),
        };
        assert_eq!(
            hit.len(),
            1,
            "{label}: exactly one call to {}",
            case.upstream
        );
        assert!(missed.is_empty(), "{label}: no call to the other upstream");
        let got = &hit[0];
        assert_eq!(got.method, case.method, "{label}: method");
        assert_eq!(
            path_and_query(&got.url),
            case.upstream_path,
            "{label}: path and query"
        );
        assert_eq!(
            got.body,
            case.body.unwrap_or_default().as_bytes(),
            "{label}: body"
        );
        // The indexer never reads a session token, so it never gets one.
        let authorization = match case.upstream {
            Upstream::Platform => Some("Bearer session.jwt"),
            Upstream::Indexer => None,
        };
        assert_eq!(
            header(got, "authorization"),
            authorization,
            "{label}: authorization"
        );
    }
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn only_allowlisted_request_headers_reach_the_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway
        .http
        .post(gateway.url(&format!("/bots/{BOT}/allocate")))
        // Forwarded:
        .header(
            "authorization",
            "Bearer eyJhbGciOiJIUzI1NiJ9.e30.c2lnbmF0dXJl",
        )
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("idempotency-key", "alloc-7f3a")
        // Dropped:
        .header("cookie", "session=stolen")
        .header("x-forwarded-for", "203.0.113.7")
        .header("x-forwarded-host", "evil.example")
        .header("x-forwarded-proto", "http")
        .header("x-real-ip", "203.0.113.7")
        .header("forwarded", "for=203.0.113.7")
        .header("true-client-ip", "203.0.113.7")
        .header("cf-connecting-ip", "203.0.113.7")
        .header("cf-ray", "8c1d2e3f4a5b6c7d-FRA")
        .header("cf-ipcountry", "DE")
        .header("x-admin-token", "admin-secret")
        .header("x-internal-state", "1")
        .header("proxy-authorization", "Basic c2VjcmV0")
        .header("origin", "https://evil.example")
        .header("referer", "https://evil.example/")
        .header("user-agent", "okhttp/4.12.0")
        .header("accept-language", "en")
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .header("x-custom", "1")
        .body(ALLOCATE_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);

    let received = gateway.received(Upstream::Platform).await;
    let got = &received[0];
    assert_eq!(
        header(got, "authorization"),
        Some("Bearer eyJhbGciOiJIUzI1NiJ9.e30.c2lnbmF0dXJl")
    );
    assert_eq!(header(got, "content-type"), Some("application/json"));
    assert_eq!(header(got, "accept"), Some("application/json"));
    assert_eq!(header(got, "idempotency-key"), Some("alloc-7f3a"));
    assert_eq!(header(got, "user-agent"), Some(USER_AGENT));
    assert_eq!(got.body, ALLOCATE_BODY.as_bytes());

    let names: BTreeSet<&str> = got.headers.keys().map(|name| name.as_str()).collect();
    let allowed = BTreeSet::from([
        "accept",
        "authorization",
        "content-length",
        "content-type",
        "host",
        "idempotency-key",
        "user-agent",
    ]);
    assert!(
        names.is_subset(&allowed),
        "headers that must not reach the upstream: {:?}",
        names.difference(&allowed).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_request_without_authorization_is_forwarded_without_one() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway
        .http
        .post(gateway.url("/auth/refresh"))
        .header("content-type", "application/json")
        .body(REFRESH_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(header(&received[0], "authorization"), None);
    assert_eq!(received[0].body, REFRESH_BODY.as_bytes());
}

/// An order cancel has no body; it goes upstream as an empty DELETE with
/// `Content-Length: 0`, as OkHttp sends it.
#[tokio::test]
async fn an_order_cancel_is_forwarded_as_an_empty_delete() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway
        .http
        .delete(gateway.url(&format!("/orders/{ORDER}")))
        .header("authorization", "Bearer session.jwt")
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), 1);
    let got = &received[0];
    assert_eq!(got.method, Method::DELETE);
    assert_eq!(got.url.path(), format!("/orders/{ORDER}"));
    assert_eq!(header(got, "authorization"), Some("Bearer session.jwt"));
    assert!(got.body.is_empty());
    assert_eq!(header(got, "content-length"), Some("0"));
    assert_eq!(header(got, "content-type"), None);
}

#[tokio::test]
async fn a_bodiless_action_is_forwarded_as_an_empty_post() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway
        .http
        .post(gateway.url(&format!("/bots/{BOT}/pause")))
        .header("authorization", "Bearer session.jwt")
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let received = gateway.received(Upstream::Platform).await;
    let got = &received[0];
    assert_eq!(got.method, Method::POST);
    assert!(got.body.is_empty());
    assert_eq!(header(got, "content-length"), Some("0"));
    assert_eq!(got.headers.get_all("content-length").iter().count(), 1);
    assert_eq!(header(got, "content-type"), None);
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upstream_bodies_pass_through_byte_for_byte() {
    let gateway = Gateway::start().await;
    // Unusual spacing and key order, an integer too big for f64, an escape and
    // raw UTF-8. Re-serialising would change at least one of them.
    let body: &[u8] = b"{ \"z\": 1,\n  \"a\":[1, 2,3], \"big\": 123456789012345678901234567890, \"s\": \"caf\\u00e9 \xe2\x9c\x93\" }\n";
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(&gateway.api)
        .await;
    let response = gateway.send(Method::GET, "/me").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.expect("body"), body);
}

#[tokio::test]
async fn upstream_error_statuses_and_bodies_pass_through_unchanged() {
    let gateway = Gateway::start().await;
    for status in [400, 401, 403, 404, 409, 410, 422, 429, 500, 502, 503] {
        gateway.api.reset().await;
        let body = format!(
            r#"{{"error":"upstream said {status}","code":{status},"category":"validation"}}"#
        );
        Mock::given(any())
            .respond_with(
                ResponseTemplate::new(status).set_body_raw(body.clone(), "application/json"),
            )
            .mount(&gateway.api)
            .await;
        let response = gateway.send(Method::GET, &format!("/bots/{BOT}")).await;
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.text().await.expect("body"), body, "{status}");
    }
}

/// The platform's 429 and 503 may say when to retry; the app sees it.
#[tokio::test]
async fn retry_after_comes_back_with_a_429_or_503() {
    let gateway = Gateway::start().await;
    for status in [429, 503] {
        gateway.api.reset().await;
        let body = format!(r#"{{"error":"busy","code":{status},"category":"upstream"}}"#);
        Mock::given(any())
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_raw(body.clone(), "application/json")
                    .insert_header("retry-after", "2"),
            )
            .mount(&gateway.api)
            .await;
        let response = gateway
            .send(Method::DELETE, &format!("/orders/{ORDER}"))
            .await;
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["retry-after"], "2", "{status}");
        assert_eq!(response.text().await.expect("body"), body, "{status}");
    }
}

#[tokio::test]
async fn only_allowlisted_response_headers_come_back_from_the_upstream() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"entrants":[]}"#, "application/json; charset=utf-8")
                .insert_header("cache-control", "private, max-age=10")
                .insert_header("retry-after", "5")
                .insert_header("idempotent-replayed", "true")
                .insert_header("set-cookie", "upstream=1; Secure")
                .insert_header("link", "</terminal/x/candles>; rel=\"canonical\"")
                .insert_header("x-powered-by", "upstream")
                .insert_header("access-control-allow-origin", "*")
                .insert_header("strict-transport-security", "max-age=31536000")
                .insert_header("x-request-id", "abc"),
        )
        .mount(&gateway.api)
        .await;
    let response = gateway
        .send(Method::GET, &format!("/arena/entrants/{ENTRANT}"))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers["content-type"], "application/json; charset=utf-8");
    assert_eq!(headers["cache-control"], "private, max-age=10");
    assert_eq!(headers["retry-after"], "5");
    let names: BTreeSet<&str> = headers.keys().map(|name| name.as_str()).collect();
    let allowed = BTreeSet::from([
        "cache-control",
        "content-length",
        "content-type",
        "date",
        "retry-after",
    ]);
    assert!(
        names.is_subset(&allowed),
        "upstream headers that must not reach the app: {:?}",
        names.difference(&allowed).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn indexer_refusals_pass_through_as_plain_text() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(400).set_body_raw("invalid res", "text/plain; charset=utf-8"),
        )
        .mount(&gateway.indexer)
        .await;
    let response = gateway
        .send(Method::GET, &format!("/tokens/{MINT}/candles?res=7"))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(response.text().await.expect("body"), "invalid res");
}

#[tokio::test]
async fn large_upstream_bodies_pass_through() {
    let gateway = Gateway::start().await;
    let body = format!("[{}]", vec!["1234567890"; 300_000].join(","));
    assert!(body.len() > 3 * MAX_REQUEST_BODY_BYTES);
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.clone(), "application/json"))
        .mount(&gateway.api)
        .await;
    let response = gateway
        .send(
            Method::GET,
            &format!("/arena/entrants/{ENTRANT}/transcript?runs=50"),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), body);
}

#[tokio::test]
async fn an_oversized_upstream_body_answers_502() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(vec![b' '; MAX_RESPONSE_BODY_BYTES + 1], "application/json"),
        )
        .mount(&gateway.api)
        .await;
    let response = gateway.send(Method::GET, "/bots").await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value = json(response).await;
    assert_error_shape(&body, 502, "upstream");
}

#[tokio::test]
async fn upstream_redirects_are_returned_not_followed() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/elsewhere", gateway.indexer.uri())),
        )
        .mount(&gateway.api)
        .await;
    let response = gateway
        .http
        .get(gateway.url("/me"))
        .header("authorization", "Bearer session.jwt")
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::FOUND);
    assert!(response.headers().get("location").is_none());
    assert!(
        gateway.received(Upstream::Indexer).await.is_empty(),
        "a redirect target must never receive the caller's request"
    );
}

// ---------------------------------------------------------------------------
// Arena
// ---------------------------------------------------------------------------

/// The thirteen arena routes in the contract, including the clone provenance
/// read that lives under `/bots`.
fn arena_cases() -> Vec<Case> {
    contract()
        .into_iter()
        .filter(|case| {
            case.pattern.starts_with("/arena/") || case.pattern == "/bots/{id}/arena-clone"
        })
        .collect()
}

/// Arena routes whose handler takes no token. Only the gate looks at one.
const PUBLIC_ARENA_PATTERNS: [&str; 6] = [
    "/arena/paper-bots",
    "/arena/season",
    "/arena/entrants/{id}",
    "/arena/feed",
    "/arena/seasons",
    "/arena/seasons/{number}",
];

#[test]
fn the_contract_lists_all_thirteen_arena_routes() {
    let patterns: Vec<&str> = arena_cases().iter().map(|case| case.pattern).collect();
    assert_eq!(patterns.len(), 13, "{patterns:?}");
    for pattern in PUBLIC_ARENA_PATTERNS {
        assert!(patterns.contains(&pattern), "{pattern}");
    }
}

#[tokio::test]
async fn public_arena_routes_are_forwarded_without_a_token() {
    let gateway = Gateway::with_ok_upstreams().await;
    let public = [
        "/arena/paper-bots".to_owned(),
        "/arena/season".to_owned(),
        format!("/arena/entrants/{ENTRANT}"),
        "/arena/feed".to_owned(),
        format!("/arena/feed?limit=30&before={FEED_CURSOR}"),
        "/arena/seasons".to_owned(),
        "/arena/seasons/6".to_owned(),
    ];
    for path in &public {
        let response = gateway.send(Method::GET, path).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    let received = gateway.received(Upstream::Platform).await;
    let paths: Vec<String> = received
        .iter()
        .map(|request| path_and_query(&request.url))
        .collect();
    assert_eq!(paths, public);
    for request in &received {
        assert_eq!(header(request, "authorization"), None, "{}", request.url);
    }
}

/// The gateway does not check tokens. A signed-in route without one is still
/// forwarded, and the platform's 401 comes back as it sent it.
#[tokio::test]
async fn signed_in_arena_routes_leave_the_401_to_the_platform() {
    let gateway = Gateway::start().await;
    let body = r#"{"error":"authentication required","code":401,"category":"not_authorized"}"#;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(401).set_body_raw(body, "application/json"))
        .mount(&gateway.api)
        .await;
    let signed_in: Vec<Case> = arena_cases()
        .into_iter()
        .filter(|case| !PUBLIC_ARENA_PATTERNS.contains(&case.pattern))
        .collect();
    assert_eq!(signed_in.len(), 7);
    for case in &signed_in {
        let label = format!("{} {}", case.method, case.path);
        let response = gateway
            .request(case)
            .send()
            .await
            .expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{label}");
        assert_eq!(response.text().await.expect("body"), body, "{label}");
    }
    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), signed_in.len());
    for request in &received {
        assert_eq!(header(request, "authorization"), None, "{}", request.url);
    }
}

/// With the arena flag off, the platform answers 404 with a JSON body whose
/// category is `internal`. The gateway must not swap in its own 404, which
/// says `validation`.
#[tokio::test]
async fn a_closed_arena_gate_404_passes_through_unchanged() {
    let gateway = Gateway::start().await;
    for case in arena_cases() {
        let path = case.path.split('?').next().expect("a path").to_owned();
        let body =
            format!(r#"{{"error":"arena not found: {path}","code":404,"category":"internal"}}"#);
        for token in [None, Some("Bearer session.jwt")] {
            let label = format!("{} {} (token: {})", case.method, case.path, token.is_some());
            gateway.api.reset().await;
            Mock::given(any())
                .respond_with(
                    ResponseTemplate::new(404).set_body_raw(body.clone(), "application/json"),
                )
                .mount(&gateway.api)
                .await;
            let mut request = gateway.request(&case);
            if let Some(token) = token {
                request = request.header("authorization", token);
            }
            let response = request.send().await.expect("the gateway answers");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{label}");
            assert_eq!(
                response.headers()["content-type"],
                "application/json",
                "{label}"
            );
            assert_eq!(response.text().await.expect("body"), body, "{label}");
            let received = gateway.received(Upstream::Platform).await;
            assert_eq!(received.len(), 1, "{label}: forwarded once");
            assert_eq!(header(&received[0], "authorization"), token, "{label}");
        }
    }
}

/// The platform's own unknown-route 404 has an empty body; it stays empty.
#[tokio::test]
async fn an_empty_upstream_404_stays_empty() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(404))
        .mount(&gateway.api)
        .await;
    let response = gateway.send(Method::GET, "/arena/season").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.bytes().await.expect("body").is_empty());
}

#[tokio::test]
async fn a_clone_and_its_retry_reach_the_platform_with_body_and_idempotency_key_intact() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(CLONE_RESPONSE, "application/json"))
        .mount(&gateway.api)
        .await;
    let path = format!("/arena/entrants/{ENTRANT}/clone");
    // The app retries a failed clone with the same key; the platform answers
    // the replay with the same 200 body.
    let mut answers = Vec::new();
    for _ in 0..2 {
        let response = gateway
            .http
            .post(gateway.url(&path))
            .header("authorization", "Bearer session.jwt")
            .header("content-type", "application/json")
            .header("idempotency-key", CLONE_KEY)
            .body(CLONE_BODY)
            .send()
            .await
            .expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::OK);
        answers.push(response.text().await.expect("body"));
    }
    assert_eq!(answers, [CLONE_RESPONSE, CLONE_RESPONSE]);

    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(
        received.len(),
        2,
        "the gateway neither retries nor deduplicates"
    );
    for got in &received {
        assert_eq!(got.method, Method::POST);
        assert_eq!(got.url.path(), path);
        assert_eq!(got.body, CLONE_BODY.as_bytes());
        let body: Value = serde_json::from_slice(&got.body).expect("JSON body");
        assert_eq!(body["idempotency_key"], CLONE_KEY);
        assert_eq!(header(got, "content-type"), Some("application/json"));
        assert_eq!(header(got, "idempotency-key"), Some(CLONE_KEY));
        assert_eq!(header(got, "authorization"), Some("Bearer session.jwt"));
    }
}

/// Clone refusals, JSON or the platform's plain-text body rejections, come
/// back with their status, type and body.
#[tokio::test]
async fn clone_refusals_pass_through_unchanged() {
    let gateway = Gateway::start().await;
    let cases = [
        (
            404,
            "application/json",
            format!(r#"{{"error":"arena_clone not found: {BOT}","code":404,"category":"internal"}}"#),
        ),
        (
            409,
            "application/json",
            r#"{"error":"a clone with this idempotency key is already being created","code":409,"category":"validation"}"#.to_owned(),
        ),
        (
            415,
            "text/plain; charset=utf-8",
            "Expected request with `Content-Type: application/json`".to_owned(),
        ),
        (
            422,
            "text/plain; charset=utf-8",
            "Failed to deserialize the JSON body into the target type: missing field `name`"
                .to_owned(),
        ),
    ];
    for (status, content_type, body) in cases {
        gateway.api.reset().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(status).set_body_raw(body.clone(), content_type))
            .mount(&gateway.api)
            .await;
        let response = gateway
            .http
            .post(gateway.url(&format!("/arena/entrants/{ENTRANT}/clone")))
            .header("authorization", "Bearer session.jwt")
            .header("content-type", "application/json")
            .body(CLONE_BODY)
            .send()
            .await
            .expect("the gateway answers");
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["content-type"], content_type, "{status}");
        assert_eq!(response.text().await.expect("body"), body, "{status}");
    }
}

/// The platform clamps `limit` and `runs` and rejects a bad cursor. The
/// gateway forwards the query as sent and leaves those decisions to it.
#[tokio::test]
async fn arena_query_strings_are_forwarded_without_clamping_or_checks() {
    let gateway = Gateway::with_ok_upstreams().await;
    let paths = [
        format!("/arena/entrants/{ENTRANT}/trades?limit=0"),
        format!("/arena/entrants/{ENTRANT}/trades?limit=9999"),
        format!("/arena/entrants/{ENTRANT}/config-history?limit=abc"),
        format!("/arena/entrants/{ENTRANT}/transcript?runs=-1"),
        "/arena/feed?limit=500&before=not-a-cursor".to_owned(),
        "/arena/seasons/6?unused=1".to_owned(),
    ];
    for path in &paths {
        let response = gateway
            .http
            .get(gateway.url(path))
            .header("authorization", "Bearer session.jwt")
            .send()
            .await
            .expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    let received: Vec<String> = gateway
        .received(Upstream::Platform)
        .await
        .iter()
        .map(|request| path_and_query(&request.url))
        .collect();
    assert_eq!(received, paths);
}

#[tokio::test]
async fn arena_cache_control_comes_back_as_the_platform_set_it() {
    let gateway = Gateway::start().await;
    for (path, cache_control) in [
        ("/arena/paper-bots".to_owned(), "no-store"),
        ("/arena/season".to_owned(), "public, max-age=30"),
        ("/arena/feed".to_owned(), "public, max-age=15"),
        (
            format!("/arena/feed?before={FEED_CURSOR}"),
            "public, max-age=3600",
        ),
        (
            "/arena/seasons".to_owned(),
            "public, max-age=86400, immutable",
        ),
        (
            "/arena/seasons/6".to_owned(),
            "public, max-age=86400, immutable",
        ),
        (format!("/arena/entrants/{ENTRANT}"), "public, max-age=10"),
    ] {
        gateway.api.reset().await;
        Mock::given(any())
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(r#"{"ok":true}"#, "application/json")
                    .insert_header("cache-control", cache_control),
            )
            .mount(&gateway.api)
            .await;
        let response = gateway.send(Method::GET, &path).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["cache-control"], cache_control, "{path}");
    }

    // The dashboard tabs set no Cache-Control, and the gateway adds none.
    gateway.api.reset().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
        .mount(&gateway.api)
        .await;
    let response = gateway
        .send(Method::GET, &format!("/arena/entrants/{ENTRANT}/trades"))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("cache-control").is_none());
}

// ---------------------------------------------------------------------------
// Account, portfolio and public routes
// ---------------------------------------------------------------------------

/// The app saves settings with `PUT /me/settings?return=minimal`; everything
/// the platform needs to apply the change arrives as the app sent it.
#[tokio::test]
async fn a_settings_save_reaches_the_platform_with_query_body_and_token_intact() {
    let gateway = Gateway::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(r#"{"ok":true}"#, "application/json"))
        .mount(&gateway.api)
        .await;
    let response = gateway
        .http
        .put(gateway.url("/me/settings?return=minimal"))
        .header("authorization", "Bearer session.jwt")
        .header("content-type", "application/json")
        .body(SETTINGS_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), r#"{"ok":true}"#);

    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), 1);
    let got = &received[0];
    assert_eq!(got.method, Method::PUT);
    assert_eq!(path_and_query(&got.url), "/me/settings?return=minimal");
    assert_eq!(got.body, SETTINGS_BODY.as_bytes());
    assert_eq!(header(got, "content-type"), Some("application/json"));
    assert_eq!(header(got, "authorization"), Some("Bearer session.jwt"));
    assert!(gateway.received(Upstream::Indexer).await.is_empty());
}

/// Token metadata and wallet SOL balances take no token on the platform, and
/// the gateway forwards them without one.
#[tokio::test]
async fn public_token_meta_and_sol_balance_are_forwarded_without_a_token() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway
        .http
        .post(gateway.url("/public/tokens-meta"))
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(TOKENS_META_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let balance = format!("/wallets/{WALLET}/sol-balance");
    let response = gateway.send(Method::GET, &balance).await;
    assert_eq!(response.status(), StatusCode::OK);

    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].method, Method::POST);
    assert_eq!(path_and_query(&received[0].url), "/public/tokens-meta");
    assert_eq!(received[0].body, TOKENS_META_BODY.as_bytes());
    assert_eq!(received[1].method, Method::GET);
    assert_eq!(path_and_query(&received[1].url), balance);
    for request in &received {
        assert_eq!(header(request, "authorization"), None, "{}", request.url);
    }
}

// ---------------------------------------------------------------------------
// The allowlist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlisted_routes_answer_404_without_reaching_any_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;
    let cases: Vec<(Method, String)> = vec![
        (Method::POST, "/admin/jobs/run".into()),
        (Method::GET, "/admin/jobs/run".into()),
        (Method::POST, "/ai/chat".into()),
        (Method::GET, "/ai/chat".into()),
        (Method::GET, "/admin/settings".into()),
        (Method::GET, "/metrics".into()),
        (Method::GET, "/health/live".into()),
        (Method::GET, "/ws".into()),
        (Method::GET, "/".into()),
        (Method::GET, "/bots/".into()),
        (Method::GET, "/BOTS".into()),
        // The indexer is reachable only through the renamed candles route.
        (Method::GET, format!("/terminal/{MINT}/candles")),
        (Method::GET, format!("/tokens/{MINT}/trades")),
        // Platform API routes that aren't allowlisted.
        (Method::POST, "/bots/import".into()),
        (Method::POST, "/bots/export".into()),
        (Method::POST, format!("/bots/{BOT}/withdraw")),
        (Method::GET, format!("/bots/{BOT}/config-history")),
        (Method::GET, format!("/bots/{BOT}/auto-tune/transcript")),
        (Method::POST, format!("/bots/{BOT}/ai/chat/stream")),
        // Listed paths with a method that is not listed.
        (Method::DELETE, format!("/bots/{BOT}")),
        (Method::PATCH, format!("/bots/{BOT}/config")),
        (Method::GET, format!("/bots/{BOT}/start")),
        (Method::GET, "/auth/privy".into()),
        (Method::DELETE, "/me".into()),
        (Method::PUT, "/bots".into()),
        (Method::GET, format!("/arena/entrants/{ENTRANT}/clone")),
        (Method::PUT, format!("/arena/entrants/{ENTRANT}/clone")),
        (Method::POST, "/arena/season".into()),
        (Method::POST, format!("/bots/{BOT}/arena-clone")),
        (Method::DELETE, "/orders".into()),
        (Method::PUT, format!("/orders/{ORDER}")),
        // Arena paths that only look like listed ones.
        (Method::GET, "/arena".into()),
        (Method::GET, "/arena/entrants".into()),
        (Method::GET, format!("/arena/entrants/{ENTRANT}/unknown")),
        (
            Method::GET,
            format!("/arena/entrants/{ENTRANT}/trades/extra"),
        ),
        (Method::GET, "/arena/seasons/6/entrants".into()),
        (Method::GET, format!("/bots/{BOT}/arena-clone/extra")),
        (Method::GET, "/admin/arena/seasons".into()),
        (Method::OPTIONS, "/bots".into()),
        (Method::POST, "/health".into()),
        // Money routes the app doesn't call stay closed, and so do other
        // methods on the ones it does.
        (Method::POST, "/trade/buy".into()),
        (Method::POST, "/trade/preview".into()),
        (Method::POST, "/trade/quote".into()),
        (Method::GET, "/trade/swap".into()),
        (Method::POST, "/trade/swap/extra".into()),
        (Method::PUT, "/orders".into()),
        (Method::GET, "/bots/live-confirm".into()),
        (Method::DELETE, format!("/bots/{BOT}/wallet")),
        (Method::GET, format!("/bots/{BOT}/wallet/fund")),
        (Method::POST, format!("/bots/{BOT}/wallet/sweep")),
        (Method::POST, format!("/bots/{BOT}/wallet/migrate")),
        (Method::GET, format!("/bots/{BOT}/wallet/locked-rent")),
        (
            Method::POST,
            format!("/bots/{BOT}/wallet/locked-rent/claim"),
        ),
        (Method::GET, format!("/bots/{BOT}/wallet/funding")),
        (
            Method::POST,
            format!("/bots/{BOT}/wallet/funding/{FUNDING}"),
        ),
        (Method::GET, "/solana/submit".into()),
        (Method::POST, "/solana/blockhash".into()),
        (Method::GET, "/solana/rpc".into()),
        (Method::POST, "/solana".into()),
        // Neighbours of the account and portfolio reads that aren't listed.
        (Method::GET, format!("/orders/{ORDER}")),
        (Method::POST, "/me/settings".into()),
        (Method::GET, "/portfolio/positions".into()),
        (Method::GET, "/wallets/sol-balances".into()),
        (Method::GET, format!("/wallets/{WALLET}/scan")),
        (Method::GET, "/public/tokens-meta".into()),
    ];
    for (method, path) in &cases {
        let response = gateway.send(method.clone(), path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
        let body: Value = json(response).await;
        assert_error_shape(&body, 404, "validation");
    }
    // HEAD is not listed either; its 404 carries no body.
    let response = gateway.send(Method::HEAD, "/bots").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "HEAD /bots");
    gateway.assert_no_upstream_calls().await;
}

#[tokio::test]
async fn static_bot_segments_are_never_captured_as_a_bot_id() {
    let gateway = Gateway::with_ok_upstreams().await;
    let statics = [
        format!("/bots/stats?ids={BOT},{OTHER_BOT}"),
        "/bots/leaderboard".to_owned(),
        "/bots/defaults".to_owned(),
        "/bots/wallets".to_owned(),
    ];
    for path in &statics {
        let response = gateway.send(Method::GET, path).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{path} must be forwarded, not refused as a malformed bot id"
        );
    }
    let received: Vec<String> = gateway
        .received(Upstream::Platform)
        .await
        .iter()
        .map(|request| path_and_query(&request.url))
        .collect();
    assert_eq!(received, statics);

    // A near miss is a bot id, and fails as one.
    for path in [
        "/bots/statsx",
        "/bots/stat",
        "/bots/leaderboards",
        "/bots/default",
        "/bots/wallet",
    ] {
        let response = gateway.send(Method::GET, path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
    assert_eq!(
        gateway.received(Upstream::Platform).await.len(),
        statics.len()
    );
}

#[tokio::test]
async fn malformed_path_parameters_answer_400_without_reaching_any_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;
    let cases: Vec<(Method, String)> = vec![
        (Method::GET, "/bots/not-a-uuid".into()),
        (
            Method::GET,
            "/bots/1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d1".into(),
        ),
        (
            Method::GET,
            "/bots/1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10ff".into(),
        ),
        (
            Method::GET,
            "/bots/zb4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10/stats".into(),
        ),
        (Method::PUT, "/bots/123/config".into()),
        (Method::POST, "/bots/xyz/start".into()),
        (Method::GET, "/bots/xyz/arena-clone".into()),
        (Method::GET, "/arena/entrants/not-a-uuid".into()),
        (Method::GET, "/arena/entrants/not-a-uuid/positions".into()),
        (
            Method::GET,
            "/arena/entrants/not-a-uuid/trades?limit=50".into(),
        ),
        (Method::GET, "/arena/entrants/not-a-uuid/watching".into()),
        (
            Method::GET,
            "/arena/entrants/not-a-uuid/config-history".into(),
        ),
        (Method::GET, "/arena/entrants/not-a-uuid/transcript".into()),
        (Method::POST, "/arena/entrants/not-a-uuid/clone".into()),
        (Method::GET, format!("/arena/entrants/{MINT}")),
        (Method::GET, "/arena/seasons/six".into()),
        (Method::GET, "/arena/seasons/6x".into()),
        (Method::GET, "/arena/seasons/1.5".into()),
        (Method::GET, "/arena/seasons/99999999999".into()),
        (Method::DELETE, "/orders/not-a-uuid".into()),
        (Method::DELETE, format!("/orders/{MINT}")),
        // Not base58 (0, O, I and l are outside the alphabet).
        (
            Method::GET,
            "/tokens/0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl/candles".into(),
        ),
        (Method::GET, "/tokens/short/candles".into()),
        (Method::GET, format!("/tokens/{MINT}xy/candles")),
        // 44 base58 characters that decode to 44 bytes, not a 32-byte key.
        (Method::GET, format!("/tokens/{}/candles", "1".repeat(44))),
        (Method::GET, "/wallets/not-an-address/sol-balance".into()),
        (Method::GET, format!("/wallets/{WALLET}xy/sol-balance")),
        (Method::GET, format!("/wallets/{BOT}/sol-balance")),
        (Method::GET, "/orders/not-a-uuid/events".into()),
        (Method::GET, format!("/orders/{MINT}/events")),
        (Method::POST, "/bots/not-a-uuid/archive".into()),
        (Method::GET, "/bots/not-a-uuid/wallet".into()),
        (Method::POST, "/bots/not-a-uuid/wallet".into()),
        (Method::POST, "/bots/not-a-uuid/wallet/delegated".into()),
        (Method::POST, "/bots/not-a-uuid/wallet/fund".into()),
        (Method::POST, format!("/bots/{MINT}/wallet/withdraw")),
        (
            Method::GET,
            format!("/bots/not-a-uuid/wallet/funding/{FUNDING}"),
        ),
        // A near miss of `/bots/live-confirm` is a bot id.
        (Method::POST, "/bots/live-confirm/archive".into()),
    ];
    for (method, path) in &cases {
        let response = gateway.send(method.clone(), path).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{method} {path}"
        );
        let body: Value = json(response).await;
        assert_error_shape(&body, 400, "validation");
    }

    // Sent raw, because a URL parser would normalise these.
    let raw_targets = [
        "/bots/..".to_owned(),
        "/bots/%2e%2e".to_owned(),
        "/bots/..%2F..%2Fadmin%2Fsettings".to_owned(),
        "/bots/%2e%2e/stats".to_owned(),
        format!("/bots/{BOT}%2F..%2F..%2Fadmin"),
        format!("/bots/{BOT}%20"),
        "/tokens/..%2F..%2Fadmin%2Fsettings/candles".to_owned(),
        "/arena/seasons/%36".to_owned(),
        "/arena/seasons/..".to_owned(),
        "/arena/entrants/%2e%2e/trades".to_owned(),
        "/arena/entrants/..%2F..%2Fadmin%2Farena/transcript".to_owned(),
        format!("/arena/entrants/{ENTRANT}%2F..%2F..%2Fadmin"),
        format!("/bots/{BOT}%2F..%2F..%2Farena/arena-clone"),
        "/wallets/..%2F..%2Fadmin%2Fsettings/sol-balance".to_owned(),
        "/orders/%2e%2e/events".to_owned(),
        format!("/bots/{BOT}/wallet/funding/%2e%2e"),
        format!("/bots/{BOT}/wallet/funding/..%2F..%2Fwallet"),
    ];
    for target in &raw_targets {
        let (status, body) = raw_request(&gateway.base, &raw_get(target), b"").await;
        assert_eq!(status, 400, "GET {target}");
        let body: Value = serde_json::from_str(&body).expect("JSON error body");
        assert_error_shape(&body, 400, "validation");
    }
    gateway.assert_no_upstream_calls().await;
}

#[tokio::test]
async fn a_query_string_reaches_the_upstream_byte_for_byte() {
    let gateway = Gateway::with_ok_upstreams().await;
    // A feed cursor sent without percent-encoding: `+` must not become a
    // space and `|` must not be encoded on the way through.
    let raw_cursor = "2026-10-01T12:00:00.123456+00:00|5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11|0";
    let queries = [
        format!("limit=30&before={raw_cursor}"),
        format!("limit=30&before={FEED_CURSOR}"),
        "limit=30&limit=31&before=&x=%41%2b+".to_owned(),
    ];
    for query in &queries {
        let (status, _) = raw_request(
            &gateway.base,
            &raw_get(&format!("/arena/feed?{query}")),
            b"",
        )
        .await;
        assert_eq!(status, 200, "{query}");
    }
    let received: Vec<Option<String>> = gateway
        .received(Upstream::Platform)
        .await
        .iter()
        .map(|request| request.url.query().map(str::to_owned))
        .collect();
    assert_eq!(
        received,
        queries.iter().cloned().map(Some).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn an_absolute_form_target_still_goes_to_the_configured_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;
    let (status, _) = raw_request(&gateway.base, &raw_get("http://evil.example/me"), b"").await;
    assert_eq!(status, 200);
    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].url.path(), "/me");
}

#[tokio::test]
async fn health_answers_without_calling_any_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;
    let response = gateway.send(Method::GET, "/health").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = json(response).await;
    assert_eq!(
        body,
        json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") })
    );
    gateway.assert_no_upstream_calls().await;
}

// ---------------------------------------------------------------------------
// Limits and failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_body_at_the_limit_is_forwarded() {
    let gateway = Gateway::with_ok_upstreams().await;
    let body = vec![b'a'; MAX_REQUEST_BODY_BYTES];
    let response = gateway
        .http
        .post(gateway.url("/bots"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received[0].body.len(), MAX_REQUEST_BODY_BYTES);
}

#[tokio::test]
async fn a_body_over_the_limit_answers_413_without_reaching_the_upstream() {
    let gateway = Gateway::with_ok_upstreams().await;

    // Declared too large: refused from the header alone.
    let head = format!(
        "POST /bots HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        MAX_REQUEST_BODY_BYTES + 1
    );
    let (status, body) = raw_request(&gateway.base, &head, b"").await;
    assert_eq!(status, 413);
    assert_error_shape(
        &serde_json::from_str(&body).expect("JSON error body"),
        413,
        "validation",
    );

    // Chunked, so no length is declared: refused once the limit is crossed.
    let size = MAX_REQUEST_BODY_BYTES + 1;
    let head = format!(
        "POST /bots HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{size:x}\r\n"
    );
    let (status, body) = raw_request(&gateway.base, &head, &vec![b'a'; size]).await;
    assert_eq!(status, 413);
    assert_error_shape(
        &serde_json::from_str(&body).expect("JSON error body"),
        413,
        "validation",
    );

    gateway.assert_no_upstream_calls().await;
}

#[tokio::test]
async fn an_unreachable_upstream_answers_502_in_the_error_shape() {
    let indexer = MockServer::start().await;
    let base = serve(
        &closed_port_url().await,
        &indexer.uri(),
        Duration::from_secs(2),
    )
    .await;
    let response = test_client()
        .get(format!("{base}/me"))
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value = json(response).await;
    assert_error_shape(&body, 502, "upstream");

    let api = MockServer::start().await;
    let base = serve(&api.uri(), &closed_port_url().await, Duration::from_secs(2)).await;
    let response = test_client()
        .get(format!("{base}/tokens/{MINT}/candles?res=1"))
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value = json(response).await;
    assert_error_shape(&body, 502, "upstream");
}

/// Arena reads have their own concurrency limit: while slow ones fill it, a
/// sign-in still goes straight through.
#[tokio::test]
async fn slow_arena_reads_do_not_delay_a_sign_in() {
    const ARENA_DELAY: Duration = Duration::from_secs(3);
    let gateway = Gateway::start().await;
    Mock::given(wiremock::matchers::path("/arena/feed"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"items":[]}"#, "application/json")
                .set_delay(ARENA_DELAY),
        )
        .mount(&gateway.api)
        .await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(r#"{"ok":true}"#, "application/json"))
        .mount(&gateway.api)
        .await;

    // More polls than the whole platform limit (64), so a shared limit would
    // leave the sign-in queued behind them.
    let polls: Vec<_> = (0..80)
        .map(|_| {
            let request = gateway.http.get(gateway.url("/arena/feed"));
            tokio::spawn(request.send())
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started = Instant::now();
    let response = gateway
        .http
        .post(gateway.url("/auth/privy"))
        .header("content-type", "application/json")
        .body(PRIVY_BODY)
        .send()
        .await
        .expect("the gateway answers");
    let waited = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        waited < Duration::from_secs(1),
        "the sign-in waited {waited:?} behind Arena reads"
    );

    for poll in polls {
        let response = poll.await.expect("join").expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::OK);
    }
}

/// The upstream timeout also bounds reading the request body; a client that
/// stops sending gets a 408, and nothing goes upstream.
#[tokio::test]
async fn a_stalled_request_body_answers_408_once_the_timeout_expires() {
    let gateway = Gateway::start_with_timeout(Duration::from_millis(200)).await;
    let head = format!(
        "POST /bots/{BOT}/allocate HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n"
    );
    let started = Instant::now();
    let (status, _) = raw_request(&gateway.base, &head, br#"{"lamports":"#).await;
    assert_eq!(status, 408);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "took {:?}",
        started.elapsed()
    );
    gateway.assert_no_upstream_calls().await;
}

#[tokio::test]
async fn a_slow_upstream_answers_502_once_the_timeout_expires() {
    let gateway = Gateway::start_with_timeout(Duration::from_millis(200)).await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"ok":true}"#, "application/json")
                .set_delay(Duration::from_secs(5)),
        )
        .mount(&gateway.api)
        .await;
    let started = Instant::now();
    let response = gateway.send(Method::GET, "/bots").await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the gateway must give up after its timeout, took {:?}",
        started.elapsed()
    );
    let body: Value = json(response).await;
    assert_error_shape(&body, 502, "upstream");
}

/// Money routes have their own concurrency limit: while slow polls fill the
/// platform limit, an order cancel still goes straight through.
#[tokio::test]
async fn slow_polls_do_not_delay_an_order_cancel() {
    const POLL_DELAY: Duration = Duration::from_secs(3);
    let gateway = Gateway::start().await;
    Mock::given(wiremock::matchers::path("/bots/stats"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("[]", "application/json")
                .set_delay(POLL_DELAY),
        )
        .mount(&gateway.api)
        .await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(r#"{"ok":true}"#, "application/json"))
        .mount(&gateway.api)
        .await;

    // More polls than the whole platform limit (64).
    let polls: Vec<_> = (0..80)
        .map(|_| {
            let request = gateway
                .http
                .get(gateway.url(&format!("/bots/stats?ids={BOT}")));
            tokio::spawn(request.send())
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started = Instant::now();
    let response = gateway
        .http
        .delete(gateway.url(&format!("/orders/{ORDER}")))
        .header("authorization", "Bearer session.jwt")
        .send()
        .await
        .expect("the gateway answers");
    let waited = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        waited < Duration::from_secs(1),
        "the cancel waited {waited:?} behind polls"
    );

    for poll in polls {
        let response = poll.await.expect("join").expect("the gateway answers");
        assert_eq!(response.status(), StatusCode::OK);
    }
}

/// A money route's own timeout is longer than the default. With a 200ms
/// default, a cancel still waits out a 1s upstream, while an ordinary route
/// gives up. Later money routes can reuse this pattern.
#[tokio::test]
async fn a_money_route_outlasts_the_default_timeout() {
    let gateway = Gateway::start_with_timeout(Duration::from_millis(200)).await;
    let body = format!(r#"{{"id":"{ORDER}","status":"cancelled"}}"#);
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(body.clone(), "application/json")
                .set_delay(Duration::from_secs(1)),
        )
        .mount(&gateway.api)
        .await;

    let response = gateway
        .send(Method::DELETE, &format!("/orders/{ORDER}"))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), body);

    let response = gateway.send(Method::GET, "/bots").await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_error_shape(&json(response).await, 502, "upstream");
}

/// Reading the request body keeps the default timeout on a money route, so a
/// stalled client can't hold a trade permit for the route's whole timeout.
#[tokio::test]
async fn a_stalled_body_on_a_money_route_answers_408_after_the_default_timeout() {
    let gateway = Gateway::start_with_timeout(Duration::from_millis(200)).await;
    let head = format!(
        "DELETE /orders/{ORDER} HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n"
    );
    let started = Instant::now();
    let (status, _) = raw_request(&gateway.base, &head, b"{").await;
    assert_eq!(status, 408);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "took {:?}, not the default timeout",
        started.elapsed()
    );
    gateway.assert_no_upstream_calls().await;
}

// ---------------------------------------------------------------------------
// Live agent wallets, trading and the Solana relay
// ---------------------------------------------------------------------------

/// A server-signed swap carries its idempotency key in the body; the body and
/// the caller's token reach the platform exactly as the app sent them.
#[tokio::test]
async fn a_swap_reaches_the_platform_with_body_and_token_intact() {
    let gateway = Gateway::start().await;
    let answer = r#"{"signature":"5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW","status":"submitted"}"#;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw(answer, "application/json"))
        .mount(&gateway.api)
        .await;

    let response = gateway
        .http
        .post(gateway.url("/trade/swap"))
        .header("authorization", "Bearer session.jwt")
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(SWAP_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), answer);

    let received = gateway.received(Upstream::Platform).await;
    assert_eq!(received.len(), 1, "one call, never retried");
    let got = &received[0];
    assert_eq!(got.method, Method::POST);
    assert_eq!(path_and_query(&got.url), "/trade/swap");
    assert_eq!(got.body, SWAP_BODY.as_bytes());
    assert_eq!(header(got, "authorization"), Some("Bearer session.jwt"));
    assert_eq!(header(got, "content-type"), Some("application/json"));
    assert_eq!(
        header(got, "content-length"),
        Some(&*SWAP_BODY.len().to_string())
    );
    assert!(gateway.received(Upstream::Indexer).await.is_empty());
}

/// With the default `UPSTREAM_TIMEOUT_MS` (15s), a swap the platform takes
/// 20s to answer still comes back: its own timeout is 65s.
#[tokio::test]
async fn a_swap_slower_than_the_default_timeout_still_succeeds() {
    let gateway = Gateway::start_with_timeout(Duration::from_secs(15)).await;
    let answer = r#"{"signature":"","status":"submitted"}"#;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(answer, "application/json")
                .set_delay(Duration::from_secs(20)),
        )
        .mount(&gateway.api)
        .await;

    let started = Instant::now();
    let response = gateway
        .http
        .post(gateway.url("/trade/swap"))
        .header("authorization", "Bearer session.jwt")
        .header("content-type", "application/json")
        .body(SWAP_BODY)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), answer);
    assert!(started.elapsed() >= Duration::from_secs(20));
    assert_eq!(gateway.received(Upstream::Platform).await.len(), 1);
}

/// Every money route outlasts the default timeout, and the routes beside them
/// that move no money don't. Same pattern as
/// `a_money_route_outlasts_the_default_timeout`, sent concurrently.
#[tokio::test]
async fn every_money_route_outlasts_the_default_timeout() {
    let gateway = Gateway::start_with_timeout(Duration::from_millis(200)).await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"ok":true}"#, "application/json")
                .set_delay(Duration::from_secs(1)),
        )
        .mount(&gateway.api)
        .await;
    let money = [
        (Method::POST, "/bots/{id}/wallet/fund"),
        (Method::POST, "/bots/{id}/wallet/withdraw"),
        (Method::POST, "/trade/swap"),
        (Method::POST, "/trade/swap/prepare"),
        (Method::POST, "/trade/swap/complete"),
        (Method::POST, "/trade/sell-init/quote"),
        (Method::POST, "/orders"),
        (Method::POST, "/solana/submit"),
    ];
    let default = [
        (Method::POST, "/bots/live-confirm"),
        (Method::POST, "/bots/{id}/wallet/delegated"),
        (Method::GET, "/bots/{id}/wallet/funding/{funding_id}"),
        (Method::GET, "/orders"),
        (Method::POST, "/solana/rpc"),
        (Method::GET, "/solana/blockhash"),
    ];
    let calls: Vec<_> = contract()
        .into_iter()
        .filter_map(|case| {
            let key = (case.method.clone(), case.pattern);
            let expected = if money.contains(&key) {
                StatusCode::OK
            } else if default.contains(&key) {
                StatusCode::BAD_GATEWAY
            } else {
                return None;
            };
            let label = format!("{} {}", case.method, case.pattern);
            let send = gateway.request(&case).send();
            Some(tokio::spawn(async move { (label, expected, send.await) }))
        })
        .collect();
    assert_eq!(calls.len(), money.len() + default.len());
    for call in calls {
        let (label, expected, response) = call.await.expect("join");
        let response = response.expect("the gateway answers");
        assert_eq!(response.status(), expected, "{label}");
    }
}

/// `{funding_id}` is checked as a UUID before anything is forwarded.
#[tokio::test]
async fn a_malformed_funding_id_answers_400_without_reaching_the_platform() {
    let gateway = Gateway::with_ok_upstreams().await;
    for funding in [
        "not-a-uuid",
        "42",
        MINT,
        "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2",
    ] {
        let path = format!("/bots/{BOT}/wallet/funding/{funding}");
        let response = gateway.send(Method::GET, &path).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        let body = json(response).await;
        assert_error_shape(&body, 400, "validation");
        assert_eq!(
            body["error"], "invalid path parameter `funding_id`: expected a UUID",
            "{path}"
        );
    }
    gateway.assert_no_upstream_calls().await;
}

/// The app reads the platform's 404 from `GET /bots/{id}/wallet` as "no wallet
/// yet". It comes back as the platform sent it, not as the gateway's own 404.
#[tokio::test]
async fn a_missing_bot_wallet_404_passes_through_unchanged() {
    let gateway = Gateway::start().await;
    let body =
        format!(r#"{{"error":"bot wallet not found: {BOT}","code":404,"category":"not_found"}}"#);
    Mock::given(wiremock::matchers::path(format!("/bots/{BOT}/wallet")))
        .respond_with(ResponseTemplate::new(404).set_body_raw(body.clone(), "application/json"))
        .mount(&gateway.api)
        .await;

    let response = gateway
        .send(Method::GET, &format!("/bots/{BOT}/wallet"))
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.text().await.expect("body"), body);
    assert_eq!(gateway.received(Upstream::Platform).await.len(), 1);
}
