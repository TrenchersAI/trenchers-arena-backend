//! Integration tests: the gateway on a local port in front of two wiremock
//! servers (platform API and indexer), checking what each one receives.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use reqwest::{Method, StatusCode};
use tokio::net::TcpListener;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use trenchers_arena_backend::routes::{Upstream, ROUTES};
use trenchers_arena_backend::{app, Config};

const BOT: &str = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
const OTHER_BOT: &str = "9f3c2a71-5d4e-4b8a-8c1f-2e6d7a9b0c34";
const ENTRANT: &str = "5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11";
const MINT: &str = "So11111111111111111111111111111111111111112";
const USER_AGENT: &str = concat!("trenchers-arena-backend/", env!("CARGO_PKG_VERSION"));

const PRIVY_BODY: &str = r#"{"privy_token":"privy.token.value","wallet_address":"9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"}"#;
const REFRESH_BODY: &str = r#"{"token":"session.token.value"}"#;
const CREATE_BOT_BODY: &str = r#"{"name":"Safe Flipper","config":{"sol_buy_amount_lamports":100000000,"exit_tiers":[{"mult_x":"2","pct_of_position":50,"slippage_override_bps":null}]},"paper_mode":true,"spend_limit_lamports":0,"required_acknowledged":["sol_buy_amount_lamports","exit_tiers"]}"#;
const CONFIG_BODY: &str = r#"{"config":{"max_hold_secs":600},"required_acknowledged":["max_hold_secs"],"change_note":"Updated in Arena"}"#;
const ALLOCATE_BODY: &str = r#"{"lamports":100000000}"#;
const CLONE_BODY: &str =
    r#"{"name":"My Opus copy","idempotency_key":"5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11"}"#;

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
        assert_eq!(
            header(got, "authorization"),
            Some("Bearer session.jwt"),
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
