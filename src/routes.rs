//! Route allowlist. A request that doesn't match an entry in [`ROUTES`] gets a
//! 404 and never reaches an upstream.

use std::fmt;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{Method, Uri};
use axum::routing::{delete, get, post, put, MethodRouter};
use axum::{Json, Router};
use serde_json::{json, Value};
use tower::limit::GlobalConcurrencyLimitLayer;

use crate::error::GatewayError;
use crate::proxy::Proxy;
use Verb::{Delete, Get, Post, Put};

/// Max in-flight requests to the platform API, other than the Arena routes.
/// Requests over a limit wait.
const PLATFORM_CONCURRENCY: usize = 64;

/// Max in-flight Arena requests. The app polls these, so they get their own
/// limit and slow Arena reads can't delay sign-ins or bot actions. The
/// platform gives each of its three Arena groups 20 permits.
const ARENA_CONCURRENCY: usize = 32;

/// Max in-flight requests that move money (swaps, orders, transfers). Their
/// own limit, so a swap never waits behind the app's polling. Above the
/// platform's own money cap (10, shared with the web app and queued there), so
/// the gateway only holds requests back once the platform is badly backed up.
const TRADE_CONCURRENCY: usize = 16;

/// Max in-flight Robinhood Chain market data reads. Their own limit, so a slow
/// chart or board read never delays a platform call.
const HOOD_CONCURRENCY: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Upstream {
    /// The platform API, at `PLATFORM_API_URL`.
    Platform,
    /// The Robinhood Chain read API (`st-indexerd-evm`), at `HOOD_API_URL`.
    /// Public market data; it never reads a session token.
    Hood,
}

impl fmt::Display for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Platform => "platform-api",
            Self::Hood => "hood-api",
        })
    }
}

/// The concurrency limit a route counts against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    Platform,
    Arena,
    Trade,
    /// Robinhood Chain market data, on the hood API.
    Hood,
    /// AI answers streamed as server-sent events. Not a tower layer: the
    /// permit has to last until the stream ends, so the proxy takes it and
    /// the response body holds it. See `StreamLimits`.
    Ai,
}

/// How the upstream's answer goes back to the app.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Read whole, then sent.
    Buffered,
    /// Passed on chunk by chunk when the upstream answers 2xx with
    /// `text/event-stream`; read whole otherwise.
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Post,
    Put,
    Delete,
}

impl Verb {
    pub fn method(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Post => Method::POST,
            Self::Put => Method::PUT,
            Self::Delete => Method::DELETE,
        }
    }
}

#[derive(Debug)]
pub struct Route {
    pub verb: Verb,
    /// The path the app calls, as an axum pattern.
    pub path: &'static str,
    pub upstream: Upstream,
    /// Upstream path; its `{name}` segments are filled from the request path.
    pub upstream_path: &'static str,
    /// The limit group, or `None` to take it from the path.
    pub limit: Option<Limit>,
    /// Upstream timeout, used when it is longer than `UPSTREAM_TIMEOUT_MS`.
    pub timeout: Option<Duration>,
    pub delivery: Delivery,
}

impl Route {
    pub fn limit(&self) -> Limit {
        if let Some(limit) = self.limit {
            return limit;
        }
        let arena = self.path.starts_with("/arena/") || self.path == "/bots/{id}/arena-clone";
        match self.upstream {
            Upstream::Hood => Limit::Hood,
            Upstream::Platform if arena => Limit::Arena,
            Upstream::Platform => Limit::Platform,
        }
    }

    /// Returns each `{name}` in the pattern with the matching raw (still
    /// percent-encoded) segment of `path`, or `None` if the shapes differ.
    pub fn params<'p>(&self, path: &'p str) -> Option<Vec<(&'static str, &'p str)>> {
        let mut pattern = self.path.split('/');
        let mut segments = path.split('/');
        let mut params = Vec::new();
        loop {
            match (pattern.next(), segments.next()) {
                (None, None) => return Some(params),
                (Some(expected), Some(segment)) => match param_name(expected) {
                    Some(name) => params.push((name, segment)),
                    None if expected == segment => {}
                    None => return None,
                },
                _ => return None,
            }
        }
    }

    /// Builds the upstream path from the params returned by [`Route::params`].
    pub fn upstream_path_for(&self, params: &[(&str, &str)]) -> Option<String> {
        self.upstream_path
            .split('/')
            .map(|segment| match param_name(segment) {
                Some(name) => params
                    .iter()
                    .find(|(param, _)| *param == name)
                    .map(|(_, value)| *value),
                None => Some(segment),
            })
            .collect::<Option<Vec<_>>>()
            .map(|segments| segments.join("/"))
    }
}

fn param_name(segment: &str) -> Option<&str> {
    segment.strip_prefix('{')?.strip_suffix('}')
}

/// A platform API route, forwarded to the same path.
const fn api(verb: Verb, path: &'static str) -> Route {
    Route {
        verb,
        path,
        upstream: Upstream::Platform,
        upstream_path: path,
        limit: None,
        timeout: None,
        delivery: Delivery::Buffered,
    }
}

/// A Robinhood Chain read, forwarded to the hood API on the same path. The
/// app uses the paths the web app uses against `VITE_HOOD_API_BASE_URL`.
const fn hood(path: &'static str) -> Route {
    Route {
        verb: Get,
        path,
        upstream: Upstream::Hood,
        upstream_path: path,
        limit: None,
        timeout: None,
        delivery: Delivery::Buffered,
    }
}

/// A platform API route that moves money: the trade limit, and an upstream
/// timeout longer than the app's own, so the gateway never gives up on a
/// swap, order or transfer the platform may still complete.
const fn trade(verb: Verb, path: &'static str, timeout: Duration) -> Route {
    Route {
        limit: Some(Limit::Trade),
        timeout: Some(timeout),
        ..api(verb, path)
    }
}

/// A platform API route answered with server-sent events: streamed back as it
/// arrives, under the AI limit and the stream timeouts in `StreamLimits`.
const fn stream(verb: Verb, path: &'static str) -> Route {
    Route {
        limit: Some(Limit::Ai),
        delivery: Delivery::Stream,
        ..api(verb, path)
    }
}

/// Every forwarded route. `GET /health` is answered locally and isn't listed.
///
/// `/bots/defaults`, `/bots/stats`, `/bots/leaderboard`, `/bots/wallets` and
/// `/bots/live-confirm` overlap `/bots/{id}`. axum prefers the static segment regardless of order;
/// `route_precedence_tests` checks it.
pub static ROUTES: &[Route] = &[
    // Auth and account
    api(Post, "/auth/privy"),
    api(Post, "/auth/refresh"),
    api(Get, "/me"),
    api(Get, "/paper-wallet"),
    // Bots
    api(Get, "/bots"),
    api(Post, "/bots"),
    api(Get, "/bots/defaults"),
    api(Get, "/bots/stats"),
    api(Get, "/bots/leaderboard"),
    api(Get, "/bots/{id}"),
    api(Put, "/bots/{id}/config"),
    api(Post, "/bots/{id}/start"),
    api(Post, "/bots/{id}/pause"),
    api(Post, "/bots/{id}/resume"),
    api(Post, "/bots/{id}/allocate"),
    api(Get, "/bots/{id}/stats"),
    api(Get, "/bots/{id}/positions"),
    api(Get, "/bots/{id}/trades"),
    api(Get, "/bots/{id}/watching"),
    api(Get, "/bots/{id}/arena-clone"),
    // Arena: public
    api(Get, "/arena/paper-bots"),
    api(Get, "/arena/season"),
    api(Get, "/arena/entrants/{id}"),
    api(Get, "/arena/feed"),
    api(Get, "/arena/seasons"),
    api(Get, "/arena/seasons/{number}"),
    // Arena: signed in
    api(Get, "/arena/entrants/{id}/positions"),
    api(Get, "/arena/entrants/{id}/trades"),
    api(Get, "/arena/entrants/{id}/watching"),
    api(Get, "/arena/entrants/{id}/config-history"),
    api(Get, "/arena/entrants/{id}/transcript"),
    api(Post, "/arena/entrants/{id}/clone"),
    // Account settings
    api(Get, "/me/settings"),
    api(Put, "/me/settings"),
    // Portfolio and trades
    api(Get, "/portfolio"),
    api(Get, "/portfolio/holdings"),
    api(Get, "/portfolio/realized"),
    api(Get, "/trades"),
    api(Get, "/fees/recommendation"),
    api(Get, "/wallets/{address}/sol-balance"),
    // Overlaps `/bots/{id}`; `route_precedence_tests` checks it.
    api(Get, "/bots/wallets"),
    // Orders (reads)
    api(Get, "/orders"),
    api(Get, "/orders/{id}/events"),
    // Public
    api(Post, "/public/tokens-meta"),
    // Orders (writes). The app waits 30s for a cancel.
    trade(Delete, "/orders/{id}", Duration::from_secs(35)),
    // AI, streamed (server-sent events). See "Streaming" in the README.
    stream(Post, "/bots/{id}/ai/insight/stream"),
    // Live agent wallets. The app reads a 404 from `GET /bots/{id}/wallet` as
    // "no wallet yet". Fund and withdraw are server-signed transfers without
    // an idempotency key; the app waits 15s for them.
    api(Post, "/bots/live-confirm"),
    api(Post, "/bots/{id}/archive"),
    api(Get, "/bots/{id}/wallet"),
    api(Post, "/bots/{id}/wallet"),
    api(Post, "/bots/{id}/wallet/delegated"),
    trade(Post, "/bots/{id}/wallet/fund", Duration::from_secs(20)),
    trade(Post, "/bots/{id}/wallet/withdraw", Duration::from_secs(20)),
    api(Get, "/bots/{id}/wallet/funding/{funding_id}"),
    // Trading. The app waits 60s for a swap or an order and 30s for the rest.
    trade(Post, "/trade/swap", Duration::from_secs(65)),
    trade(Post, "/trade/swap/prepare", Duration::from_secs(35)),
    trade(Post, "/trade/swap/complete", Duration::from_secs(35)),
    trade(Post, "/trade/sell-init/quote", Duration::from_secs(35)),
    trade(Post, "/orders", Duration::from_secs(65)),
    // Solana relay. The platform allowlists the RPC methods.
    trade(Post, "/solana/submit", Duration::from_secs(35)),
    api(Post, "/solana/rpc"),
    api(Get, "/solana/blockhash"),
    // Robinhood Chain trading (Hood v2, `st-api/src/hood/`, on the
    // solana-terminal `lane/trade-routes` branch). The platform answers 404
    // until it ships them. Timeouts as for the Solana swap.
    api(Get, "/hood/trade/availability"),
    api(Post, "/hood/wallets/register"),
    trade(Post, "/hood/trade/quote", Duration::from_secs(35)),
    trade(Post, "/hood/trade/swap", Duration::from_secs(65)),
    api(Get, "/hood/trade/orders/{id}"),
    // Robinhood Chain market data, from the hood API (`st-indexerd-evm`).
    // `{token}` and `{wallet}` are 0x EVM addresses. Live updates come from
    // its WebSocket, which the app opens directly.
    hood("/robinhood/board"),
    hood("/robinhood/pulse"),
    hood("/robinhood/search"),
    hood("/robinhood/tokens/{token}"),
    hood("/robinhood/tokens/{token}/trades"),
    hood("/robinhood/terminal/{token}/candles"),
    hood("/robinhood/terminal/{token}/marks"),
    hood("/robinhood/terminal/{token}/holders"),
    hood("/robinhood/terminal/{token}/dev-tokens"),
    hood("/robinhood/terminal/{token}/status"),
    hood("/robinhood/terminal/{token}/top-traders"),
    hood("/robinhood/terminal/{token}/trades"),
    hood("/robinhood/terminal/{token}/overview"),
    hood("/robinhood/terminal/{token}/open"),
    hood("/robinhood/terminal/{token}/funding"),
    hood("/robinhood/wallets/{wallet}/balances"),
];

/// Builds the router: the allowed routes, `GET /health`, and a JSON 404 for
/// everything else, including a known path with the wrong method.
pub fn router(proxy: Proxy) -> Router {
    Router::new()
        .merge(limited(group(Limit::Platform), PLATFORM_CONCURRENCY))
        .merge(limited(group(Limit::Arena), ARENA_CONCURRENCY))
        .merge(limited(group(Limit::Trade), TRADE_CONCURRENCY))
        .merge(limited(group(Limit::Hood), HOOD_CONCURRENCY))
        // The proxy limits these itself, for as long as each stream is open.
        .merge(group(Limit::Ai))
        // Outside the limits, so health checks never wait behind upstream calls.
        .route("/health", get(health))
        .fallback(not_found)
        // Only applies to routes added before this call.
        .method_not_allowed_fallback(not_found)
        .with_state(proxy)
}

fn group(limit: Limit) -> Router<Proxy> {
    ROUTES
        .iter()
        .filter(|route| route.limit() == limit)
        .fold(Router::new(), |router, route| {
            router.route(route.path, forwarding(route))
        })
}

/// One concurrency limit for the whole group. `ConcurrencyLimitLayer` would
/// create a semaphore per route, because axum layers each route separately.
fn limited<S>(group: Router<S>, max: usize) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    group.layer(GlobalConcurrencyLimitLayer::new(max))
}

fn forwarding(route: &'static Route) -> MethodRouter<Proxy> {
    let handler = move |State(proxy): State<Proxy>, request: Request| async move {
        proxy.forward(route, request).await
    };
    match route.verb {
        Get => get(handler),
        Post => post(handler),
        Put => put(handler),
        Delete => delete(handler),
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

async fn not_found(method: Method, uri: Uri) -> GatewayError {
    tracing::debug!(%method, "no route");
    GatewayError::RouteNotFound {
        method,
        path: uri.path().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::validate;

    /// While one route holds the group's only permit, other routes in the
    /// group have to wait for it.
    #[tokio::test]
    async fn should_share_one_limit_across_every_route_in_a_group() {
        use std::sync::Arc;
        use std::time::Duration;

        use axum::body::Body;
        use axum::http::Request;
        use tokio::sync::Notify;
        use tower::ServiceExt;

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let slow = {
            let (entered, release) = (entered.clone(), release.clone());
            move || async move {
                entered.notify_one();
                release.notified().await;
                "slow"
            }
        };
        let group = Router::new()
            .route("/slow", get(slow))
            .route("/fast", get(|| async { "fast" }));
        let router = limited(group, 1);
        let request = |uri: &str| Request::get(uri).body(Body::empty()).expect("request");

        let holder = tokio::spawn(router.clone().oneshot(request("/slow")));
        entered.notified().await;
        let fast = router.clone().oneshot(request("/fast"));
        tokio::pin!(fast);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut fast)
                .await
                .is_err(),
            "/fast must wait for the permit /slow holds"
        );

        release.notify_one();
        let fast = fast.await.expect("infallible");
        assert_eq!(fast.status(), axum::http::StatusCode::OK);
        let slow = holder.await.expect("join").expect("infallible");
        assert_eq!(slow.status(), axum::http::StatusCode::OK);
    }

    #[test]
    fn should_give_the_arena_routes_their_own_limit() {
        let arena: Vec<&str> = ROUTES
            .iter()
            .filter(|route| route.limit() == Limit::Arena)
            .map(|route| route.path)
            .collect();
        assert_eq!(arena.len(), 13, "all thirteen Arena routes: {arena:?}");
        assert!(arena.contains(&"/arena/paper-bots"));
        assert!(arena.contains(&"/bots/{id}/arena-clone"));
        for route in ROUTES {
            if route.path.starts_with("/auth/") || route.path == "/bots/{id}/pause" {
                assert_eq!(route.limit(), Limit::Platform, "{}", route.path);
            }
        }
    }

    /// The routes that move money, with their upstream timeouts. Each is a
    /// few seconds longer than the app waits for it.
    const TRADE_ROUTES: &[(Verb, &str, u64)] = &[
        (Delete, "/orders/{id}", 35),
        (Post, "/bots/{id}/wallet/fund", 20),
        (Post, "/bots/{id}/wallet/withdraw", 20),
        (Post, "/trade/swap", 65),
        (Post, "/trade/swap/prepare", 35),
        (Post, "/trade/swap/complete", 35),
        (Post, "/trade/sell-init/quote", 35),
        (Post, "/orders", 65),
        (Post, "/solana/submit", 35),
        (Post, "/hood/trade/quote", 35),
        (Post, "/hood/trade/swap", 65),
    ];

    #[test]
    fn should_give_only_the_money_routes_the_trade_limit_and_a_timeout() {
        for (verb, path, secs) in TRADE_ROUTES {
            let route = ROUTES
                .iter()
                .find(|route| route.verb == *verb && route.path == *path)
                .unwrap_or_else(|| panic!("{path} is listed"));
            assert_eq!(route.limit(), Limit::Trade, "{path}");
            assert_eq!(route.timeout, Some(Duration::from_secs(*secs)), "{path}");
            assert_eq!(route.upstream, Upstream::Platform, "{path}");
        }
        for route in ROUTES {
            let money = TRADE_ROUTES
                .iter()
                .any(|(verb, path, _)| route.verb == *verb && route.path == *path);
            if !money {
                assert_ne!(route.limit(), Limit::Trade, "{}", route.path);
                assert_eq!(route.timeout, None, "{}", route.path);
            }
        }
    }

    /// The live-agent and Solana routes that move no money stay in the
    /// platform group, on the default timeout.
    #[test]
    fn should_keep_the_other_live_agent_and_solana_routes_in_the_platform_group() {
        for (verb, path) in [
            (Post, "/bots/live-confirm"),
            (Post, "/bots/{id}/archive"),
            (Get, "/bots/{id}/wallet"),
            (Post, "/bots/{id}/wallet"),
            (Post, "/bots/{id}/wallet/delegated"),
            (Get, "/bots/{id}/wallet/funding/{funding_id}"),
            (Post, "/solana/rpc"),
            (Get, "/solana/blockhash"),
        ] {
            let route = ROUTES
                .iter()
                .find(|route| route.verb == verb && route.path == path)
                .unwrap_or_else(|| panic!("{path} is listed"));
            assert_eq!(route.limit(), Limit::Platform, "{path}");
            assert_eq!(route.timeout, None, "{path}");
        }
    }

    #[test]
    fn should_stream_only_the_insight_route_under_the_ai_limit() {
        for route in ROUTES {
            let insight = route.verb == Post && route.path == "/bots/{id}/ai/insight/stream";
            let (delivery, ai) = if insight {
                (Delivery::Stream, true)
            } else {
                (Delivery::Buffered, false)
            };
            assert_eq!(route.delivery, delivery, "{}", route.path);
            assert_eq!(route.limit() == Limit::Ai, ai, "{}", route.path);
        }
        assert!(
            ROUTES
                .iter()
                .any(|route| route.delivery == Delivery::Stream),
            "the insight route is listed"
        );
    }

    /// `GET /orders` and `POST /orders` sit in different groups. axum merges
    /// them into one path, and each method keeps its own group's limit.
    #[tokio::test]
    async fn should_keep_each_methods_limit_when_two_groups_share_a_path() {
        use std::sync::Arc;
        use std::time::Duration;

        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tokio::sync::Notify;
        use tower::ServiceExt;

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let slow = {
            let (entered, release) = (entered.clone(), release.clone());
            move || async move {
                entered.notify_one();
                release.notified().await;
                "read"
            }
        };
        let reads = limited(Router::new().route("/orders", get(slow)), 1);
        let writes = limited(
            Router::new().route("/orders", post(|| async { "write" })),
            1,
        );
        let router = Router::new().merge(reads).merge(writes);
        let request = |method: Method| {
            Request::builder()
                .method(method)
                .uri("/orders")
                .body(Body::empty())
                .expect("request")
        };

        let holder = tokio::spawn(router.clone().oneshot(request(Method::GET)));
        entered.notified().await;
        let write = tokio::time::timeout(
            Duration::from_secs(1),
            router.clone().oneshot(request(Method::POST)),
        )
        .await
        .expect("the write must not wait for the read's permit")
        .expect("infallible");
        assert_eq!(write.status(), StatusCode::OK);

        release.notify_one();
        let read = holder.await.expect("join").expect("infallible");
        assert_eq!(read.status(), StatusCode::OK);
    }

    #[test]
    fn should_list_each_method_and_path_once() {
        let mut seen = BTreeSet::new();
        for route in ROUTES {
            assert!(
                seen.insert((route.verb.method().to_string(), route.path)),
                "listed twice: {} {}",
                route.verb.method(),
                route.path
            );
        }
    }

    #[test]
    fn should_check_every_path_parameter() {
        for route in ROUTES {
            for segment in route.path.split('/').chain(route.upstream_path.split('/')) {
                if let Some(name) = param_name(segment) {
                    assert!(
                        validate::is_known(name),
                        "{}: `{{{name}}}` has no check in validate::path_param",
                        route.path
                    );
                }
            }
        }
    }

    #[test]
    fn should_forward_every_route_on_its_own_path() {
        for route in ROUTES {
            assert_eq!(route.upstream_path, route.path, "{}", route.path);
        }
    }

    /// The `/robinhood/...` reads, and only they, go to the hood API, as GETs
    /// under the hood limit on the default timeout.
    #[test]
    fn should_send_only_the_robinhood_reads_to_the_hood_api() {
        let mut hood = 0;
        for route in ROUTES {
            let read = route.path.starts_with("/robinhood/");
            assert_eq!(route.upstream == Upstream::Hood, read, "{}", route.path);
            assert_eq!(route.limit() == Limit::Hood, read, "{}", route.path);
            if read {
                hood += 1;
                assert_eq!(route.verb, Get, "{}", route.path);
                assert_eq!(route.timeout, None, "{}", route.path);
                assert_eq!(route.delivery, Delivery::Buffered, "{}", route.path);
            }
        }
        assert_eq!(hood, 16, "every hood API read except /healthz");
    }

    /// The Hood v2 trading routes go to the platform API. The quote and the
    /// swap move money; the availability check, the wallet registration and
    /// the order read don't.
    #[test]
    fn should_send_the_hood_trading_routes_to_the_platform_api() {
        for (verb, path, limit) in [
            (Get, "/hood/trade/availability", Limit::Platform),
            (Post, "/hood/wallets/register", Limit::Platform),
            (Post, "/hood/trade/quote", Limit::Trade),
            (Post, "/hood/trade/swap", Limit::Trade),
            (Get, "/hood/trade/orders/{id}", Limit::Platform),
        ] {
            let route = ROUTES
                .iter()
                .find(|route| route.verb == verb && route.path == path)
                .unwrap_or_else(|| panic!("{path} is listed"));
            assert_eq!(route.upstream, Upstream::Platform, "{path}");
            assert_eq!(route.limit(), limit, "{path}");
        }
    }

    #[test]
    fn should_pair_params_with_raw_segments() {
        let route = ROUTES
            .iter()
            .find(|route| route.path == "/arena/entrants/{id}/trades")
            .expect("listed");
        assert_eq!(
            route.params("/arena/entrants/abc%2F..%2F/trades"),
            Some(vec![("id", "abc%2F..%2F")])
        );
        assert_eq!(route.params("/arena/entrants/abc/positions"), None);
        assert_eq!(route.params("/arena/entrants/abc"), None);
        assert_eq!(route.params("/arena/entrants/abc/trades/x"), None);
        assert_eq!(
            ROUTES[0].params("/auth/privy"),
            Some(vec![]),
            "a static route has no params"
        );
    }

    #[test]
    fn should_fill_the_upstream_path_from_the_params() {
        let funding = ROUTES
            .iter()
            .find(|route| route.path == "/bots/{id}/wallet/funding/{funding_id}")
            .expect("listed");
        let path = "/bots/abc/wallet/funding/f%2F1";
        let params = funding.params(path).expect("matches");
        assert_eq!(funding.upstream_path_for(&params), Some(path.to_owned()));
        assert_eq!(funding.upstream_path_for(&[("id", "abc")]), None);
    }

    /// The static `/bots/...` routes must win over `/bots/{id}` in either
    /// registration order. Each handler echoes the pattern that matched.
    mod route_precedence_tests {
        use axum::body::Body;
        use axum::extract::MatchedPath;
        use axum::http::{Request, StatusCode};
        use axum::routing::on;
        use axum::routing::MethodFilter;
        use tower::ServiceExt;

        use super::super::*;

        async fn echo_matched_path(matched: MatchedPath) -> String {
            matched.as_str().to_owned()
        }

        fn filter(verb: Verb) -> MethodFilter {
            match verb {
                Get => MethodFilter::GET,
                Post => MethodFilter::POST,
                Put => MethodFilter::PUT,
                Delete => MethodFilter::DELETE,
            }
        }

        fn echo_router<'a>(routes: impl Iterator<Item = &'a Route>) -> Router {
            routes.fold(Router::new(), |router, route| {
                router.route(route.path, on(filter(route.verb), echo_matched_path))
            })
        }

        async fn matched(router: Router, uri: &str) -> (StatusCode, String) {
            method_matched(router, Method::GET, uri).await
        }

        async fn method_matched(router: Router, method: Method, uri: &str) -> (StatusCode, String) {
            let response = router
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .expect("build request"),
                )
                .await
                .expect("the router is infallible");
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .expect("read body");
            (
                status,
                String::from_utf8(bytes.to_vec()).expect("UTF-8 body"),
            )
        }

        #[tokio::test]
        async fn static_bot_segments_are_never_captured_as_a_bot_id() {
            let id = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                for (uri, pattern) in [
                    ("/bots/defaults", "/bots/defaults"),
                    ("/bots/stats", "/bots/stats"),
                    (
                        "/bots/stats?ids=1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10&limit=50",
                        "/bots/stats",
                    ),
                    ("/bots/leaderboard", "/bots/leaderboard"),
                    ("/bots/wallets", "/bots/wallets"),
                ] {
                    assert_eq!(
                        matched(build(), uri).await,
                        (StatusCode::OK, pattern.to_owned()),
                        "{label}: {uri} must reach its own route, not `/bots/{{id}}`"
                    );
                }
                // Anything else in that position is an id.
                for uri in [
                    format!("/bots/{id}"),
                    "/bots/statsx".to_owned(),
                    "/bots/stat".to_owned(),
                    "/bots/leaderboards".to_owned(),
                    "/bots/wallet".to_owned(),
                ] {
                    assert_eq!(
                        matched(build(), &uri).await,
                        (StatusCode::OK, "/bots/{id}".to_owned()),
                        "{label}: {uri}"
                    );
                }
                assert_eq!(
                    matched(build(), &format!("/bots/{id}/stats")).await,
                    (StatusCode::OK, "/bots/{id}/stats".to_owned()),
                    "{label}: the per-bot stats route is distinct from the batch"
                );
                assert_eq!(
                    method_matched(build(), Method::POST, "/bots/live-confirm").await,
                    (StatusCode::OK, "/bots/live-confirm".to_owned()),
                    "{label}: /bots/live-confirm must reach its own route, not `/bots/{{id}}`"
                );
                // `GET /bots/live-confirm` is not a bot read: the static
                // segment wins, and it only takes POST.
                assert_ne!(
                    matched(build(), "/bots/live-confirm").await.0,
                    StatusCode::OK,
                    "{label}"
                );
            }
        }

        /// The bot wallet routes sit under `/bots/{id}` next to `/bots/wallets`
        /// and the other per-bot routes. Each reaches its own pattern in either
        /// order, `funding` is not taken for `fund`, and nothing longer or
        /// shorter reaches the funding read.
        #[tokio::test]
        async fn bot_wallet_routes_reach_their_own_route() {
            let id = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
            let funding = "0f8d2c4e-6a1b-4c3d-9e5f-7a8b9c0d1e2f";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                let cases = [
                    (Method::GET, "/bots/wallets".to_owned(), "/bots/wallets"),
                    (
                        Method::GET,
                        format!("/bots/{id}/wallet"),
                        "/bots/{id}/wallet",
                    ),
                    (
                        Method::POST,
                        format!("/bots/{id}/wallet"),
                        "/bots/{id}/wallet",
                    ),
                    (
                        Method::POST,
                        format!("/bots/{id}/wallet/delegated"),
                        "/bots/{id}/wallet/delegated",
                    ),
                    (
                        Method::POST,
                        format!("/bots/{id}/wallet/fund"),
                        "/bots/{id}/wallet/fund",
                    ),
                    (
                        Method::POST,
                        format!("/bots/{id}/wallet/withdraw"),
                        "/bots/{id}/wallet/withdraw",
                    ),
                    (
                        Method::GET,
                        format!("/bots/{id}/wallet/funding/{funding}"),
                        "/bots/{id}/wallet/funding/{funding_id}",
                    ),
                    (
                        Method::POST,
                        format!("/bots/{id}/archive"),
                        "/bots/{id}/archive",
                    ),
                    (Method::GET, "/bots/wallet".to_owned(), "/bots/{id}"),
                ];
                for (method, uri, pattern) in &cases {
                    assert_eq!(
                        method_matched(build(), method.clone(), uri).await,
                        (StatusCode::OK, (*pattern).to_owned()),
                        "{label}: {method} {uri}"
                    );
                }
                for (method, uri) in [
                    (Method::GET, format!("/bots/{id}/wallet/funding")),
                    (Method::POST, format!("/bots/{id}/wallet/funding/{funding}")),
                    (
                        Method::GET,
                        format!("/bots/{id}/wallet/funding/{funding}/x"),
                    ),
                    (Method::GET, format!("/bots/{id}/wallet/fund")),
                    (Method::POST, format!("/bots/{id}/wallet/sweep")),
                    (Method::GET, format!("/bots/{id}/wallet/locked-rent")),
                    (Method::POST, format!("/bots/{id}/withdraw")),
                    (Method::GET, format!("/bots/{id}/archive")),
                ] {
                    assert_ne!(
                        method_matched(build(), method.clone(), &uri).await.0,
                        StatusCode::OK,
                        "{label}: {method} {uri}"
                    );
                }
            }
        }

        /// The trading and Solana relay paths each reach their own pattern,
        /// and `GET /orders` and `POST /orders` share one path.
        #[tokio::test]
        async fn trading_and_solana_paths_reach_their_own_route() {
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                for (method, uri) in [
                    (Method::POST, "/trade/swap"),
                    (Method::POST, "/trade/swap/prepare"),
                    (Method::POST, "/trade/swap/complete"),
                    (Method::POST, "/trade/sell-init/quote"),
                    (Method::GET, "/orders"),
                    (Method::POST, "/orders"),
                    (Method::POST, "/solana/submit"),
                    (Method::POST, "/solana/rpc"),
                    (Method::GET, "/solana/blockhash"),
                ] {
                    assert_eq!(
                        method_matched(build(), method.clone(), uri).await,
                        (StatusCode::OK, uri.to_owned()),
                        "{label}: {method} {uri}"
                    );
                }
                for (method, uri) in [
                    (Method::GET, "/trade/swap"),
                    (Method::POST, "/trade/quote"),
                    (Method::POST, "/trade/buy"),
                    (Method::POST, "/trade/swap/extra"),
                    (Method::POST, "/trade/sell-init"),
                    (Method::GET, "/solana/submit"),
                    (Method::POST, "/solana/blockhash"),
                    (Method::POST, "/solana"),
                ] {
                    assert_ne!(
                        method_matched(build(), method.clone(), uri).await.0,
                        StatusCode::OK,
                        "{label}: {method} {uri}"
                    );
                }
            }
        }

        /// The Robinhood Chain paths each reach their own pattern in either
        /// order: `/robinhood/tokens/{token}` and its trades are distinct, the
        /// terminal family never swallows a longer path, and the trading
        /// routes don't overlap the Solana ones.
        #[tokio::test]
        async fn robinhood_paths_reach_their_own_route() {
            let token = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984";
            let order = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                let mut cases = vec![
                    (
                        Method::POST,
                        "/hood/wallets/register".to_owned(),
                        "/hood/wallets/register",
                    ),
                    (
                        Method::POST,
                        "/hood/trade/quote".to_owned(),
                        "/hood/trade/quote",
                    ),
                    (
                        Method::POST,
                        "/hood/trade/swap".to_owned(),
                        "/hood/trade/swap",
                    ),
                    (
                        Method::GET,
                        format!("/hood/trade/orders/{order}"),
                        "/hood/trade/orders/{id}",
                    ),
                    (
                        Method::GET,
                        "/robinhood/board".to_owned(),
                        "/robinhood/board",
                    ),
                    (
                        Method::GET,
                        "/robinhood/pulse".to_owned(),
                        "/robinhood/pulse",
                    ),
                    (
                        Method::GET,
                        "/robinhood/search".to_owned(),
                        "/robinhood/search",
                    ),
                    (
                        Method::GET,
                        format!("/robinhood/tokens/{token}"),
                        "/robinhood/tokens/{token}",
                    ),
                    (
                        Method::GET,
                        format!("/robinhood/tokens/{token}/trades"),
                        "/robinhood/tokens/{token}/trades",
                    ),
                    (
                        Method::GET,
                        format!("/robinhood/wallets/{token}/balances"),
                        "/robinhood/wallets/{wallet}/balances",
                    ),
                ];
                for leaf in [
                    "candles",
                    "marks",
                    "holders",
                    "dev-tokens",
                    "status",
                    "top-traders",
                    "trades",
                    "overview",
                    "open",
                    "funding",
                ] {
                    let uri = format!("/robinhood/terminal/{token}/{leaf}");
                    let pattern: &'static str = ROUTES
                        .iter()
                        .find(|route| {
                            route.path.ends_with(&format!("/{{token}}/{leaf}"))
                                && route.path.starts_with("/robinhood/terminal/")
                        })
                        .expect("listed")
                        .path;
                    cases.push((Method::GET, uri, pattern));
                }
                for (method, uri, pattern) in &cases {
                    assert_eq!(
                        method_matched(build(), method.clone(), uri).await,
                        (StatusCode::OK, (*pattern).to_owned()),
                        "{label}: {method} {uri}"
                    );
                }
                for (method, uri) in [
                    (Method::GET, "/robinhood".to_owned()),
                    (Method::GET, "/robinhood/healthz".to_owned()),
                    (Method::GET, "/base/pulse".to_owned()),
                    (Method::POST, "/robinhood/pulse".to_owned()),
                    (Method::GET, format!("/robinhood/terminal/{token}")),
                    (
                        Method::GET,
                        format!("/robinhood/terminal/{token}/candles/extra"),
                    ),
                    (Method::GET, format!("/robinhood/tokens/{token}/holders")),
                    (Method::GET, format!("/robinhood/wallets/{token}")),
                    (Method::GET, "/hood/trade/quote".to_owned()),
                    (Method::POST, "/hood/trade/orders".to_owned()),
                    (Method::DELETE, format!("/hood/trade/orders/{order}")),
                    (Method::GET, "/hood/wallets/register".to_owned()),
                ] {
                    assert_ne!(
                        method_matched(build(), method.clone(), &uri).await.0,
                        StatusCode::OK,
                        "{label}: {method} {uri}"
                    );
                }
            }
        }

        /// Arena paths that share a prefix each reach their own pattern, in
        /// either registration order, and a deeper path is not cut short.
        #[tokio::test]
        async fn arena_paths_with_a_shared_prefix_reach_their_own_route() {
            let id = "5b0e8f0e-2c1d-4a7e-9a55-0d3f3d2b8c11";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                let cases = [
                    ("/arena/season".to_owned(), "/arena/season"),
                    ("/arena/seasons".to_owned(), "/arena/seasons"),
                    ("/arena/seasons/6".to_owned(), "/arena/seasons/{number}"),
                    (format!("/arena/entrants/{id}"), "/arena/entrants/{id}"),
                    (
                        format!("/arena/entrants/{id}/positions"),
                        "/arena/entrants/{id}/positions",
                    ),
                    (
                        format!("/arena/entrants/{id}/trades"),
                        "/arena/entrants/{id}/trades",
                    ),
                    (
                        format!("/arena/entrants/{id}/watching"),
                        "/arena/entrants/{id}/watching",
                    ),
                    (
                        format!("/arena/entrants/{id}/config-history"),
                        "/arena/entrants/{id}/config-history",
                    ),
                    (
                        format!("/arena/entrants/{id}/transcript"),
                        "/arena/entrants/{id}/transcript",
                    ),
                    (format!("/bots/{id}"), "/bots/{id}"),
                    (format!("/bots/{id}/arena-clone"), "/bots/{id}/arena-clone"),
                ];
                for (uri, pattern) in &cases {
                    assert_eq!(
                        matched(build(), uri).await,
                        (StatusCode::OK, (*pattern).to_owned()),
                        "{label}: {uri}"
                    );
                }
                // No pattern swallows a longer path.
                for uri in [
                    format!("/arena/entrants/{id}/unknown"),
                    format!("/arena/entrants/{id}/trades/extra"),
                    "/arena/seasons/6/entrants".to_owned(),
                    format!("/bots/{id}/arena-clone/extra"),
                ] {
                    assert_eq!(
                        matched(build(), &uri).await.0,
                        StatusCode::NOT_FOUND,
                        "{label}: {uri}"
                    );
                }
            }
        }

        /// `DELETE /orders/{id}` and `GET /orders/{id}/events` each reach their
        /// own pattern in either order, and neither swallows the other.
        #[tokio::test]
        async fn an_order_cancel_and_its_event_stream_reach_their_own_route() {
            let id = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                assert_eq!(
                    method_matched(build(), Method::DELETE, &format!("/orders/{id}")).await,
                    (StatusCode::OK, "/orders/{id}".to_owned()),
                    "{label}"
                );
                assert_eq!(
                    method_matched(build(), Method::GET, &format!("/orders/{id}/events")).await,
                    (StatusCode::OK, "/orders/{id}/events".to_owned()),
                    "{label}"
                );
                for (method, uri) in [
                    (Method::GET, format!("/orders/{id}")),
                    (Method::DELETE, format!("/orders/{id}/events")),
                    (Method::DELETE, "/orders".to_owned()),
                    (Method::DELETE, format!("/orders/{id}/extra")),
                    (Method::GET, format!("/orders/{id}/events/extra")),
                ] {
                    assert_ne!(
                        method_matched(build(), method.clone(), &uri).await.0,
                        StatusCode::OK,
                        "{label}: {method} {uri}"
                    );
                }
            }
        }

        /// The insight stream sits under `/bots/{id}` with the other per-bot
        /// routes. It reaches its own pattern in either order, and nothing
        /// shorter or longer reaches it. A static word in the id position
        /// (`/bots/stats/...`) matches as an id and is refused by the UUID check.
        #[tokio::test]
        async fn the_insight_stream_reaches_its_own_route() {
            let id = "1b4b1a48-0e19-4a5c-9f5e-7c7a0f2b3d10";
            for (label, router) in [
                ("table order", echo_router(ROUTES.iter())),
                ("reverse order", echo_router(ROUTES.iter().rev())),
            ] {
                let build = || router.clone();
                for uri in [
                    format!("/bots/{id}/ai/insight/stream"),
                    "/bots/stats/ai/insight/stream".to_owned(),
                ] {
                    assert_eq!(
                        method_matched(build(), Method::POST, &uri).await,
                        (StatusCode::OK, "/bots/{id}/ai/insight/stream".to_owned()),
                        "{label}: {uri}"
                    );
                }
                assert_eq!(
                    matched(build(), &format!("/bots/{id}")).await,
                    (StatusCode::OK, "/bots/{id}".to_owned()),
                    "{label}"
                );
                for (method, uri) in [
                    (Method::GET, format!("/bots/{id}/ai/insight/stream")),
                    (Method::POST, format!("/bots/{id}/ai/insight")),
                    (Method::POST, format!("/bots/{id}/ai/chat/stream")),
                    (Method::POST, format!("/bots/{id}/ai/insight/stream/extra")),
                    (Method::POST, "/bots/ai/insight/stream".to_owned()),
                ] {
                    assert_ne!(
                        method_matched(build(), method.clone(), &uri).await.0,
                        StatusCode::OK,
                        "{label}: {method} {uri}"
                    );
                }
            }
        }
    }
}
