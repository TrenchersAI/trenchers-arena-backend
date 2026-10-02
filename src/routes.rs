//! Route allowlist. A request that doesn't match an entry in [`ROUTES`] gets a
//! 404 and never reaches an upstream.

use std::fmt;

use axum::extract::{Request, State};
use axum::http::{Method, Uri};
use axum::routing::{get, post, put, MethodRouter};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::error::GatewayError;
use crate::proxy::Proxy;
use Verb::{Get, Post, Put};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Upstream {
    /// The platform API, at `PLATFORM_API_URL`.
    Platform,
    /// The indexer API, at `INDEXER_API_URL`.
    Indexer,
}

impl fmt::Display for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Platform => "platform-api",
            Self::Indexer => "indexer-api",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Post,
    Put,
}

impl Verb {
    pub fn method(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Post => Method::POST,
            Self::Put => Method::PUT,
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
}

impl Route {
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
    }
}

/// Every forwarded route. `GET /health` is answered locally and isn't listed.
///
/// `/bots/defaults`, `/bots/stats` and `/bots/leaderboard` overlap
/// `/bots/{id}`. axum prefers the static segment regardless of order;
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
    // Charts: the only route whose upstream path differs.
    Route {
        verb: Get,
        path: "/tokens/{mint}/candles",
        upstream: Upstream::Indexer,
        upstream_path: "/terminal/{mint}/candles",
    },
];

/// Builds the router: the allowed routes, `GET /health`, and a JSON 404 for
/// everything else, including a known path with the wrong method.
pub fn router(proxy: Proxy) -> Router {
    Router::new()
        .merge(group(Upstream::Platform))
        .merge(group(Upstream::Indexer))
        .route("/health", get(health))
        .fallback(not_found)
        // Only applies to routes added before this call.
        .method_not_allowed_fallback(not_found)
        .with_state(proxy)
}

fn group(upstream: Upstream) -> Router<Proxy> {
    ROUTES
        .iter()
        .filter(|route| route.upstream == upstream)
        .fold(Router::new(), |router, route| {
            router.route(route.path, forwarding(route))
        })
}

fn forwarding(route: &'static Route) -> MethodRouter<Proxy> {
    let handler = move |State(proxy): State<Proxy>, request: Request| async move {
        proxy.forward(route, request).await
    };
    match route.verb {
        Get => get(handler),
        Post => post(handler),
        Put => put(handler),
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
    fn should_rename_only_the_candles_route() {
        for route in ROUTES {
            if route.upstream == Upstream::Indexer {
                assert_eq!(route.path, "/tokens/{mint}/candles");
                assert_eq!(route.upstream_path, "/terminal/{mint}/candles");
            } else {
                assert_eq!(
                    route.upstream_path, route.path,
                    "platform API paths are kept"
                );
            }
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
        let candles = ROUTES
            .iter()
            .find(|route| route.upstream == Upstream::Indexer)
            .expect("listed");
        let mint = "So11111111111111111111111111111111111111112";
        let path = format!("/tokens/{mint}/candles");
        let params = candles.params(&path).expect("matches");
        assert_eq!(
            candles.upstream_path_for(&params),
            Some(format!("/terminal/{mint}/candles"))
        );
        assert_eq!(candles.upstream_path_for(&[]), None);
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
            }
        }

        fn echo_router<'a>(routes: impl Iterator<Item = &'a Route>) -> Router {
            routes.fold(Router::new(), |router, route| {
                router.route(route.path, on(filter(route.verb), echo_matched_path))
            })
        }

        async fn matched(router: Router, uri: &str) -> (StatusCode, String) {
            let response = router
                .oneshot(
                    Request::builder()
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
            }
        }
    }
}
