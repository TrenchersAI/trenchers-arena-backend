//! Configuration from environment variables. The platform API URL is
//! required; the other settings have defaults.

use std::net::IpAddr;
use std::time::Duration;

use reqwest::Url;

pub const DEFAULT_PORT: u16 = 8080;
pub const DEFAULT_UPSTREAM_TIMEOUT_MS: u64 = 15_000;

#[derive(Clone, Debug)]
pub struct Config {
    /// The platform API: auth, bots, arena, leaderboard.
    pub platform_api_url: Url,
    pub port: u16,
    /// One upstream call, from connect until its whole body is read, unless
    /// the route sets a longer timeout. Also the limit for reading the
    /// client's request body, on every route.
    pub upstream_timeout: Duration,
    /// Limits for the streamed route. Fixed, not read from the environment.
    pub streams: StreamLimits,
}

/// Limits for a route answered with server-sent events, which the gateway
/// passes on as they arrive instead of reading them whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamLimits {
    /// Until the upstream's response headers arrive. Longer than the app's
    /// 30s, so the app gives up first. On expiry: 502.
    pub headers_timeout: Duration,
    /// Longest silence between two chunks. The AI service sends a keep-alive
    /// every 15s, so this is three missed ones.
    pub idle_timeout: Duration,
    /// The whole call, headers and body. An insight is at most five model
    /// calls of up to 60s each.
    pub max_duration: Duration,
    /// Max open streams per replica. Each holds a permit until it ends.
    pub concurrency: usize,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            headers_timeout: Duration::from_secs(35),
            idle_timeout: Duration::from_secs(45),
            max_duration: Duration::from_secs(300),
            concurrency: 16,
        }
    }
}

/// Messages name the variable but never echo its value, which could hold
/// credentials.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("{var} is required")]
    Missing { var: &'static str },

    #[error("{var} is not a valid URL")]
    InvalidUrl { var: &'static str },

    #[error("{var} must use https (plain http is allowed only for localhost)")]
    InsecureUrl { var: &'static str },

    #[error(
        "{var} must be an origin such as https://host[:port], without credentials, path, query \
         or fragment"
    )]
    NotAnOrigin { var: &'static str },

    #[error("PORT must be an integer from 1 to 65535")]
    InvalidPort,

    #[error("UPSTREAM_TIMEOUT_MS must be a positive whole number of milliseconds")]
    InvalidTimeout,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Reads each variable through `lookup`. Blank values count as unset.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let var = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        let required = |name: &'static str| var(name).ok_or(ConfigError::Missing { var: name });
        let platform_api_url = upstream_url("PLATFORM_API_URL", &required("PLATFORM_API_URL")?)?;
        let port = match var("PORT") {
            None => DEFAULT_PORT,
            Some(raw) => raw
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or(ConfigError::InvalidPort)?,
        };
        let timeout_ms = match var("UPSTREAM_TIMEOUT_MS") {
            None => DEFAULT_UPSTREAM_TIMEOUT_MS,
            Some(raw) => raw
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .ok_or(ConfigError::InvalidTimeout)?,
        };
        Ok(Self {
            platform_api_url,
            port,
            upstream_timeout: Duration::from_millis(timeout_ms),
            streams: StreamLimits::default(),
        })
    }
}

fn upstream_url(var: &'static str, raw: &str) -> Result<Url, ConfigError> {
    let url = Url::parse(raw).map_err(|_| ConfigError::InvalidUrl { var })?;
    if url.host_str().is_none() {
        return Err(ConfigError::InvalidUrl { var });
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::NotAnOrigin { var });
    }
    match url.scheme() {
        "https" => Ok(url),
        "http" if is_localhost(&url) => Ok(url),
        _ => Err(ConfigError::InsecureUrl { var }),
    }
}

fn is_localhost(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const PLATFORM: &str = "https://platform.example.com";

    /// A valid platform API URL plus `vars`, which override it.
    fn config(vars: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let vars: HashMap<String, String> = [("PLATFORM_API_URL", PLATFORM)]
            .iter()
            .chain(vars)
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|name| vars.get(name).cloned())
    }

    fn platform_api_url(value: &str) -> Result<Config, ConfigError> {
        config(&[("PLATFORM_API_URL", value)])
    }

    #[test]
    fn should_default_port_and_timeout() {
        let config = config(&[]).expect("the platform API URL is enough");
        assert_eq!(
            config.platform_api_url.as_str(),
            "https://platform.example.com/"
        );
        assert_eq!(config.port, 8080);
        assert_eq!(config.upstream_timeout, Duration::from_millis(15_000));
    }

    #[test]
    fn should_require_the_platform_api_url() {
        let var = "PLATFORM_API_URL";
        for missing in ["", "  "] {
            assert_eq!(
                config(&[(var, missing)]).expect_err(var),
                ConfigError::Missing { var },
                "{var}={missing:?}"
            );
        }
        let error = Config::from_lookup(|_| None).expect_err("nothing set");
        assert_eq!(
            error,
            ConfigError::Missing {
                var: "PLATFORM_API_URL"
            }
        );
    }

    #[test]
    fn should_treat_blank_values_as_unset() {
        let config = config(&[("PORT", ""), ("UPSTREAM_TIMEOUT_MS", " ")])
            .expect("blank values fall back to defaults");
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(
            config.upstream_timeout,
            Duration::from_millis(DEFAULT_UPSTREAM_TIMEOUT_MS)
        );
    }

    #[test]
    fn should_accept_remote_and_local_upstreams() {
        let config = config(&[
            ("PLATFORM_API_URL", "https://api.example.net"),
            ("PORT", "3000"),
            ("UPSTREAM_TIMEOUT_MS", "2500"),
        ])
        .expect("custom values are valid");
        assert_eq!(config.platform_api_url.as_str(), "https://api.example.net/");
        assert_eq!(config.port, 3000);
        assert_eq!(config.upstream_timeout, Duration::from_millis(2500));

        for local in [
            "http://localhost:8081",
            "http://127.0.0.1:8081",
            "http://[::1]:8081",
        ] {
            assert!(platform_api_url(local).is_ok(), "{local}");
        }
    }

    #[test]
    fn should_require_https_except_for_localhost() {
        for insecure in [
            "http://platform.example.com",
            "http://10.0.0.5:8080",
            "http://localhost.evil.example",
            "ftp://platform.example.com",
        ] {
            assert_eq!(
                platform_api_url(insecure).expect_err(insecure),
                ConfigError::InsecureUrl {
                    var: "PLATFORM_API_URL"
                },
                "{insecure}"
            );
        }
    }

    #[test]
    fn should_require_a_bare_origin() {
        for not_an_origin in [
            "https://user:secret@platform.example.com",
            "https://platform.example.com/v1",
            "https://platform.example.com/?key=1",
            "https://platform.example.com/#top",
        ] {
            assert_eq!(
                platform_api_url(not_an_origin).expect_err(not_an_origin),
                ConfigError::NotAnOrigin {
                    var: "PLATFORM_API_URL"
                },
                "{not_an_origin}"
            );
        }
    }

    #[test]
    fn should_reject_values_that_are_not_urls() {
        for invalid in ["platform.example.com", "https://", "not a url"] {
            assert_eq!(
                platform_api_url(invalid).expect_err(invalid),
                ConfigError::InvalidUrl {
                    var: "PLATFORM_API_URL"
                },
                "{invalid}"
            );
        }
    }

    #[test]
    fn should_never_echo_a_value_in_an_error() {
        let error =
            platform_api_url("https://user:secret@platform.example.com").expect_err("credentials");
        assert!(!error.to_string().contains("secret"), "{error}");
    }

    #[test]
    fn should_reject_bad_ports_and_timeouts() {
        for port in ["0", "65536", "-1", "http", "80.5"] {
            assert_eq!(
                config(&[("PORT", port)]).expect_err(port),
                ConfigError::InvalidPort
            );
        }
        for timeout in ["0", "-5", "1.5", "15s"] {
            assert_eq!(
                config(&[("UPSTREAM_TIMEOUT_MS", timeout)]).expect_err(timeout),
                ConfigError::InvalidTimeout
            );
        }
    }
}
