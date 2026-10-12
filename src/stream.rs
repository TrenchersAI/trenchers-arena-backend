//! Passes a server-sent event stream back as it arrives, for the routes marked
//! [`Delivery::Stream`](crate::routes::Delivery::Stream).
//!
//! Once the first bytes have gone out the status can't change, so a stream
//! the gateway has to cut (idle, too long, upstream broke off) ends with an
//! SSE `error` event the app already handles, unless the app has already been
//! sent its final `done` or `error` event. Then it just ends.
//!
//! The body owns the upstream response and the stream's permit. When the app
//! goes away hyper drops the body, which drops the upstream response before
//! its end: that connection closes instead of being read out, and the
//! platform cancels the answer.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, HttpBody};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::response::Response;
use http_body::Frame;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Sleep;

use crate::routes::Route;

/// Response headers copied back on a stream. hyper sends the body chunked, so
/// `Content-Length` is never copied.
pub static STREAMED_RESPONSE_HEADERS: [HeaderName; 2] =
    [header::CONTENT_TYPE, header::CACHE_CONTROL];

/// Set by the gateway rather than copied: it tells a proxy in front of the
/// gateway (nginx reads it) not to buffer, so it has to be on the response
/// that leaves the gateway.
const X_ACCEL_BUFFERING: HeaderName = HeaderName::from_static("x-accel-buffering");

/// Sent when the gateway cuts a stream. The leading blank line ends any event
/// the cut left half-written; the app skips it as malformed.
pub const CUT_NOTICE: &[u8] =
    b"\n\nevent: error\ndata: {\"message\":\"The answer was cut off. Please retry.\"}\n\n";

/// A 2xx with `Content-Type: text/event-stream`. Anything else (a 429, a 401,
/// a JSON body) is read whole like on every other route.
pub fn is_event_stream(response: &reqwest::Response) -> bool {
    response.status().is_success()
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("text/event-stream")
            })
}

/// The response to pass back: upstream status and headers, and a body that
/// forwards each chunk as it arrives and holds `permit` until it ends.
pub fn respond(
    route: &'static Route,
    response: reqwest::Response,
    permit: OwnedSemaphorePermit,
    idle_timeout: Duration,
) -> Response {
    let mut headers = HeaderMap::new();
    for name in &STREAMED_RESPONSE_HEADERS {
        for value in response.headers().get_all(name) {
            headers.append(name.clone(), value.clone());
        }
    }
    headers.insert(X_ACCEL_BUFFERING, HeaderValue::from_static("no"));
    let status = response.status();
    let body = EventStream {
        // Keeps reqwest's timeout, the stream's maximum length.
        upstream: reqwest::Body::from(response),
        idle: Box::pin(tokio::time::sleep(idle_timeout)),
        idle_timeout,
        final_event: FinalEvent::default(),
        state: State::Open,
        route,
        started: Instant::now(),
        bytes: 0,
        _permit: permit,
    };
    let mut out = Response::new(Body::new(body));
    *out.status_mut() = status;
    *out.headers_mut() = headers;
    out
}

struct EventStream {
    upstream: reqwest::Body,
    /// Reset on every chunk.
    idle: Pin<Box<Sleep>>,
    idle_timeout: Duration,
    final_event: FinalEvent,
    state: State,
    route: &'static Route,
    started: Instant,
    bytes: u64,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Open,
    /// Cut by the gateway; [`CUT_NOTICE`] goes out next.
    Cut,
    Ended,
}

/// Why a stream ended, for the log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    /// The upstream finished it.
    Complete,
    /// No chunk within the idle timeout.
    Idle,
    /// reqwest's timeout, set to the stream's maximum length.
    MaxDuration,
    /// The upstream connection broke off.
    UpstreamError,
    /// The app went away first.
    ClientGone,
}

impl End {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Idle => "idle",
            Self::MaxDuration => "max_duration",
            Self::UpstreamError => "upstream_error",
            Self::ClientGone => "client_gone",
        }
    }
}

impl EventStream {
    fn end(&mut self, end: End, error: Option<reqwest::Error>) {
        let cut = matches!(end, End::Idle | End::MaxDuration | End::UpstreamError);
        let notified = cut && !self.final_event.seen;
        self.state = if notified { State::Cut } else { State::Ended };
        let duration_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let reason = end.as_str();
        if cut {
            // Drop the URL so ids stay out of the logs.
            let error = error.map(|error| error.without_url().to_string());
            tracing::warn!(
                route = self.route.path,
                upstream = %self.route.upstream,
                reason,
                error = error.as_deref().unwrap_or(""),
                notified,
                bytes = self.bytes,
                duration_ms,
                "stream cut"
            );
        } else {
            tracing::info!(
                route = self.route.path,
                upstream = %self.route.upstream,
                reason,
                bytes = self.bytes,
                duration_ms,
                "stream ended"
            );
        }
    }
}

impl HttpBody for EventStream {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        loop {
            match this.state {
                State::Ended => return Poll::Ready(None),
                State::Cut => {
                    this.state = State::Ended;
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(CUT_NOTICE)))));
                }
                State::Open => {}
            }
            match Pin::new(&mut this.upstream).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    // Trailers are not forwarded.
                    let Ok(data) = frame.into_data() else {
                        continue;
                    };
                    this.final_event.feed(&data);
                    this.bytes += data.len() as u64;
                    let deadline = tokio::time::Instant::now() + this.idle_timeout;
                    this.idle.as_mut().reset(deadline);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(None) => this.end(End::Complete, None),
                Poll::Ready(Some(Err(error))) => {
                    let end = if error.is_timeout() {
                        End::MaxDuration
                    } else {
                        End::UpstreamError
                    };
                    this.end(end, Some(error));
                }
                Poll::Pending => {
                    if this.idle.as_mut().poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    this.end(End::Idle, None);
                }
            }
        }
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if self.state == State::Open {
            self.end(End::ClientGone, None);
        }
    }
}

/// Watches the forwarded bytes for an `event: done` or `event: error` line,
/// the app's two final events. A line may arrive split across chunks. The
/// event name is trimmed, as the app's parser does.
#[derive(Debug, Default)]
struct FinalEvent {
    /// The start of the current line.
    line: Vec<u8>,
    /// The current line is longer than [`FinalEvent::KEEP`], so it is neither.
    long: bool,
    seen: bool,
}

impl FinalEvent {
    const KEEP: usize = 32;

    fn feed(&mut self, bytes: &[u8]) {
        if self.seen {
            return;
        }
        for &byte in bytes {
            if byte == b'\n' || byte == b'\r' {
                if !self.long && is_final_event(&self.line) {
                    self.seen = true;
                    return;
                }
                self.line.clear();
                self.long = false;
            } else if self.line.len() < Self::KEEP {
                self.line.push(byte);
            } else {
                self.long = true;
            }
        }
    }
}

fn is_final_event(line: &[u8]) -> bool {
    line.strip_prefix(b"event:")
        .map(<[u8]>::trim_ascii)
        .is_some_and(|name| name == b"done" || name == b"error")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(chunks: &[&str]) -> bool {
        let mut final_event = FinalEvent::default();
        for chunk in chunks {
            final_event.feed(chunk.as_bytes());
        }
        final_event.seen
    }

    #[test]
    fn should_see_a_final_event_line_however_it_is_split() {
        let done = "event: done\ndata: {\"reply\":\"ok\"}\n\n";
        for at in 0..=done.len() {
            assert!(seen(&[&done[..at], &done[at..]]), "split at {at}");
        }
        let bytes: Vec<String> = done.chars().map(String::from).collect();
        let bytes: Vec<&str> = bytes.iter().map(String::as_str).collect();
        assert!(seen(&bytes), "one byte at a time");
    }

    #[test]
    fn should_see_done_and_error_in_every_form_the_app_reads() {
        for chunk in [
            "event: done\n",
            "event:done\n",
            "event:  done \n",
            "event: done\r\n",
            "event: done\r",
            "event: error\n",
            ": keep-alive\n\nevent: text\ndata: {\"delta\":\"a\"}\n\nevent: error\n",
        ] {
            assert!(seen(&[chunk]), "{chunk:?}");
        }
    }

    #[test]
    fn should_ignore_other_events_and_lines_that_only_mention_done() {
        for chunk in [
            "event: text\ndata: {\"delta\":\"done\"}\n\n",
            "data: event: done\n",
            " event: done\n",
            "event: done2\n",
            "event: errors\n",
            "id: done\n",
            ": event: done\n",
            // Not a complete line yet.
            "event: done",
            // Too long to be either, even though it starts like one.
            "event: done                                      x\n",
        ] {
            assert!(!seen(&[chunk]), "{chunk:?}");
        }
    }
}
