# trenchers-arena-backend

Backend for the Trenchers Arena mobile app. It is a small HTTP gateway (Rust, axum)
that forwards an allowlist of routes to the platform API and the Robinhood Chain read
API, and refuses everything else:

```text
Arena app
   |
   v
trenchers-arena-backend  (this repo)
   |
   |-- auth, bots, arena, trading,
   |   Robinhood Chain trading (/hood/*) --> platform API  (PLATFORM_API_URL)
   '-- Robinhood Chain market data
       (/robinhood/*)                    --> hood API      (HOOD_API_URL, optional)
```

Solana market data (charts, the pulse feed) and all WebSockets do not go through the
gateway; the app calls those services directly (see
[Pointing the mobile app at the gateway](#pointing-the-mobile-app-at-the-gateway)).
Robinhood Chain market data does go through it (see
[Robinhood Chain](#robinhood-chain)).

What the gateway does on every request:

1. Match the method and path against the allowlist in `src/routes.rs`. No match: a
   JSON 404, and nothing is sent upstream.
2. Check each path parameter (`src/validate.rs`). A bad one: a JSON 400, and nothing is
   sent upstream.
3. Forward the request: same method, path and query string, the request body, and only
   the `Authorization`, `Content-Type`, `Accept` and `Idempotency-Key` headers.
   `Authorization` goes to the platform API only, never to the hood API.
4. Return the upstream status and body unchanged, with only `Content-Type`,
   `Cache-Control` and `Retry-After` copied back. The one streamed route passes its
   events back as they arrive (see [Streaming](#streaming)).

Each group of routes has its own limit on in-flight requests (`src/routes.rs`): 64 for
the platform API, 32 for the Arena routes, 32 for the Robinhood Chain reads and 16 for
the routes that move money. A request over its group's limit waits. The app polls the Arena routes, so slow
Arena reads fill only their own limit and never make a sign-in or a bot action wait. The
app's polling (fleet stats every 30s) sits in the platform group, so it never makes a
swap or an order wait either. The platform's own money group allows 10 in flight and
queues the rest; 16 is above that, so the gateway only holds money calls back once the
platform is badly backed up. Streamed AI answers have their own limit of 16 open
streams per replica, and a request over it gets a `503` at once instead of waiting (see
[Streaming](#streaming)).

The gateway does not check tokens, clamp query values or cache anything. The platform
API does all of that, and its answers (including 401, 404 and plain-text errors) come
back as it sent them.

Errors the gateway produces itself use the platform API's error body:

```json
{ "error": "route not found: GET /admin/settings", "code": 404, "category": "validation" }
```

## Routes

Every route goes to the platform API on the same path.

| Method | Path | Upstream | Path params |
|--------|------|----------|-------------|
| POST | `/auth/privy` | platform | |
| POST | `/auth/refresh` | platform | |
| GET | `/me` | platform | |
| GET | `/paper-wallet` | platform | |
| GET | `/bots` | platform | |
| POST | `/bots` | platform | |
| GET | `/bots/defaults` | platform | |
| GET | `/bots/stats` | platform | |
| GET | `/bots/leaderboard` | platform | |
| GET | `/bots/{id}` | platform | `id`: UUID |
| PUT | `/bots/{id}/config` | platform | `id`: UUID |
| POST | `/bots/{id}/start` | platform | `id`: UUID |
| POST | `/bots/{id}/pause` | platform | `id`: UUID |
| POST | `/bots/{id}/resume` | platform | `id`: UUID |
| POST | `/bots/{id}/allocate` | platform | `id`: UUID |
| GET | `/bots/{id}/stats` | platform | `id`: UUID |
| GET | `/bots/{id}/positions` | platform | `id`: UUID |
| GET | `/bots/{id}/trades` | platform | `id`: UUID |
| GET | `/bots/{id}/watching` | platform | `id`: UUID |
| GET | `/me/settings` | platform | |
| PUT | `/me/settings` | platform | |
| GET | `/portfolio` | platform | |
| GET | `/portfolio/holdings` | platform | |
| GET | `/portfolio/realized` | platform | |
| GET | `/trades` | platform | |
| GET | `/fees/recommendation` | platform | |
| GET | `/wallets/{address}/sol-balance` | platform | `address`: base58 address |
| GET | `/bots/wallets` | platform | |
| GET | `/orders` | platform | |
| GET | `/orders/{id}/events` | platform | `id`: UUID |
| POST | `/public/tokens-meta` | platform | |
| DELETE | `/orders/{id}` | platform (money route, 35s) | `id`: UUID |
| POST | `/bots/{id}/ai/insight/stream` | platform (streamed, see [Streaming](#streaming)) | `id`: UUID |
| POST | `/bots/live-confirm` | platform | |
| POST | `/bots/{id}/archive` | platform | `id`: UUID |
| GET | `/bots/{id}/wallet` | platform | `id`: UUID |
| POST | `/bots/{id}/wallet` | platform | `id`: UUID |
| POST | `/bots/{id}/wallet/delegated` | platform | `id`: UUID |
| POST | `/bots/{id}/wallet/fund` | platform (money route, 20s) | `id`: UUID |
| POST | `/bots/{id}/wallet/withdraw` | platform (money route, 20s) | `id`: UUID |
| GET | `/bots/{id}/wallet/funding/{funding_id}` | platform | `id`, `funding_id`: UUID |
| POST | `/trade/swap` | platform (money route, 65s) | |
| POST | `/trade/swap/prepare` | platform (money route, 35s) | |
| POST | `/trade/swap/complete` | platform (money route, 35s) | |
| POST | `/trade/sell-init/quote` | platform (money route, 35s) | |
| POST | `/orders` | platform (money route, 65s) | |
| POST | `/solana/submit` | platform (money route, 35s) | |
| POST | `/solana/rpc` | platform | |
| GET | `/solana/blockhash` | platform | |
| POST | `/hood/wallets/register` | platform | |
| POST | `/hood/trade/quote` | platform (money route, 35s) | |
| POST | `/hood/trade/swap` | platform (money route, 65s) | |
| GET | `/hood/trade/orders/{id}` | platform | `id`: UUID |
| GET | `/robinhood/board` | hood | |
| GET | `/robinhood/pulse` | hood | |
| GET | `/robinhood/search` | hood | |
| GET | `/robinhood/tokens/{token}` | hood | `token`: EVM address |
| GET | `/robinhood/tokens/{token}/trades` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/candles` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/marks` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/holders` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/dev-tokens` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/status` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/top-traders` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/trades` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/overview` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/open` | hood | `token`: EVM address |
| GET | `/robinhood/terminal/{token}/funding` | hood | `token`: EVM address |
| GET | `/robinhood/wallets/{wallet}/balances` | hood | `wallet`: EVM address |

`/fees/recommendation`, `/wallets/{address}/sol-balance` and `/public/tokens-meta` take
no token on the platform; the others need one. `/public/tokens-meta` takes a JSON body
`{ "mints": [...] }` (at most 100) and shares one rate limit across all app users (see
[Known limit](#known-limit-per-ip-rate-limits-are-shared)). The swaps, orders, bot wallet
transfers and `/solana/submit` are money routes (see [Money routes](#money-routes)).
Other platform trading and bot wallet routes (`/trade/preview`, `/trade/quote`, the
wallet sweep, migrate and locked-rent routes, `POST /bots/{id}/withdraw`) are not
forwarded.

Live agents, briefly (the platform does the checks; the gateway only passes them on):

- **Wallet reads.** `GET /bots/{id}/wallet` answers `404` before the bot has a wallet,
  and the app reads that as "no wallet yet". It is the platform's 404, passed through;
  the gateway's own 404 (`category: validation`) means the route isn't listed.
- **Funding.** `fund` and `withdraw` are server-signed transfers between the Trenchers
  wallet and the bot wallet, with `{ "amount_lamports": <int> }` (`"all"` on a
  withdraw). They carry no idempotency key. `GET /bots/{id}/wallet/funding/{funding_id}`
  reads one transfer, owner-scoped (404 otherwise).
- **Swaps.** The app tries a client-signed swap first (`/trade/swap/prepare`, sign,
  `/solana/submit`, `/trade/swap/complete`). The platform answers `prepare` with `404`
  unless `ST_CLIENT_SIGNING` is on in that environment, and the app then falls back to
  the server-signed `POST /trade/swap`, whose idempotency key is the `idempotency_key`
  field in the body.
- **Solana relay.** `/solana/rpc` takes a JSON-RPC body; the platform allowlists the
  methods. `/solana/submit` takes `{ "transaction": <base64> }`.

### Money routes

Routes that move money (swaps, orders, wallet transfers) are declared with `trade()`
in `src/routes.rs`. Each one gets:

- **Its own limit group** (16 in flight), separate from the app's polling.
- **Its own upstream timeout**, a few seconds longer than the app waits for that call,
  so the gateway never gives up first. `UPSTREAM_TIMEOUT_MS` is a floor: if it is set
  higher than a route's timeout, the route uses it. Reading the request body keeps
  `UPSTREAM_TIMEOUT_MS` on every route: nothing has gone upstream yet, so its 408 is
  safe to retry, and a stalled client can't hold a money permit for long.

| Route | Gateway timeout | App timeout |
|-------|-----------------|-------------|
| `POST /trade/swap`, `POST /orders`, `POST /hood/trade/swap` | 65s | 60s |
| `DELETE /orders/{id}`, `POST /trade/swap/prepare`, `/trade/swap/complete`, `/trade/sell-init/quote`, `POST /solana/submit`, `POST /hood/trade/quote` | 35s | 30s |
| `POST /bots/{id}/wallet/fund`, `/bots/{id}/wallet/withdraw` | 20s | 15s (the app's default) |

`/trade/sell-init/quote` moves no money (the platform treats it as a read) but sits in
the money group for its longer timeout. `/solana/rpc` and `/solana/blockhash` stay in
the platform group on the default timeout.

When a money route times out the app gets the gateway's
`502 {"error":"platform-api did not answer in time","code":502,"category":"upstream"}`,
but the platform may still finish the work. Treat that 502 as "outcome unknown": check
the order or trade list before retrying, because `POST /orders` and the wallet transfers
carry no idempotency key. The gateway never retries a request by itself.

The platform's 429 and 503 answers may carry `Retry-After`; it comes back to the app
unchanged.

### Streaming

`POST /bots/{id}/ai/insight/stream` (the bot's AI insight card) is answered with
server-sent events: `text {delta}` while the answer is written, then `done {reply}` or
`error {message}`. It needs a token and a JSON body `{ "message": ... }`. It is declared
with `stream()` in `src/routes.rs`; every other route is read whole before it is sent
back.

- **When it streams.** Only when the platform answers 2xx with
  `Content-Type: text/event-stream`. Anything else (the AI rate limit's `429`, a `503`,
  a `401` when AI access is off, a JSON body) is read whole and comes back unchanged,
  `Retry-After` included. A `401` here does not mean the session ended.
- **Headers.** `Content-Type` and `Cache-Control` come back from the platform, plus
  `X-Accel-Buffering: no`, set by the gateway so a proxy in front of it doesn't buffer
  the stream. There is no `Content-Length`; the body is chunked. The gateway never
  compresses or buffers it.
- **Timeouts** (`StreamLimits` in `src/config.rs`; fixed, not environment variables):

  | Limit | Value | Why | On expiry |
  |-------|-------|-----|-----------|
  | Response headers | 35s | The app waits 30s for them, so it gives up first | `502` in the usual error body |
  | Silence between chunks | 45s | The AI service sends a keep-alive every 15s | Stream ends (below) |
  | Whole call | 5 min | An insight is at most five model calls of up to 60s | Stream ends (below) |

- **Ending a stream early.** Once events have gone out, the status can't change. When
  the gateway cuts a stream (silence, the 5-minute cap, or the platform's connection
  breaking off), it sends one last event,
  `event: error` / `data: {"message":"The answer was cut off. Please retry."}`, and ends
  the body cleanly, so the app shows a retry message instead of a truncated answer. If
  the app has already been sent `done` or `error`, nothing is added and the stream just
  ends: the app treats any `error` event as a failure, even after `done`.
- **Limit.** At most 16 open streams per replica. Each holds its permit until the stream
  ends, not just until its headers. A request over the limit gets
  `503 {"error":"too many AI answers in progress, retry shortly","code":503,"category":"upstream"}`
  at once, because waiting would use up the app's 30s wait for the first byte. Streams
  don't count against the other groups' limits.
- **The app going away.** The gateway drops its connection to the platform as soon as it
  notices, at its next write (the keep-alives make that within about 15s), and doesn't
  read the rest of the stream. The platform then cancels the answer.

### Arena routes

All thirteen Arena routes go to the platform API on the same path. The auth and query
columns describe what the platform does with them; the gateway only passes them on.

| Method | Path | Platform auth | Query params (platform default, range) |
|--------|------|---------------|----------------------------------------|
| GET | `/arena/paper-bots` | none (gate only) | none; rolling 24h closed profit before fees, refreshed every 60s |
| GET | `/arena/season` | none (a token only affects the arena gate) | |
| GET | `/arena/entrants/{id}` | optional | |
| GET | `/arena/feed` | none (gate only) | `limit` (30, 1..100), `before` (cursor) |
| GET | `/arena/seasons` | none (gate only) | |
| GET | `/arena/seasons/{number}` | none (gate only) | |
| GET | `/arena/entrants/{id}/positions` | required | |
| GET | `/arena/entrants/{id}/trades` | required | `limit` (50, 1..500) |
| GET | `/arena/entrants/{id}/watching` | required | |
| GET | `/arena/entrants/{id}/config-history` | required | `limit` (50, 1..200) |
| GET | `/arena/entrants/{id}/transcript` | required | `runs` (20, 1..50) |
| POST | `/arena/entrants/{id}/clone` | required, plus the `tuner` flag | JSON body `{ name, idempotency_key }` |
| GET | `/bots/{id}/arena-clone` | required (bot owner only) | |

Path params: `{id}` is a UUID (an entrant id, or a bot id for `/bots/{id}/arena-clone`);
`{number}` is a season number (a 32-bit integer).

Things worth knowing when calling them through the gateway:

- **Arena gate.** With the `arena` feature flag off for the caller, the platform answers
  every Arena route (including `/bots/{id}/arena-clone`) with
  `404 {"error":"arena not found: <path>","code":404,"category":"internal"}`. The gateway
  passes it through as is, so check the HTTP status, not the category.
- **Clone retries.** The idempotency key is the `idempotency_key` field in the JSON body.
  Reuse it when retrying the same clone; the platform answers a replay with the same 200
  body. The gateway never retries a request by itself.
- **Caching.** The platform's `Cache-Control` comes back unchanged (for example
  `public, max-age=15` on the first feed page, `public, max-age=3600` on a page with
  `before`, `public, max-age=86400, immutable` on past seasons). The signed-in dashboard
  tabs set none, and the gateway adds none.

There is no Arena websocket; the app polls these routes.

The source for these facts is the platform API (`solana-terminal`, `crates/st-api`).

### Robinhood Chain

Hood v2 (Robinhood Chain, chain 4663) is built on the `robinhood` branch of
solana-terminal; its design is `docs/architecture/hood-v2.md` there. The gateway
forwards both of its HTTP surfaces:

- **Market data** (`/robinhood/...`) goes to the hood API, `st-indexerd-evm`'s read
  role (the `evm-api` deployment), on the same path. These are the paths the web app
  calls against `VITE_HOOD_API_BASE_URL`, so the app can use them unchanged. They are
  public reads: the gateway never sends `Authorization` there, and the hood API (and its
  ingress) does no per-IP rate limiting today, so the shared-IP problem in
  [Known limit](#known-limit-per-ip-rate-limits-are-shared) does not apply. Its errors
  come back as it sent them, `{ "error": "..." }`: a `404` for an unknown token or a
  chain it does not index, a `400` for a bad query value, `502`/`503` when ClickHouse
  or the chain RPC is down. `/healthz` is not forwarded.
- **Trading** (`/hood/...`) goes to the platform API (`st-api/src/hood/`, behind
  `require_auth` and the `hood_trading` user flag). The quote and the swap are money
  routes. **These four routes are listed from the design (section 11) and not built
  yet**: until st-api ships them, the platform answers `404`. Check their paths and
  bodies against st-api when it does.

`{token}` and `{wallet}` must be `0x` and 40 hex digits, in any case (the hood API
lowercases them); anything else is a `400` and nothing is forwarded. While
`HOOD_API_URL` is unset, every `/robinhood/...` route answers
`503 {"error":"hood-api is not configured","code":503,"category":"upstream"}` and calls
nothing.

Live updates come from the hood API's WebSocket (`wss://<hood host>/v2/robinhood/ws`),
which the app opens directly, like the other sockets.

### Route precedence

Some patterns share a prefix: `/bots/stats` (and the other static `/bots/...` reads,
including `/bots/wallets`) and `/bots/{id}`, `/arena/seasons` and
`/arena/seasons/{number}`, `/arena/entrants/{id}` and its tabs,
`/bots/{id}/arena-clone` next to the other `/bots/{id}/...` routes, and
`DELETE /orders/{id}` next to `GET /orders/{id}/events`,
`/bots/{id}/ai/insight/stream` under `/bots/{id}`, `/bots/live-confirm` and
`/bots/{id}`, and the `/bots/{id}/wallet/...` routes (`fund` next to
`funding/{funding_id}`), and the `/robinhood/tokens/{token}` and
`/robinhood/terminal/{token}/...` families. axum matches whole
segments and prefers a static segment over a `{param}`, whatever the order in
`ROUTES`. The `route_precedence_tests` module in `src/routes.rs` checks this in both
registration orders.

### Adding a route

1. Add one line to `ROUTES` in `src/routes.rs`: `api(verb, path)`, or
   `trade(verb, path, timeout)` for a route that moves money (see Money routes), and
   add it to `TRADE_ROUTES` in the unit tests. A Robinhood Chain read uses
   `hood(path)`. A route answered with server-sent
   events uses `stream(verb, path)` (see Streaming).
2. If it has a new `{param}` name, add its check to `src/validate.rs` (a unit test fails
   until you do).
3. Add a row to `contract()` (or `hood_cases()`) in `tests/gateway.rs` (a test fails
   until the two lists match).
4. Add it to the table above.

## Configuration

Read from environment variables only (`src/config.rs`). See `.env.example`.

| Variable | Required | Default | Meaning |
|----------|----------|---------|---------|
| `PLATFORM_API_URL` | yes | | Platform API origin, `https://host[:port]`. Plain `http` only for localhost. |
| `HOOD_API_URL` | no | | Robinhood Chain read API origin (`evm-api`), same rules as `PLATFORM_API_URL`. Unset: the `/robinhood/...` routes answer 503. |
| `PORT` | no | `8080` | Port to listen on. The gateway always binds all interfaces (`0.0.0.0`). |
| `UPSTREAM_TIMEOUT_MS` | no | `15000` | Used twice: as the limit for reading the client's request body on every route (on expiry: 408), and separately as the default limit for one upstream call, including its response body (on expiry: 502). Money routes set a longer upstream timeout (see Money routes), and this value is their floor. A slow body followed by a slow upstream can take the two limits added up. |
| `RUST_LOG` | no | `info` | Log filter. |

Each URL must be a bare origin: no path, query, fragment or credentials. Do not commit
real service URLs; keep them in `.env` (git-ignored) or the host's settings.

## Known limit: per-IP rate limits are shared

Read this before pointing the gateway at real users.

The platform API rate-limits `POST /auth/privy` (20 per minute) and `POST /auth/refresh`
(120 per minute) per client IP. It takes the client IP only from `CF-Connecting-IP`, the
header Cloudflare sets, and puts every caller without it in one shared bucket. It
deliberately ignores `X-Forwarded-For`, because clients can fake it.

The gateway drops all client-IP headers, so the platform sees every app user as one
caller: either the shared bucket or the gateway's own address. Across the whole user
base that is 20 sign-ins and 120 refreshes per minute in total. The 21st sign-in in a
minute gets `429` even if that user tried once.

`POST /public/tokens-meta` is affected the same way. The platform limits it to 60
requests per minute per client IP (`pubip:{CF-Connecting-IP}`), so through the gateway
all app users share those 60. The app calls it whenever it needs names and logos for
mints it has not seen, so expect `429`s on it with more than a handful of active users.

Forwarding `CF-Connecting-IP` from the gateway does not fix this: a client could set it
to anything, and Cloudflare in front of the platform would overwrite it. The fix needs a
platform API change, for example trusting a client-IP header only from the gateway (with
a shared secret), or giving the gateway its own larger budget while the gateway limits
sign-ins per client IP itself. Until then, this gateway is fine for testing and small
groups, not for a public launch.

## Running locally

```sh
cp .env.example .env              # fill in PLATFORM_API_URL
set -a && . ./.env && set +a
cargo run
curl -s http://localhost:8080/health
curl -s http://localhost:8080/arena/season
```

Or with Docker:

```sh
docker build -t trenchers-arena-backend .
docker run --rm -p 8080:8080 --env-file .env trenchers-arena-backend
```

## Pointing the mobile app at the gateway

The app reads one REST base URL, `EXPO_PUBLIC_API_BASE_URL`
(`src/api/environment.ts` in the app repo). Set it to the gateway's origin, with no
path:

```sh
EXPO_PUBLIC_API_BASE_URL=https://<gateway host>
```

The app then calls `https://<gateway host>/arena/season`, `/auth/privy` and so on, and
the gateway forwards them.

Solana market data stays off the gateway. `EXPO_PUBLIC_PULSE_HTTP_URL` (pulse, terminal data
including candles, token images) points straight at the indexer, and `EXPO_PUBLIC_WS_URL`
and `EXPO_PUBLIC_PULSE_WS_URL` straight at st-ws-server. Leave them as they are: those
services rate-limit by the real client IP (`CF-Connecting-IP`), and a gateway in between
would put every app user in one bucket (see
[Known limit](#known-limit-per-ip-rate-limits-are-shared)).

Two rules from the app's code shape local testing:

- The app accepts only an `https://` base URL (`validateApiBase` rejects anything else,
  including `http://localhost`).
- `EXPO_PUBLIC_*` values are inlined when Metro builds the bundle, so restart Metro
  with `--clear` after changing it.

### Testing on a phone against a local gateway

Because the app needs HTTPS, put an HTTPS tunnel in front of the local gateway:

```text
Phone app --https--> tunnel (public URL) --http--> laptop :8080 (gateway) --https--> platform API
```

1. Start the gateway on the laptop (see Running locally). It listens on all
   interfaces, port `PORT` (8080 by default).
2. Start a tunnel to it, for example with Cloudflare's quick tunnel:

   ```sh
   cloudflared tunnel --url http://localhost:8080
   ```

   It prints an `https://<random>.trycloudflare.com` URL. `ngrok http 8080` works the
   same way.
3. In the app repo, start Metro with that URL as the API base and open the app on the
   phone as its README describes (`npm run android:device`):

   ```sh
   EXPO_NO_DOTENV=1 EXPO_PUBLIC_API_BASE_URL=https://<random>.trycloudflare.com npm start -- --clear
   ```

   For a standalone preview build (`npm run android:api-preview`), put the same value in
   the app's `.env` instead; that script reads it from there.
4. Check from the phone's browser that `https://<random>.trycloudflare.com/health`
   answers `{"status":"ok",...}`.

`adb reverse tcp:8080 tcp:8080` also makes the laptop's port 8080 reachable as
`localhost:8080` on a USB-connected phone, which is handy for `curl` checks. The app
itself will not use it, because the URL would be `http://`.

Anyone with the tunnel URL can reach the gateway, and through it the allowlisted routes
of whatever `PLATFORM_API_URL` points at, while the tunnel runs. Stop the tunnel when
you are done.

## Tests and CI

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

`tests/gateway.rs` runs the gateway on a local port in front of a
[wiremock](https://docs.rs/wiremock) server and checks what it receives.
`tests/streaming.rs` puts a small axum upstream behind it instead, which feeds an event
stream chunk by chunk, to test streaming, its timeouts, the AI limit and cancellation. CI
(`.github/workflows/ci.yml`) runs the three commands above and a Docker build on every
pull request.
