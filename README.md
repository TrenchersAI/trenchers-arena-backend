# trenchers-arena-backend

Backend for the Trenchers Arena mobile app. It is a small HTTP gateway (Rust, axum)
that forwards an allowlist of routes to two upstream services and refuses everything
else:

```text
Arena app
   |
   v
trenchers-arena-backend  (this repo)
   |
   |-- auth, bots, arena ----> platform API  (PLATFORM_API_URL)
   |
   '-- token charts ---------> indexer API   (INDEXER_API_URL)
```

What the gateway does on every request:

1. Match the method and path against the allowlist in `src/routes.rs`. No match: a
   JSON 404, and nothing is sent upstream.
2. Check each path parameter (`src/validate.rs`). A bad one: a JSON 400, and nothing is
   sent upstream.
3. Forward the request: same method, path and query string, the request body, and only
   the `Content-Type`, `Accept` and `Idempotency-Key` headers, plus `Authorization` for
   the platform API. The indexer never reads a session token, so it never gets one.
4. Return the upstream status and body unchanged, with only `Content-Type` and
   `Cache-Control` copied back.

Each group of routes has its own limit on in-flight requests (`src/routes.rs`): 64 for
the platform API, 32 for the Arena routes and 32 for charts. A request over its group's
limit waits. The app polls the Arena routes, so slow Arena reads fill only their own
limit and never make a sign-in or a bot action wait.

The gateway does not check tokens, clamp query values or cache anything. The platform
API does all of that, and its answers (including 401, 404 and plain-text errors) come
back as it sent them.

Errors the gateway produces itself use the platform API's error body:

```json
{ "error": "route not found: GET /admin/settings", "code": 404, "category": "validation" }
```

## Routes

Platform API routes keep their path. The only renamed route is the chart route.

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
| GET | `/tokens/{mint}/candles` | indexer, as `/terminal/{mint}/candles` | `mint`: base58 address |

### Arena routes

All twelve Arena routes go to the platform API on the same path. The auth and query
columns describe what the platform does with them; the gateway only passes them on.

| Method | Path | Platform auth | Query params (platform default, range) |
|--------|------|---------------|----------------------------------------|
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

### Route precedence

Some patterns share a prefix: `/bots/stats` and `/bots/{id}`, `/arena/seasons` and
`/arena/seasons/{number}`, `/arena/entrants/{id}` and its tabs, and
`/bots/{id}/arena-clone` next to the other `/bots/{id}/...` routes. axum matches whole
segments and prefers a static segment over a `{param}`, whatever the order in
`ROUTES`. The `route_precedence_tests` module in `src/routes.rs` checks this in both
registration orders.

### Adding a route

1. Add one line to `ROUTES` in `src/routes.rs`.
2. If it has a new `{param}` name, add its check to `src/validate.rs` (a unit test fails
   until you do).
3. Add a row to `contract()` in `tests/gateway.rs` (a test fails until the two lists
   match).
4. Add it to the table above.

## Configuration

Read from environment variables only (`src/config.rs`). See `.env.example`.

| Variable | Required | Default | Meaning |
|----------|----------|---------|---------|
| `PLATFORM_API_URL` | yes | | Platform API origin, `https://host[:port]`. Plain `http` only for localhost. |
| `INDEXER_API_URL` | yes | | Indexer API origin, same rules. |
| `PORT` | no | `8080` | Port to listen on. The gateway always binds all interfaces (`0.0.0.0`). |
| `UPSTREAM_TIMEOUT_MS` | no | `15000` | Used twice: as the limit for reading the client's request body (on expiry: 408), and separately as the limit for one upstream call, including its response body (on expiry: 502). A slow body followed by a slow upstream can take up to twice this value. |
| `RUST_LOG` | no | `info` | Log filter. |

The URLs must be bare origins: no path, query, fragment or credentials. Do not commit
real service URLs; keep them in `.env` (git-ignored) or the host's settings.

## Known limit: sign-in rate limits are shared

Read this before pointing the gateway at real users.

The platform API rate-limits `POST /auth/privy` (20 per minute) and `POST /auth/refresh`
(120 per minute) per client IP. It takes the client IP only from `CF-Connecting-IP`, the
header Cloudflare sets, and puts every caller without it in one shared bucket. It
deliberately ignores `X-Forwarded-For`, because clients can fake it.

The gateway drops all client-IP headers, so the platform sees every app user as one
caller: either the shared bucket or the gateway's own address. Across the whole user
base that is 20 sign-ins and 120 refreshes per minute in total. The 21st sign-in in a
minute gets `429` even if that user tried once.

Forwarding `CF-Connecting-IP` from the gateway does not fix this: a client could set it
to anything, and Cloudflare in front of the platform would overwrite it. The fix needs a
platform API change, for example trusting a client-IP header only from the gateway (with
a shared secret), or giving the gateway its own larger budget while the gateway limits
sign-ins per client IP itself. Until then, this gateway is fine for testing and small
groups, not for a public launch.

## Running locally

```sh
cp .env.example .env              # fill in PLATFORM_API_URL and INDEXER_API_URL
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

`tests/gateway.rs` runs the gateway on a local port in front of two
[wiremock](https://docs.rs/wiremock) servers and checks what each one receives. CI
(`.github/workflows/ci.yml`) runs the three commands above and a Docker build on every
pull request.
