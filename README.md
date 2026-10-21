# TradeLocker Streams API

Real-time account updates for TradeLocker accounts over [Socket.IO](https://socket.io/): account status
(balance, equity, margin, per-position PnL), positions, closed positions and orders. One connection
carries all the accounts you are allowed to stream.

| | |
|---|---|
| **Docs** | https://tradelocker.github.io/streams-api-docs/ |
| **Contract** | [`asyncapi/streams-api.asyncapi.yaml`](asyncapi/streams-api.asyncapi.yaml) · [JSON](asyncapi/streams-api.asyncapi.json) (AsyncAPI 3.0) |
| **Examples** | [Node.js / TypeScript](examples/node) · [Python](examples/python) · [Java](examples/java) · [C# / .NET](examples/csharp) |
| **For AI coding agents** | [`AGENTS.md`](AGENTS.md) |
| **Local mock server** | [`mock-server/`](mock-server/server.mjs) |

You need a **developer API key** from TradeLocker and the TradeLocker login (email, password, server)
of the user whose accounts you stream.

## How it works

```text
your service                          TradeLocker
────────────                          ───────────
POST /backend-api/auth/jwt/accounts/tokens  ──►  one JWT per account
     {email, password, server}

Socket.IO connect (one connection)    ──►  /streams-api, path /streams-api/socket.io,
  header developer-api-key                 websocket only
                                      ◄──  "connection" {status: ok, code: connected}

for each account:
  emit "subscriptions"                ──►  {action: SUBSCRIBE, token}
                                      ◄──  "stream" snapshot: AccountStatus, Position, OpenOrder ...
                                      ◄──  "stream" {type: Property, name: SyncEnd}
                                      ◄──  acknowledgement {status: ok, code: connected}
                                      ◄──  "stream" live updates ...
```

| Environment | Streams API (Socket.IO base URL) | Account tokens |
|---|---|---|
| Production | `https://api.tradelocker.com` or `https://connect.tradelocker.com` | `https://live.tradelocker.com/backend-api` (live accounts), `https://demo.tradelocker.com/backend-api` (demo accounts) |
| Development | `https://api-dev.tradelocker.com` | `https://stg.tradelocker.com/backend-api` |

Socket.IO options in every environment: namespace `/streams-api`, `path: "/streams-api/socket.io"`,
`transports: ["websocket"]`, header `developer-api-key`.

## Before you write code: the mistakes everyone makes

1. **A connection per account.** Use one connection for all accounts; connections per key are capped (`conflict`).
2. **Brand from user input.** Read `brand` and `aid` from the account token's claims. `Blueg` ≠ `BLUEG`, and the server matches exactly.
3. **Waiting for `SyncEnd` after the acknowledgement.** It arrives before the ack. Register the wait before you emit SUBSCRIBE.
4. **Subscribing once at startup.** Subscriptions belong to a connection; after every reconnect, subscribe all accounts again.
5. **Trusting auto-reconnect.** Socket.IO does not reconnect after a server-side close (`io server disconnect`). Handle it.
6. **Short acknowledgement timeouts.** A subscribe can take up to about 60 s.
7. **Floats for money, numbers for IDs.** Values are decimal strings; IDs exceed 2^53.

The full list, with what to do for every result code, is in [`AGENTS.md`](AGENTS.md) and in the
"Rules" section of the docs.

## Run an example

Each example reads the same environment variables:

| Variable | |
|---|---|
| `TL_DEVELOPER_API_KEY` | your developer API key |
| `TL_EMAIL`, `TL_PASSWORD`, `TL_SERVER` | the user's TradeLocker login |
| `TL_BACKEND_URL` | token endpoint base, default `https://demo.tradelocker.com/backend-api` |
| `TL_STREAMS_URL` | Streams API base, default `https://api.tradelocker.com` |

```bash
cd examples/node && npm install && npm start
```

Every example's README has the commands for its language.

## Try it without credentials

The mock server follows the production contract (handshake, validation rules, result codes, message
order) with made-up account data:

```bash
npm install
npm run mock
```

Then run any example with:

```bash
export TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005
export TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND
```

`scripts/e2e.mjs` runs an example against the mock with the failures from the list above injected
(socket dropped by the server, one account ended server-side, lowercase server name) and checks the
result. `--drop transport` drops the socket the way a proxy does, with a bare WebSocket close. CI
runs both for every example:

```bash
node scripts/e2e.mjs --cwd examples/node -- npm start --silent
```

Point it at your own client the same way before you go live.

## Repository layout

```text
asyncapi/        the contract: streams-api.asyncapi.yaml (source of truth) and its JSON copy
docs/            the published site (GitHub Pages): rendered page, asyncapi.yaml, asyncapi.json
examples/        node, python, java, csharp
mock-server/     local mock of the Streams API
scripts/         build-docs.mjs (spec → docs/), e2e.mjs (example acceptance test)
AGENTS.md        integration rules for AI coding agents and reviewers
```

## Updating the docs

Edit `asyncapi/streams-api.asyncapi.yaml`, then:

```bash
npm run build    # validate, regenerate asyncapi/*.json and docs/
```

Commit the generated files with the change. CI fails when `docs/` does not match the spec
(`npm run check`). GitHub Pages serves the `docs/` folder of `main`.
