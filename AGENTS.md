# AGENTS.md

Instructions for AI coding agents (Claude Code, Codex, Cursor, Copilot, ...) that write or review a
client for the **TradeLocker Streams API**. Humans are welcome too: this is the short version of the
contract plus every mistake we have seen integrations make.

- Contract: [`asyncapi/streams-api.asyncapi.yaml`](asyncapi/streams-api.asyncapi.yaml) (AsyncAPI 3.0; JSON copy next to it).
- Rendered docs: https://tradelocker.github.io/streams-api-docs/
- Working examples that pass the checks below: [`examples/`](examples/) (Node.js/TypeScript, Python, Java, C#).

When this file and the AsyncAPI spec disagree, the spec wins. When in doubt, copy the structure of the
example in your language rather than inventing a new one.

## The contract in 12 lines

1. Get one token per account: `POST https://{live|demo}.tradelocker.com/backend-api/auth/jwt/accounts/tokens`, JSON `{"email","password","server"}` → `{"data":[{"accessToken","expireDate","accountId"}]}`. No developer key on this call. The response has **no `brandId`**.
2. Decode each `accessToken` payload (base64url, no signature check needed) and read the `brand` and `aid` claims. These are the account's `brandId` and `accountId`. Key everything by `brandId#accountId`.
3. Open **one** Socket.IO v4 connection: URL `https://api.tradelocker.com/streams-api` (namespace `/streams-api`), option `path: "/streams-api/socket.io"`, `transports: ["websocket"]`, HTTP header `developer-api-key: <key>`.
4. Wait for the server's `connection` event with `{"status":"ok","code":"connected"}`.
5. For each account emit `subscriptions` `{"action":"SUBSCRIBE","token":"<accessToken>"}` **with an acknowledgement callback** (timeout ≥ 65 s). Exactly those two keys.
6. Before emitting, mark the account as awaiting `SyncEnd`: it can arrive **before** the ack.
7. Read the `stream` event: per account a snapshot (`AccountStatus`, `Position`, `OpenOrder`), then `{"type":"Property","name":"SyncEnd"}`, then live updates. Every message has `brandId` and `accountId`.
8. On every new connection (after any disconnect), subscribe **all** accounts again. Subscriptions die with the connection.
9. If the server closed the connection (`io server disconnect` and equivalents), reconnect yourself; Socket.IO clients do not.
10. `{"type":"Property","name":"Disconnected"}` for an account you did not unsubscribe: subscribe that one account again (fresh token if expired). Never restart the whole connection for one account.
11. To stop one account: `{"action":"UNSUBSCRIBE","brandId":"<brand claim>","accountId":"<aid claim>"}`. Exact, case-sensitive match.
12. Numbers are decimal strings, IDs are numeric strings beyond 2^53, absent fields are omitted. Ignore unknown types and fields.

## Acknowledgement codes → what the client does

| `code` | Meaning | Client action |
|---|---|---|
| `connected` | Subscribed. `SyncEnd` was already sent. | Mark subscribed. |
| `alreadyConnectedError` | Already subscribed or connecting on this socket. | Treat as subscribed. |
| `disconnected` | Unsubscribed. | Done. (A `Property Disconnected` for it arrives too; ignore it.) |
| `noSuchConnectionError` | UNSUBSCRIBE matched nothing. | Bug: wrong `brandId` case or `accountId` format. Use the token claims. |
| `invalidJwt` | Token malformed, expired or for another environment. | Fetch new tokens, retry. |
| `tooManyRequests` | Rate limit of the developer key. | Back off, retry. |
| `connectionError`, `unknown`, ack timeout | Account could not be connected right now. | Retry with exponential backoff + jitter, cap ~60 s. |
| `clientNotFound` | The socket is closing. | Nothing; the reconnect subscribes again. |
| `validationError - <issue>` | Malformed payload. | Bug in the client. Do not retry. Fix the payload. |

`connection` event errors: `unauthorized.streams.invalidApiKey` and `unauthorized.streams.developer.notFound`
mean the key is wrong: **stop, do not reconnect in a loop**. `conflict` means the key has too many open
connections: find the leak (usually one connection per account or per worker).

## Rules (each one has caused a production incident or a support ticket)

- **One connection for all accounts.** Connections per developer key are capped; a per-account design hits `conflict`.
- **Brand and account IDs come from the token, never from user input.** The user may type `Blueg`; the server's brand is `BLUEG`. A mismatched UNSUBSCRIBE silently does nothing, and `SyncEnd` / stream messages will not match your keys.
- **Register "awaiting SyncEnd" before emitting SUBSCRIBE.** Waiting only after the ack leaves every account "pending" forever.
- **Resubscribe on every connection**, not just once at startup.
- **Handle the server-initiated disconnect explicitly** (JS: `reason === "io server disconnect"` → `socket.connect()`).
- **Use long ack timeouts.** Subscribe includes a trade-node logon and snapshot: up to ~60 s. python-socketio's `call()` defaults to 60 s; set 70 s.
- **Do not block the Socket.IO event loop/thread.** The server pings every second and drops the connection after 60 s without a pong. Queue heavy work.
- **A watchdog acts per account.** If one account looks stale, unsubscribe and subscribe that account. Restarting the connection drops every account.
- **Bound concurrency** of SUBSCRIBEs (the examples use 10 in flight) and back off on errors.
- **Tokens are short-lived.** Fetch fresh ones before resubscribing if `expireDate` is near. Once subscribed, the stream does not depend on the token.
- **Never log or commit** the developer key, user passwords or tokens. The developer key is a server-side secret: browsers cannot send the header anyway.
- **Parse money with a decimal type** (`decimal.Decimal`, `BigDecimal`, `decimal`, decimal.js). Never float. Never parse IDs as numbers.

## Client library notes

Behaviour of the Socket.IO clients the examples use, found by testing them against the mock. Each
one breaks a naive integration.

**socket.io-client (Node.js, 4.8)**
- No reconnect after `io server disconnect`: call `socket.connect()` yourself, with backoff.
- `emit` while disconnected is buffered and sent on reconnect. Only subscribe after the `connection` event of the current connection.
- Use `socket.timeout(ms).emitWithAck(...)`; without `.timeout()` an ack can wait forever.

**python-socketio (AsyncClient, 5.17)**
- The URL path is ignored: pass `namespaces=["/streams-api"]` to `connect()` and `namespace=` to every handler and call, or the client silently joins `/` and never sees `connection`.
- `socketio_path="/streams-api/socket.io"` and `transports=["websocket"]` (the default starts with polling).
- No reconnect after a server-side close (`reason == "server disconnect"`), even with `reconnection=True`.
- `call()` defaults to a 60 s timeout, and a pending `call()` does **not** fail when the connection drops; it waits out its timeout. Race it against a "connection closed" event.
- Every incoming event runs in its own task: do not `await` in handlers, or later messages can overtake earlier ones.
- It installs its own SIGINT handler (`handle_sigint=True`); turn it off if you handle signals.

**io.socket socket.io-client (Java, 2.1)**
- No reconnect after `io server disconnect`: call `socket.connect()` yourself, with backoff.
- A WebSocket close frame without a Socket.IO disconnect packet is noticed only at the ping timeout (about 61 s): engine.io-client leaves OkHttp's `onClosing()` empty. Pass `options.webSocketFactory` with a listener whose `onClosing()` calls `webSocket.close(1000, null)`.
- All listeners, ack callbacks and ping replies run on one static EventThread: never block it.
- `AckWithTimeout` starts a timer thread per ack and calls `onTimeout()` twice when the connection drops: call `cancelTimer()` in `onTimeout()` and make completion idempotent.
- Emits made while disconnected are buffered and replayed on the next connect. `extraHeaders` does reach the WebSocket handshake.
- On JDK 25, import `io.socket.client.IO` by name: a wildcard import clashes with `java.lang.IO`.
- The JVM exits with 130/143 after SIGINT/SIGTERM even when shutdown hooks succeed; supervisors may read that as a crash.

**SocketIOClient (C#, 4.0.5)**
- Default transport is polling: set `Transport = TransportProtocol.WebSocket`. `ExtraHeaders` works on the WebSocket handshake.
- No reconnect after a server-side close, and the built-in reconnect gives up silently after `ReconnectionAttempts` (default 10).
- A WebSocket close frame without a preceding Socket.IO disconnect packet is not noticed at all (`Connected` stays true, no event), and there is no client ping timeout. Add a watchdog: no server ping for 61 s (1 s interval + 60 s) means the connection is dead.
- No ack timeout, and pending acks are dropped silently on disconnect: wrap them in a `TaskCompletionSource` with a timeout and cancel on disconnect.
- Handlers run on the receive loop: an `await` for an ack inside a handler deadlocks until the handler returns. Exceptions in handlers are swallowed; an exception in an ack callback tears down the connection.
- `DisconnectAsync` can wait forever for the server's close frame: pass a timeout.
- `dotnet run` does not forward a SIGINT sent to its own process; run the built DLL under a supervisor.

## Verify your client before you ship it

This repo has a local mock server that follows the production contract (same validation, codes and
message order) and an end-to-end check that injects the failures above.

```bash
npm install                      # once, at the repo root
npm run mock                     # mock on http://localhost:3005, developer key tl-mock-developer-key
```

Point any client at it with:

```bash
TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005 \
TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND
```

Run the automated check (it starts its own mock on a free port, drops the socket, ends one account
server-side, sends SIGINT, and asserts on what the server saw):

```bash
node scripts/e2e.mjs --cwd <your-client-dir> -- <command that starts your client>
```

Add `--drop transport` to drop the socket with a bare WebSocket close (what proxies and load
balancers send) instead of a Socket.IO disconnect; several client libraries miss that one.

Your client must print `sync-end <brandId>#<accountId>` when it receives `SyncEnd`, and exit 0 on
SIGINT after unsubscribing. Passing it is necessary, not sufficient: also test against the real API
with a development key.

## Working on this repo

- The spec is the source of truth. After editing `asyncapi/streams-api.asyncapi.yaml` run `npm run build`
  (validates, writes `asyncapi/streams-api.asyncapi.json` and the rendered site in `docs/`) and commit the output.
  CI runs `npm run check` and fails if they drift.
- Describe the server as it behaves in production. Facts in the spec come from the `streams-api` service code, not from memory.
- Every example must keep passing `scripts/e2e.mjs` in both drop modes; CI runs all four.
- Pin dependency versions; prefer releases at least two weeks old.
- No secrets anywhere, including examples and test fixtures (the mock uses made-up credentials).
