# TradeLocker Streams API: Python example

One Socket.IO connection, many accounts, reconnect-safe. Built on asyncio with
[python-socketio](https://python-socketio.readthedocs.io/) (`AsyncClient`) and
[aiohttp](https://docs.aiohttp.org/).

What it does, and what a production integration must do too:

1. fetches one account token per account of the user (HTTP),
2. opens **one** Socket.IO connection with your developer key,
3. subscribes every account on that connection, and subscribes them again after every reconnect,
4. routes `stream` messages by `brandId` + `accountId`,
5. on Ctrl+C (SIGINT) or SIGTERM unsubscribes the accounts, closes the connection and exits with status 0.

Comments starting with `PITFALL:` in [`main.py`](main.py) mark mistakes we have seen in real
integrations. [`messages.py`](messages.py) has the payload types.

## Prerequisites

- Python 3.10 or newer
- [uv](https://docs.astral.sh/uv/) (recommended), or pip

## Install

```bash
cd examples/python
uv sync
```

`uv sync` creates `.venv/` with the exact versions from `uv.lock`. `uv run` does the same on
first use, so you can skip this step.

Without uv:

```bash
python3 -m venv .venv
.venv/bin/pip install "python-socketio[asyncio-client]==5.17.0" "python-engineio==4.14.0" "aiohttp==3.14.3"
```

(Then run `.venv/bin/python main.py` wherever the commands below say `uv run main.py`.)

## Configuration

| Variable | |
|---|---|
| `TL_DEVELOPER_API_KEY` | your developer API key (required) |
| `TL_EMAIL`, `TL_PASSWORD`, `TL_SERVER` | the user's TradeLocker login (required) |
| `TL_BACKEND_URL` | token endpoint base, default `https://demo.tradelocker.com/backend-api` (live accounts: `https://live.tradelocker.com/backend-api`) |
| `TL_STREAMS_URL` | Streams API base, default `https://api.tradelocker.com` |

A missing required variable exits with status 2. A rejected developer key
(`unauthorized.streams.*`), or a token request that fails at startup, exits with status 1 without
retrying. (Token requests that fail later, before a resubscribe, are retried with backoff.)

## Run against the real API

```bash
export TL_DEVELOPER_API_KEY=...            # your developer key
export TL_EMAIL=... TL_PASSWORD=... TL_SERVER=...
uv run main.py
```

Demo accounts work with the defaults. For live accounts also set
`TL_BACKEND_URL=https://live.tradelocker.com/backend-api`.

## Run against the local mock

In the repository root:

```bash
npm install
npm run mock
```

In another terminal:

```bash
cd examples/python
TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005 \
TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND \
uv run main.py
```

Expected output (abridged):

```text
tokens for 2 account(s): MOCKBRAND#D#1001, MOCKBRAND#L#2002
socket connected id=...
connection ok
AccountStatus MOCKBRAND#D#1001 balance=9888.95 equity=9956.62 USD
Position MOCKBRAND#D#1001 144115188076017329 BUY 0.01 ETHUSD @ 3299.71
OpenOrder MOCKBRAND#D#1001 144115188077153593 STATUS_ACCEPTED BUY 10 AUDUSD
sync-end MOCKBRAND#D#1001
subscribe MOCKBRAND#D#1001 -> connected
...
^C
SIGINT: unsubscribing 2 account(s)
stream ended MOCKBRAND#D#1001 (unsubscribed)
unsubscribe MOCKBRAND#D#1001 -> disconnected
...
socket disconnected reason=client disconnect
```

### Acceptance test

The repository's end-to-end check runs this example against the mock with faults injected (the
server ends one account's stream, then closes the socket; `TL_SERVER` is passed in lowercase).
From the repository root:

```bash
node scripts/e2e.mjs --cwd examples/python -- uv run --locked main.py
```

It prints `PASS` when every account was subscribed again after each failure and unsubscribed on
SIGINT.

## Pitfalls this example handles

Streams API rules (the same in every language):

- **One connection for all accounts.** Connections per developer key are capped (`conflict`).
- **Subscribe only after `connection` `{status: ok, code: connected}`**, and subscribe every account
  again after every reconnect: subscriptions belong to the connection.
- **Reconnect after a server-side close.** Socket.IO does not do it for you (see below).
- **`SyncEnd` arrives before the SUBSCRIBE acknowledgement.** The example starts waiting for it before
  it emits.
- **Brand and account come from the token's `brand` and `aid` claims**, never from `TL_SERVER`:
  UNSUBSCRIBE and stream messages are matched exactly, case included.
- **SUBSCRIBE can take about 60 s.** The acknowledgement timeout is 70 s. At most 10 SUBSCRIBEs are in
  flight; failures are retried with exponential backoff and jitter, `invalidJwt` after fetching new
  tokens, `validationError` not at all.
- **`Property` `Disconnected`** from the server ends one account: only that account is subscribed
  again. After your own UNSUBSCRIBE it is expected and ignored.
- **A rejected developer key stops the client** instead of reconnecting in a loop.
- **Amounts are decimal strings** (parsed with `decimal.Decimal`, never `float`), IDs stay strings,
  optional fields are omitted rather than `null`, unknown message types are ignored.
- **The developer key goes to the Socket.IO handshake only**, never to the token endpoint, and
  neither the key, the password nor tokens are logged.

python-socketio specifics:

- **The namespace is not taken from the URL** (unlike the JavaScript client). It is passed with
  `namespaces=["/streams-api"]` and to every handler; otherwise you land on `/` and never get the
  `connection` event.
- **No reconnect after a server-side close.** The disconnect reason is `"server disconnect"`, and
  rejected handshakes (`conflict`, `unknown`) end the same way. The example turns the library's
  reconnection off and runs one reconnect loop for every reason, with a delay that grows while
  handshakes are rejected.
- **`call()` waits 60 s by default**, less than a SUBSCRIBE can take: the timeout is passed explicitly.
- **A pending `call()` does not fail when the connection drops**; it waits for its full timeout. The
  example stops waiting as soon as the connection ends, so the reconnect is not held up.
- **Every incoming message is handled in its own task.** The `stream` handler is a plain function, so
  messages are processed in the order they arrived.
- **python-socketio installs a SIGINT handler** unless `handle_sigint=False`; the example handles
  SIGINT and SIGTERM itself, so it can unsubscribe before it disconnects.
- **Only websocket.** python-socketio starts with HTTP long-polling by default, which the server
  rejects: `transports=["websocket"]`.

## Files

| File | |
|---|---|
| `main.py` | the example |
| `messages.py` | `TypedDict`s of the payloads (the contract is `asyncapi/streams-api.asyncapi.yaml`) |
| `pyproject.toml`, `uv.lock` | dependencies, pinned (resolved with `exclude-newer` so only releases that have been public for a while are used) |
