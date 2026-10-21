# TradeLocker Streams API: C# (.NET) example

A console app that streams account updates for all accounts of one TradeLocker user. It does
what a production integration has to do:

1. fetch one account token per account (`POST /auth/jwt/accounts/tokens`),
2. open **one** Socket.IO connection with the developer key,
3. subscribe every account on it, and subscribe them all again after every reconnect,
4. route `stream` messages by `brandId#accountId`,
5. on Ctrl+C (SIGINT) or SIGTERM, unsubscribe the accounts, disconnect and exit 0.

`Program.cs` is the whole client, and `Messages.cs` holds the payload types. Comments that start
with `PITFALL:` mark mistakes we have seen in real integrations. The contract is
[`asyncapi/streams-api.asyncapi.yaml`](../../asyncapi/streams-api.asyncapi.yaml).

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0), **or** Docker (`run.sh` builds
  and runs the example in `mcr.microsoft.com/dotnet/sdk:10.0`).
- A developer API key and a TradeLocker login, or the local mock server in this repository.

Dependency: [`SocketIOClient`](https://www.nuget.org/packages/SocketIOClient) 4.0.5, pinned in
`StreamsExample.csproj`. Everything else (`HttpClient`, `System.Text.Json`) is part of .NET.

## Configuration

| Variable | Required | Default |
|---|---|---|
| `TL_DEVELOPER_API_KEY` | yes | |
| `TL_EMAIL`, `TL_PASSWORD` | yes | |
| `TL_SERVER` | yes | (the brand the user logs in to) |
| `TL_BACKEND_URL` | no | `https://demo.tradelocker.com/backend-api` (live accounts: `https://live.tradelocker.com/backend-api`) |
| `TL_STREAMS_URL` | no | `https://api.tradelocker.com` |

If a required variable is missing, the example says so and exits with status 2. If the developer
key or the login is rejected, it exits with status 1. Keep these values out of source control.

## Run with the .NET SDK

```sh
cd examples/csharp
export TL_DEVELOPER_API_KEY=... TL_EMAIL=... TL_PASSWORD=... TL_SERVER=...
dotnet run
```

Press Ctrl+C to stop. In PowerShell, set the variables with `$env:TL_EMAIL = "..."` first.

## Run in Docker (no local SDK)

```sh
cd examples/csharp
export TL_DEVELOPER_API_KEY=... TL_EMAIL=... TL_PASSWORD=... TL_SERVER=...
./run.sh
```

`run.sh` mounts this directory read-only and keeps the NuGet cache and build output in the Docker
volumes `tl-streams-example-nuget` and `tl-streams-example-build`. It passes the `TL_*` variables
by name, so their values never appear on a command line. It rewrites `localhost` / `127.0.0.1` in
the URLs to `host.docker.internal`, so the container can reach a mock server on your machine. It
also forwards Ctrl+C / SIGINT / SIGTERM to the .NET process (`docker run --init`, then `exec dotnet`).
To use another image, set `DOTNET_SDK_IMAGE`. Remove the volumes with
`docker volume rm tl-streams-example-nuget tl-streams-example-build`.

## Run against the local mock

In the repository root, run `npm install` (once), then `npm run mock` (listens on port 3005).
In a second terminal:

```sh
cd examples/csharp
export TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005 \
  TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com \
  TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND
dotnet run        # or ./run.sh
```

```text
tokens for 2 account(s): MOCKBRAND#D#1001, MOCKBRAND#L#2002
socket connected id=4AGmZVZO90OXiTLnAAAA
connection ok
AccountStatus MOCKBRAND#D#1001 balance=9888.95 equity=9948.72 USD
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
socket disconnected reason=io client disconnect
```

## Acceptance test

`scripts/e2e.mjs` starts the mock with injected faults (the server ends one account's stream,
then closes the socket), runs the example, sends SIGINT and checks what the server saw. From the
repository root:

```sh
node scripts/e2e.mjs --cwd examples/csharp --timeout 120 -- ./run.sh
```

With a local SDK, build first and run the DLL:

```sh
dotnet build examples/csharp -c Release
node scripts/e2e.mjs --cwd examples/csharp -- dotnet bin/Release/net10.0/StreamsExample.dll
```

Do not test with `dotnet run` here: it does not pass on a SIGINT sent to its own process ID
(Ctrl+C in a terminal works, because the terminal signals the app directly).

## Pitfalls this example handles

- **One connection for all accounts.** Connections per developer key are capped, and above the
  cap the handshake is rejected with `conflict`.
- **WebSocket only, with the key in the `developer-api-key` header.** SocketIOClient starts with
  HTTP polling unless you set `Transport = TransportProtocol.WebSocket`.
- **Subscribe only after `connection` `{status: ok, code: connected}`, and again after every
  reconnect.** Subscriptions end with the socket.
- **Reconnect after "io server disconnect" too.** Socket.IO clients do not do this by themselves.
  The example turns SocketIOClient's own reconnect off and runs a single reconnect loop with
  exponential backoff and jitter, for every kind of disconnect.
- **`unauthorized.*` stops the client** (exit 1) instead of reconnecting in a loop. On `conflict` or
  `unknown`, the client backs off and tries again.
- **SUBSCRIBE is exactly `{action, token}`, and UNSUBSCRIBE is exactly `{action, brandId,
  accountId}`.** Extra keys fail with `validationError - unrecognized_keys`, which is never retried.
- **brandId and accountId come from the token's `brand` and `aid` claims**, never from user
  input (`TL_SERVER=mockbrand` must still unsubscribe `MOCKBRAND#...`).
- **Wait up to 70 s for a SUBSCRIBE acknowledgement.** A subscribe can take about 60 s.
- **At most 10 SUBSCRIBEs in flight.** Retries use exponential backoff and jitter, capped at 60 s.
  `invalidJwt` fetches new tokens first, and only one token refresh runs at a time.
- **Start waiting for `SyncEnd` before you emit the SUBSCRIBE**, because it arrives before the
  acknowledgement.
- **`Property` `Disconnected` outside your own UNSUBSCRIBE** ends one account. Subscribe that one
  account again after about 1 s; do not restart the connection.
- **Decimals are strings.** They are read into `decimal`, never `double`, and printed with
  `CultureInfo.InvariantCulture`. IDs stay strings. Unknown message types and fields are ignored.
- **Secrets stay out of logs.** Records print every property in `ToString()`, so the records that
  hold the key, password or tokens override it.

## SocketIOClient 4.0.5: what to know

Checked against the library source (commit `9deebcfe`) and with fault-injecting test servers:

- **No acknowledgement timeout, and pending acknowledgements are dropped on disconnect.**
  `EmitWithAckAsync` adds both, with `TaskCompletionSource.Task.WaitAsync(timeout, token)`.
- **Handlers run on the receive loop.** If a handler awaits an acknowledgement, the acknowledgement
  is not read until the handler returns, so the await hangs until it times out. A slow handler also
  delays the server's pings. The example starts that work with `Task.Run`.
- **An exception in an event handler is swallowed** (it goes to the library's `ILogger`, which prints
  nothing by default). An exception in an **ack callback** closes the connection. Both are caught.
- **Built-in reconnect:** it gives up after `ReconnectionAttempts` (default 10) and never runs
  after "io server disconnect". The example turns it off (`Reconnection = false`) and reconnects
  itself, on the same `SocketIO` instance.
- **A WebSocket close frame without a Socket.IO disconnect packet goes unnoticed**, and the library
  does not watch the server's pings. Such a connection looks open, but nothing arrives. The example
  closes the connection when no ping has arrived for 61 s (the server pings every second). It also
  reconnects as soon as the server announces a shutdown (`connection` `{ok, disconnected}`).
- **`DisconnectAsync` waits for the server's close frame without a time limit**, so the example
  gives it 3 s.
- The namespace comes from the URL path (`.../streams-api`). The handshake path is the separate
  `Path` option (`/streams-api/socket.io`). Headers go in `ExtraHeaders`.
