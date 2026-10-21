# TradeLocker Streams API: Java example

One Socket.IO connection for all accounts of a user. It subscribes every account, subscribes them
again after every reconnect, routes `stream` messages by `brandId` + `accountId`, and on Ctrl+C
unsubscribes and exits cleanly. Java 21 and the `io.socket:socket.io-client` 2.1 client.

The contract is [`asyncapi/streams-api.asyncapi.yaml`](../../asyncapi/streams-api.asyncapi.yaml).
Comments starting with `PITFALL:` in the code mark mistakes we have seen in real integrations;
`QUIRK:` marks behaviour of the Java client library that the example works around.

| File | What is in it |
|---|---|
| `src/main/java/com/tradelocker/examples/streams/StreamsExample.java` | tokens, connection, subscriptions, stream routing, shutdown |
| `src/main/java/com/tradelocker/examples/streams/StreamMessage.java` | typed `stream` messages (`BigDecimal` amounts, `String` IDs) |
| `src/main/java/com/tradelocker/examples/streams/StatusMessage.java` | `connection` event and acknowledgement payload |

## Prerequisites

- JDK 21 or newer (`java -version`)
- Maven 3.6.3 or newer
- A developer API key, and the TradeLocker login (email, password, server) of the user whose
  accounts you stream

## Build

```sh
mvn -q package
```

This writes `target/streams-api-example.jar`, a runnable jar with the dependencies inside.

## Configuration

| Variable | Required | Default | |
|---|---|---|---|
| `TL_DEVELOPER_API_KEY` | yes | | your developer API key |
| `TL_EMAIL` | yes | | the user's TradeLocker email |
| `TL_PASSWORD` | yes | | the user's TradeLocker password |
| `TL_SERVER` | yes | | the server (brand) the user logs in to |
| `TL_BACKEND_URL` | no | `https://demo.tradelocker.com/backend-api` | token endpoint; live accounts: `https://live.tradelocker.com/backend-api` |
| `TL_STREAMS_URL` | no | `https://api.tradelocker.com` | Streams API |

## Run against the Streams API

```sh
export TL_DEVELOPER_API_KEY='<your developer API key>'
export TL_EMAIL='trader@example.com'
export TL_PASSWORD='<password>'
export TL_SERVER='<server>'
# export TL_BACKEND_URL=https://live.tradelocker.com/backend-api   # for live accounts
java -jar target/streams-api-example.jar
```

Output looks like this (one line per event; the example never prints the key, the password or tokens):

```text
tokens for 2 account(s): BAPIA#D#1001, BAPIA#L#2002
socket connected id=QthkXwSWZFkKAZ5eAAAA
connection ok
AccountStatus BAPIA#D#1001 balance=9888.95 equity=9922.80 USD
Position BAPIA#D#1001 144115188076017329 BUY 0.01 ETHUSD @ 3299.71
OpenOrder BAPIA#D#1001 144115188077153593 STATUS_ACCEPTED BUY 10 AUDUSD
sync-end BAPIA#D#1001
subscribe BAPIA#D#1001 -> connected
...
```

Stop it with Ctrl+C (or SIGTERM): it unsubscribes each account, closes the connection and exits
with status 0. Exit status 1 means the developer key was rejected or the account tokens could not
be fetched (wrong login, for example), 2 that a required variable is missing.

## Run against the local mock

The [mock server](../../mock-server/server.mjs) follows the same contract with made-up data. In the
repository root:

```sh
npm install   # once
npm run mock
```

In a second terminal, in `examples/java`:

```sh
TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005 \
TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com \
TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND \
java -jar target/streams-api-example.jar
```

## End-to-end check

From the repository root, after `mvn -q -f examples/java/pom.xml package`:

```sh
node scripts/e2e.mjs --cwd examples/java -- java -jar target/streams-api-example.jar
```

It starts the mock, ends one account's stream server-side, closes the connection from the server,
then sends SIGINT. It prints `PASS` when every account was subscribed again, nothing failed
validation, each account was unsubscribed and the example exited with status 0.

## Pitfalls this example handles

- **One connection for all accounts.** Connections per developer key are capped (`conflict`).
- **Subscribe again after every reconnect**, on each `connection` `connected`. Subscriptions die
  with the connection.
- **Reconnect after a server-side close.** The client reconnects by itself after network errors,
  but not after `io server disconnect` (deploys, rejected handshakes): the example does it, with a
  growing delay.
- **Stop on `unauthorized.*`** instead of reconnecting in a loop.
- **Ask for the acknowledgement**, with a 70 s timeout: a SUBSCRIBE can take about 60 s.
- **Wait for `SyncEnd` before you emit** the SUBSCRIBE: it can arrive before the acknowledgement.
- **Retry by result code**: `invalidJwt` fetches new tokens, `validationError - ...` is a client
  bug and is not retried, `tooManyRequests` / `connectionError` / `unknown` / timeouts back off
  exponentially with jitter (at most 60 s). At most 10 SUBSCRIBEs wait for an acknowledgement.
- **Brand and account from the token's `brand` and `aid` claims**, never from `TL_SERVER`
  (`Blueg` is not `BLUEG`). UNSUBSCRIBE must match exactly.
- **`Property` `Disconnected`**: after your own UNSUBSCRIBE it is expected; otherwise only that
  account is subscribed again, the connection stays.
- **Tokens are short-lived**: refreshed (one request at a time) before a SUBSCRIBE when they expire
  within 60 s. The developer key is not sent to the token endpoint.
- **Amounts are decimal strings, IDs are numeric strings above 2^53**: `BigDecimal` and `String`,
  never `double`. Omitted fields are `null`; unknown message types and fields are ignored.
- **Never log secrets.** `Config` and `Account` override the `toString()` that records generate,
  which would print the key, the password and tokens.

## Notes on the Java client (io.socket 2.1)

- **One event thread.** Every listener and acknowledgement callback runs on the client's single
  `EventThread`, which also answers the server's pings (the server closes the connection after
  60 s without a pong). The example never blocks it: each event goes to the example's own thread,
  and retries are scheduled with `CompletableFuture.delayedExecutor()`, not `Thread.sleep()`.
- **No reconnect after `io server disconnect`**, as in the JavaScript client. The example calls
  `socket.connect()` itself after a backoff.
- **A close frame from the server goes unanswered.** engine.io-client 2.1 leaves OkHttp's
  `onClosing()` empty, so when the server or a proxy closes the websocket with a close frame, the
  client notices only at its ping timeout (about 61 s). `answeringCloseFrames()` answers the frame
  so the client reconnects at once.
- **`AckWithTimeout` starts a timer thread per acknowledgement**, and when the connection closes it
  calls `onTimeout()` at once and again when the timer fires. The example cancels the timer in
  `onTimeout()`.
- **Emits made while disconnected are queued** and sent after the next connect. The example checks
  the connection right before each SUBSCRIBE.
- **org.json**: the client declares org.json 20090211; `pom.xml` pins a current release, which is
  API compatible.
- **JDK 25**: `import io.socket.client.*` makes `IO` ambiguous with the new `java.lang.IO`. Import
  `io.socket.client.IO` by name.
- **Exit status after a signal**: the JVM exits with 130 (SIGINT) or 143 (SIGTERM) even after
  clean shutdown hooks. The example's hook ends with `Runtime.halt(0)`.
- **The token request uses HTTP/1.1.** Over plain http, Java's `HttpClient` otherwise sends
  `Upgrade: h2c`, and a Node.js server with Socket.IO attached (the mock) drops such requests.
