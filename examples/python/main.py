"""TradeLocker Streams API example for Python (asyncio, python-socketio, aiohttp).

What it does, and what a production integration must do too:
  1. fetches one account token per account of a user (HTTP),
  2. opens ONE Socket.IO connection with the developer key,
  3. subscribes every account on that connection, and subscribes them again after every reconnect,
  4. routes `stream` messages by brandId + accountId,
  5. on Ctrl+C unsubscribes the accounts and closes the connection.
Lines starting with PITFALL: mark mistakes we have seen in real integrations.

Configuration (environment variables):
  TL_DEVELOPER_API_KEY  your developer API key (required)
  TL_EMAIL, TL_PASSWORD, TL_SERVER   the user's TradeLocker login (required)
  TL_BACKEND_URL   token endpoint base, default https://demo.tradelocker.com/backend-api
                   (live accounts: https://live.tradelocker.com/backend-api)
  TL_STREAMS_URL   Streams API base, default https://api.tradelocker.com
"""

from __future__ import annotations

import asyncio
import base64
import itertools
import json
import os
import random
import signal
import sys
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import aiohttp
import socketio  # type: ignore[import-untyped]  # python-socketio ships no type hints

from messages import StatusMessage, StreamMessage, SubscriptionAction

NAMESPACE = "/streams-api"
SOCKETIO_PATH = "/streams-api/socket.io"

# PITFALL: a SUBSCRIBE includes the trade-node logon and the account snapshot and can take up to
# about 60 s. A shorter acknowledgement timeout makes you retry subscriptions that are succeeding.
# python-socketio's call() waits exactly 60 s unless you pass timeout=, so always pass it.
SUBSCRIBE_ACK_TIMEOUT_S = 70.0
UNSUBSCRIBE_ACK_TIMEOUT_S = 10.0
MAX_SUBSCRIBES_IN_FLIGHT = 10
TOKEN_REFRESH_MARGIN_S = 60.0
RECONNECT_DELAY_S, RECONNECT_DELAY_MAX_S = 1.0, 30.0
RETRY_DELAY_S, RETRY_DELAY_MAX_S = 1.0, 60.0


@dataclass(frozen=True)
class Config:
    # repr=False keeps secrets out of logs and tracebacks that print the object.
    developer_api_key: str = field(repr=False)
    email: str
    password: str = field(repr=False)
    server: str
    backend_url: str
    streams_url: str

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            developer_api_key=required("TL_DEVELOPER_API_KEY"),
            email=required("TL_EMAIL"),
            password=required("TL_PASSWORD"),
            server=required("TL_SERVER"),
            backend_url=os.environ.get("TL_BACKEND_URL", "https://demo.tradelocker.com/backend-api"),
            streams_url=os.environ.get("TL_STREAMS_URL", "https://api.tradelocker.com"),
        )


@dataclass(frozen=True)
class Account:
    key: str  # f"{brand_id}#{account_id}", the same key the server uses
    brand_id: str
    account_id: str
    token: str = field(repr=False)
    expires_at: float  # Unix time in seconds


class TokenRequestError(Exception):
    """The account tokens could not be fetched."""


class StreamsClient:
    def __init__(self, config: Config, http: aiohttp.ClientSession) -> None:
        self.config = config
        self.http = http
        self.accounts: dict[str, Account] = {}
        self.refreshing: asyncio.Task[None] | None = None
        self.generation = 0  # bumped on every accepted connection; work for an older connection stops
        self.accepted = False  # the current connection got `connection ok`
        self.closed = asyncio.Event()  # set when the current connection ends
        self.shutting_down = False
        self.fatal = False  # the developer key was rejected: stop reconnecting
        self.subscribed: set[str] = set()  # accounts acknowledged on the current connection
        self.awaiting_sync_end: set[str] = set()
        self.unsubscribing: set[str] = set()
        self.subscribe_slots = asyncio.Semaphore(MAX_SUBSCRIBES_IN_FLIGHT)
        self.tasks: set[asyncio.Task[None]] = set()

        self.sio = socketio.AsyncClient(
            # Reconnects are handled in run(), for every disconnect reason (see the PITFALL there).
            reconnection=False,
            # PITFALL: python-socketio installs its own SIGINT handler unless told not to. Leave the
            # signals to the application, so that Ctrl+C can unsubscribe before it disconnects.
            handle_sigint=False,
            http_session=http,
        )
        # PITFALL: unlike the JavaScript client, python-socketio ignores the path of the URL: the
        # namespace must be passed to connect() and to every handler. Without it you end up on the
        # default namespace "/" and never receive the `connection` event.
        for event, handler in [
            ("connect", self.on_connect),
            ("connect_error", self.on_connect_error),
            ("disconnect", self.on_disconnect),
            ("connection", self.on_connection),
            ("stream", self.on_stream),
        ]:
            self.sio.on(event, handler, namespace=NAMESPACE)

    # --- Account tokens ------------------------------------------------------------------------

    async def refresh_tokens(self) -> None:
        # One refresh at a time, however many subscriptions ask for it.
        if self.refreshing is None:
            self.refreshing = asyncio.create_task(self._refresh_tokens())
        # shield(): a caller that is cancelled must not cancel the refresh that others wait for.
        await asyncio.shield(self.refreshing)

    async def _refresh_tokens(self) -> None:
        try:
            config = self.config
            url = f"{config.backend_url}/auth/jwt/accounts/tokens"
            credentials = {"email": config.email, "password": config.password, "server": config.server}
            try:
                # PITFALL: this endpoint takes the user's credentials only. Do not send your developer key here.
                timeout = aiohttp.ClientTimeout(total=30)
                async with self.http.post(url, json=credentials, timeout=timeout) as response:
                    if not response.ok:
                        raise TokenRequestError(f"account tokens request failed: HTTP {response.status}")
                    body = await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
                raise TokenRequestError(f"account tokens request failed: {describe(error)}") from error

            accounts: dict[str, Account] = {}
            try:
                for item in body["data"]:
                    # PITFALL: the response has no brandId, and the brand the user typed ("Blueg") is not
                    # necessarily the brand the server uses ("BLUEG"). Take brand and account from the token's
                    # claims: they are exactly what the server matches UNSUBSCRIBE and stream messages against.
                    brand, aid = jwt_claims(item["accessToken"])
                    key = f"{brand}#{aid}"
                    expires_at = parse_timestamp(item["expireDate"])
                    accounts[key] = Account(key, brand, aid, item["accessToken"], expires_at)
            except (KeyError, TypeError, ValueError) as error:
                raise TokenRequestError(f"unexpected account tokens response: {describe(error)}") from error
            self.accounts = accounts
            log(f"tokens for {len(accounts)} account(s): {', '.join(accounts)}")
        finally:
            self.refreshing = None

    # --- Connection ----------------------------------------------------------------------------

    async def run(self) -> None:
        """Keeps one connection open until shutdown, reconnecting with backoff."""
        attempt = 0
        while not self.shutting_down and not self.fatal:
            self.closed = asyncio.Event()
            self.accepted = False
            try:
                # PITFALL: one connection carries all accounts. Do not open a connection per account:
                # connections per developer key are capped, and above the cap the handshake is
                # rejected with `conflict`.
                await self.sio.connect(
                    self.config.streams_url,
                    headers={"developer-api-key": self.config.developer_api_key},
                    # PITFALL: the server accepts the websocket transport only; HTTP long-polling
                    # (python-socketio's default first transport) is rejected.
                    transports=["websocket"],
                    namespaces=[NAMESPACE],
                    socketio_path=SOCKETIO_PATH,
                    wait_timeout=10,  # for the namespace to connect; the default is 1 s
                )
            except socketio.exceptions.ConnectionError:
                pass  # already logged by on_connect_error
            else:
                # Returns about 1 s after the connection ended, whatever the reason.
                await self.sio.wait()

            if self.shutting_down or self.fatal:
                return
            # PITFALL: Socket.IO clients reconnect by themselves after network errors and ping timeouts,
            # but NOT when the server closed the connection (reason "server disconnect"; "io server
            # disconnect" in JavaScript), for example on a deploy. Rejected handshakes end that way too.
            # Without a loop like this one, the process keeps running and every account goes silent.
            # The delay starts over once a connection was accepted, and keeps growing while handshakes
            # are rejected or the server cannot be reached.
            if self.accepted:
                attempt = 0
            delay = backoff(attempt, RECONNECT_DELAY_S, RECONNECT_DELAY_MAX_S)
            attempt += 1
            log(f"reconnecting in {delay:.1f} s")
            await asyncio.sleep(delay)

    def on_connect(self) -> None:
        log(f"socket connected id={self.sio.get_sid(NAMESPACE)}")

    def on_connect_error(self, data: Any) -> None:
        log(f"socket connect error: {data}")

    def on_disconnect(self, reason: str) -> None:
        # PITFALL: subscriptions belong to the connection. After any disconnect, the client reconnects
        # with a NEW connection that has no subscriptions, and nothing arrives until you subscribe again.
        self.subscribed.clear()
        self.awaiting_sync_end.clear()
        self.closed.set()
        log(f"socket disconnected reason={reason}")

    # The server answers every handshake on the `connection` event. Subscribe only after `connected`.
    def on_connection(self, status: StatusMessage) -> None:
        if status["status"] == "ok" and status["code"] == "connected":
            log("connection ok")
            self.accepted = True
            self.generation += 1
            self.spawn(self.subscribe_all(self.generation))
            return
        if status["status"] == "ok" and status["code"] == "disconnected":
            log("server is shutting down this connection; the client will reconnect")
            return
        log(f"connection error code={status['code']} message={status.get('message')}")
        if status["code"].startswith("unauthorized."):
            # PITFALL: a wrong or unknown developer key never starts working by retrying. Stop instead
            # of reconnecting in a loop (that loop is visible to us, and rejected handshakes are throttled).
            self.fatal = True
            self.spawn(self.sio.disconnect())
        # `conflict` (too many connections for the key) and `unknown`: the server closes the socket and
        # run() retries with a growing delay. Look for connections you leaked elsewhere.

    # --- Subscriptions -------------------------------------------------------------------------

    async def subscribe_all(self, current: int) -> None:
        # The semaphore in subscribe() keeps at most MAX_SUBSCRIBES_IN_FLIGHT of these waiting for an ack.
        await asyncio.gather(*(self.subscribe(key, current) for key in list(self.accounts)))

    async def subscribe(self, key: str, current: int) -> None:
        for attempt in itertools.count():
            if attempt:
                await asyncio.sleep(backoff(attempt - 1, RETRY_DELAY_S, RETRY_DELAY_MAX_S))
            if current != self.generation or not self.sio.connected or self.shutting_down:
                return

            account = self.accounts.get(key)
            # PITFALL: a SUBSCRIBE needs a token that has not expired. Tokens are short-lived: fetch new
            # ones before you subscribe again after a reconnect. (Once subscribed, the stream does not
            # depend on the token any more.)
            if account is not None and account.expires_at - time.time() < TOKEN_REFRESH_MARGIN_S:
                try:
                    await self.refresh_tokens()
                except TokenRequestError as error:
                    log(f"{error}; retrying {key}")
                    continue
                account = self.accounts.get(key)
            if account is None:
                return

            async with self.subscribe_slots:
                # PITFALL: SyncEnd for this account can arrive BEFORE the acknowledgement of this
                # SUBSCRIBE. Start waiting for it before you emit, or you will miss it.
                self.awaiting_sync_end.add(key)
                # PITFALL: always ask for the acknowledgement. It is the only place the result is reported.
                # SUBSCRIBE takes exactly {action, token}; any other key fails validation.
                ack = await self.request({"action": "SUBSCRIBE", "token": account.token}, SUBSCRIBE_ACK_TIMEOUT_S)
            log(f"subscribe {key} -> {ack['code']}")

            code = ack["code"]
            if code in ("connected", "alreadyConnectedError"):  # alreadyConnectedError: nothing to do
                self.subscribed.add(key)
                return
            if code in ("clientNotFound", "socketClosed"):
                # The connection is going away (socketClosed is this example's own code, see request()).
                # The next `connection ok` subscribes again.
                return
            if code == "invalidJwt":
                try:
                    await self.refresh_tokens()
                except TokenRequestError as error:
                    log(str(error))
            elif code.startswith("validationError"):
                log(f"bug in this client, not retrying: {ack.get('message')}")
                return
            # tooManyRequests, connectionError, unknown, ackTimeout: retry with backoff

    async def unsubscribe(self, key: str) -> None:
        account = self.accounts.get(key)
        if account is None:
            return
        self.unsubscribing.add(key)
        # PITFALL: brandId and accountId must match the subscription exactly, case included.
        # Use the values from the token claims (or from stream messages), never the user's input.
        ack = await self.request(
            {"action": "UNSUBSCRIBE", "brandId": account.brand_id, "accountId": account.account_id},
            UNSUBSCRIBE_ACK_TIMEOUT_S,
        )
        log(f"unsubscribe {key} -> {ack['code']}")

    async def request(self, action: SubscriptionAction, timeout: float) -> StatusMessage:
        """Emits `subscriptions` and returns the acknowledgement, or an "ackTimeout" or "socketClosed" status."""
        # PITFALL: when the connection drops, python-socketio does not fail a pending call(): it keeps
        # waiting for its full timeout (70 s here), holding one of the in-flight slots, although that
        # acknowledgement can never arrive. Stop waiting when the connection ends.
        # (Concurrent calls are safe here: payloads without binary data are sent as a single packet.)
        closed = asyncio.create_task(self.closed.wait())
        call = asyncio.create_task(self.sio.call("subscriptions", action, namespace=NAMESPACE, timeout=timeout))
        try:
            done, _ = await asyncio.wait({call, closed}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            closed.cancel()
            call.cancel()  # no effect on a finished task
        if call not in done:
            return {"status": "error", "code": "socketClosed", "message": "the connection closed before the ack"}
        try:
            ack = call.result()
        except socketio.exceptions.TimeoutError:
            return {"status": "error", "code": "ackTimeout", "message": "no acknowledgement"}
        except socketio.exceptions.BadNamespaceError:  # not connected (any more)
            return {"status": "error", "code": "socketClosed", "message": "not connected"}
        if not isinstance(ack, dict) or not isinstance(ack.get("code"), str):
            return {"status": "error", "code": "unknown", "message": f"unexpected acknowledgement {ack!r}"}
        return cast(StatusMessage, ack)

    # --- Stream --------------------------------------------------------------------------------

    # PITFALL: keep this handler a plain function (no await) and fast. python-socketio runs every
    # incoming message in its own task, so an await in here lets later messages overtake earlier ones;
    # and the server pings every second and closes the connection after 60 s without an answer, so
    # heavy work here stalls the event loop. Hand the message to a queue instead (queue.put_nowait).
    def on_stream(self, message: StreamMessage) -> None:
        # Every message carries brandId and accountId: one connection, many accounts.
        key = f"{message['brandId']}#{message['accountId']}"
        if message["type"] == "Property":
            if message["name"] == "SyncEnd":
                self.awaiting_sync_end.discard(key)
                log(f"sync-end {key}")
            elif message["name"] == "Disconnected":
                self.subscribed.discard(key)
                if self.shutting_down or key in self.unsubscribing:
                    self.unsubscribing.discard(key)
                    log(f"stream ended {key} (unsubscribed)")
                else:
                    # The server ended this one account (for example its trading session was closed).
                    # Subscribe that account again. PITFALL: do not restart the whole connection for it.
                    log(f"account-disconnected {key}")
                    self.spawn(self.resubscribe(key, self.generation))
        elif message["type"] == "AccountStatus":
            # Decimal, not float: amounts are decimal strings and must add up exactly.
            balance = Decimal(message["balance"])
            equity = Decimal(message["equity"]) if "equity" in message else "-"
            log(f"AccountStatus {key} balance={balance} equity={equity} {message['currency']}")
        elif message["type"] == "Position":
            log(
                f"Position {key} {message['positionId']} {message['side']} {Decimal(message['lots'])} "
                f"{message['instrument']} @ {Decimal(message['openPrice'])}"
            )
        elif message["type"] == "ClosePosition":
            log(f"ClosePosition {key} {message['positionId']} @ {message.get('closePrice', '-')}")
        elif message["type"] == "OpenOrder":
            log(
                f"OpenOrder {key} {message['orderId']} {message.get('status', '-')} {message.get('side', '-')} "
                f"{Decimal(message['amount'])} {message['instrument']}"
            )
        # PITFALL: new message types can be added. Ignore what you do not know.

    async def resubscribe(self, key: str, current: int) -> None:
        await asyncio.sleep(1)
        await self.subscribe(key, current)

    # --- Shutdown ------------------------------------------------------------------------------

    async def shutdown(self, signal_name: str) -> None:
        self.shutting_down = True  # also stops run() from reconnecting
        log(f"{signal_name}: unsubscribing {len(self.subscribed)} account(s)")
        # Closing the connection ends every subscription on it anyway; unsubscribing first is shown
        # here because it is how you stop a single account while keeping the others.
        if self.sio.connected:
            await asyncio.gather(*(self.unsubscribe(key) for key in list(self.subscribed)))
        await self.sio.disconnect()

    async def cancel_tasks(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def spawn(self, coroutine: Coroutine[Any, Any, None]) -> None:
        # asyncio keeps only a weak reference to a running task: hold on to it until it is done.
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log(f"background task failed: {task.exception()!r}")


async def main(config: Config) -> int:
    async with aiohttp.ClientSession() as http:
        client = StreamsClient(config, http)
        try:
            await client.refresh_tokens()
        except TokenRequestError as error:
            log(str(error))
            return 1

        loop = asyncio.get_running_loop()
        stop: asyncio.Future[str] = loop.create_future()  # resolved with the name of the signal

        def request_stop(name: str) -> None:
            if not stop.done():
                stop.set_result(name)

        def on_signal(signum: int, _frame: object) -> None:  # for Windows, which has no add_signal_handler()
            loop.call_soon_threadsafe(request_stop, signal.Signals(signum).name)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, on_signal)

        connection = asyncio.create_task(client.run())  # ends early only if the developer key is rejected
        waiting: list[asyncio.Future[Any]] = [connection, stop]
        await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        if stop.done():
            await client.shutdown(stop.result())
        connection.cancel()
        await asyncio.gather(connection, return_exceptions=True)
        await client.cancel_tasks()
        return 1 if client.fatal else 0


# --- Helpers -----------------------------------------------------------------------------------


def jwt_claims(token: str) -> tuple[str, str]:
    """Reads brand and aid from the JWT payload. No signature check needed: the Streams API verifies the token."""
    try:
        payload_b64 = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)))
    except (IndexError, ValueError) as error:
        raise TokenRequestError("account token is not a JWT") from error
    brand, aid = (payload.get("brand"), payload.get("aid")) if isinstance(payload, dict) else (None, None)
    if not isinstance(brand, str) or not isinstance(aid, str):
        raise TokenRequestError("account token has no brand/aid claims")
    return brand, aid


def parse_timestamp(value: str) -> float:
    # datetime.fromisoformat() accepts the trailing "Z" from Python 3.11 on only.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).timestamp()


def backoff(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with jitter: base * 2^attempt, at most cap, then randomized by +-50 %."""
    return min(cap, base * 2.0 ** min(attempt, 30)) * (0.5 + random.random())


def describe(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Set {name} (see README.md).", file=sys.stderr)
        sys.exit(2)
    return value


def log(line: str) -> None:
    # PITFALL: never log the developer key, the user's password or account tokens.
    # flush=True: when stdout is a pipe or a file, Python buffers it, and lines show up late.
    print(line, flush=True)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(Config.from_env())))
