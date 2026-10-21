// TradeLocker Streams API example for Node.js (TypeScript, runs on Node >= 22.6 without a build step).
//
// What it does, and what a production integration must do too:
//   1. fetches one account token per account of a user (HTTP),
//   2. opens ONE Socket.IO connection with the developer key,
//   3. subscribes every account on that connection, and subscribes them again after every reconnect,
//   4. routes `stream` messages by brandId + accountId,
//   5. on Ctrl+C unsubscribes the accounts and closes the connection.
// Lines starting with PITFALL: mark mistakes we have seen in real integrations.
//
// Configuration (environment variables):
//   TL_DEVELOPER_API_KEY  your developer API key (required)
//   TL_EMAIL, TL_PASSWORD, TL_SERVER   the user's TradeLocker login (required)
//   TL_BACKEND_URL   token endpoint base, default https://demo.tradelocker.com/backend-api
//                    (live accounts: https://live.tradelocker.com/backend-api)
//   TL_STREAMS_URL   Streams API base, default https://api.tradelocker.com
import { setTimeout as sleep } from "node:timers/promises";
import { io } from "socket.io-client";
import type { StatusMessage, StreamMessage } from "./messages.ts";

const config = {
  developerApiKey: required("TL_DEVELOPER_API_KEY"),
  email: required("TL_EMAIL"),
  password: required("TL_PASSWORD"),
  server: required("TL_SERVER"),
  backendUrl: process.env.TL_BACKEND_URL ?? "https://demo.tradelocker.com/backend-api",
  streamsUrl: process.env.TL_STREAMS_URL ?? "https://api.tradelocker.com",
};

// PITFALL: a SUBSCRIBE includes the trade-node logon and the account snapshot and can take up to
// about 60 s. A shorter acknowledgement timeout makes you retry subscriptions that are succeeding.
const SUBSCRIBE_ACK_TIMEOUT_MS = 70_000;
const UNSUBSCRIBE_ACK_TIMEOUT_MS = 10_000;
const MAX_SUBSCRIBES_IN_FLIGHT = 10;
const TOKEN_REFRESH_MARGIN_MS = 60_000;

interface Account {
  key: string; // `${brandId}#${accountId}`, the same key the server uses
  brandId: string;
  accountId: string;
  token: string;
  expiresAt: number;
}

let accounts = new Map<string, Account>();
let refreshing: Promise<void> | undefined;
let generation = 0; // bumped on every accepted connection; work for an older connection stops
let shuttingDown = false;
let fatal = false; // the developer key was rejected: stop reconnecting
let reconnectAttempt = 0; // for our own reconnects after a server-side close; reset on `connected`
const subscribed = new Set<string>(); // accounts acknowledged on the current connection
const awaitingSyncEnd = new Set<string>();
const unsubscribing = new Set<string>();

// --- Account tokens ------------------------------------------------------------------------

async function refreshTokens(): Promise<void> {
  // One refresh at a time, however many subscriptions ask for it.
  refreshing ??= (async () => {
    // PITFALL: this endpoint takes the user's credentials only. Do not send your developer key here.
    const response = await fetch(`${config.backendUrl}/auth/jwt/accounts/tokens`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ email: config.email, password: config.password, server: config.server }),
    });
    if (!response.ok) throw new Error(`account tokens request failed: HTTP ${response.status}`);
    const body = (await response.json()) as { data: { accessToken: string; expireDate: string; accountId: string }[] };

    const next = new Map<string, Account>();
    for (const { accessToken, expireDate } of body.data) {
      // PITFALL: the response has no brandId, and the brand the user typed ("Blueg") is not
      // necessarily the brand the server uses ("BLUEG"). Take brand and account from the token's
      // claims: they are exactly what the server matches UNSUBSCRIBE and stream messages against.
      const { brand, aid } = jwtClaims(accessToken);
      const key = `${brand}#${aid}`;
      next.set(key, { key, brandId: brand, accountId: aid, token: accessToken, expiresAt: Date.parse(expireDate) });
    }
    accounts = next;
    log(`tokens for ${next.size} account(s): ${[...next.keys()].join(", ")}`);
  })().finally(() => (refreshing = undefined));
  return refreshing;
}

/** Reads the JWT payload. No signature check needed: the Streams API verifies the token. */
function jwtClaims(token: string): { brand: string; aid: string } {
  const payload = JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString("utf8"));
  if (typeof payload.brand !== "string" || typeof payload.aid !== "string") throw new Error("token has no brand/aid claims");
  return { brand: payload.brand, aid: payload.aid };
}

// --- Connection ----------------------------------------------------------------------------

await refreshTokens();

// PITFALL: one connection carries all accounts. Do not open a connection per account: connections
// per developer key are capped, and above the cap the handshake is rejected with `conflict`.
const socket = io(`${config.streamsUrl}/streams-api`, {
  path: "/streams-api/socket.io",
  // PITFALL: the server accepts the websocket transport only; HTTP long-polling is rejected.
  transports: ["websocket"],
  extraHeaders: { "developer-api-key": config.developerApiKey },
  reconnectionDelay: 1_000,
  reconnectionDelayMax: 30_000,
  randomizationFactor: 0.5,
});

socket.on("connect", () => log(`socket connected id=${socket.id}`));
socket.on("connect_error", (error) => log(`socket connect error: ${error.message}`));

// PITFALL: subscriptions belong to the connection. After any disconnect, the client reconnects
// with a NEW connection that has no subscriptions, and nothing arrives until you subscribe again.
socket.on("disconnect", (reason) => {
  subscribed.clear();
  awaitingSyncEnd.clear();
  log(`socket disconnected reason=${reason}`);
  // PITFALL: Socket.IO reconnects by itself after network errors and ping timeouts, but NOT when
  // the server closed the connection ("io server disconnect"), for example on a deploy. Without
  // this, the process keeps running and every account goes silent.
  if (reason === "io server disconnect" && !shuttingDown && !fatal) {
    // Back off like the built-in reconnect does: a server that keeps rejecting the handshake
    // (`conflict`) must not be hit in a tight loop.
    void sleep(backoff(reconnectAttempt++, 30_000)).then(() => {
      if (!shuttingDown && !fatal) socket.connect();
    });
  }
});

// The server answers every handshake on the `connection` event. Subscribe only after `connected`.
socket.on("connection", (status: StatusMessage) => {
  if (status.status === "ok" && status.code === "connected") {
    log("connection ok");
    reconnectAttempt = 0;
    const current = ++generation;
    void subscribeAll(current);
    return;
  }
  if (status.status === "ok" && status.code === "disconnected") {
    log("server is shutting down this connection; the client will reconnect");
    return;
  }
  log(`connection error code=${status.code} message=${status.message}`);
  if (status.code.startsWith("unauthorized.")) {
    // PITFALL: a wrong or unknown developer key never starts working by retrying. Stop instead of
    // reconnecting in a loop (that loop is visible to us, and rejected handshakes are throttled).
    fatal = true;
    socket.io.reconnection(false);
    socket.disconnect();
    process.exitCode = 1;
  }
  // `conflict` (too many connections for the key) and `unknown`: the server closes the socket and
  // the client retries with backoff. Look for connections you leaked elsewhere.
});

// --- Subscriptions -------------------------------------------------------------------------

async function subscribeAll(current: number): Promise<void> {
  const queue = [...accounts.keys()];
  const worker = async () => {
    for (let key = queue.shift(); key !== undefined; key = queue.shift()) await subscribe(key, current);
  };
  await Promise.all(Array.from({ length: Math.min(MAX_SUBSCRIBES_IN_FLIGHT, queue.length) }, worker));
}

async function subscribe(key: string, current: number, attempt = 0): Promise<void> {
  if (current !== generation || !socket.connected || shuttingDown) return;

  let account = accounts.get(key);
  if (!account) return;
  // PITFALL: a SUBSCRIBE needs a token that has not expired. Tokens are short-lived: fetch new ones
  // before you subscribe again after a reconnect. (Once subscribed, the stream does not depend on
  // the token any more.)
  if (account.expiresAt - Date.now() < TOKEN_REFRESH_MARGIN_MS) {
    if (!(await tryRefreshTokens())) return retryLater(key, current, attempt);
    account = accounts.get(key);
    if (!account) return;
  }

  // PITFALL: SyncEnd for this account can arrive BEFORE the acknowledgement of this SUBSCRIBE.
  // Start waiting for it before you emit, or you will miss it.
  awaitingSyncEnd.add(key);

  let ack: StatusMessage;
  try {
    // PITFALL: always ask for the acknowledgement. It is the only place the result is reported.
    // SUBSCRIBE takes exactly { action, token }; any other key fails validation.
    ack = await socket.timeout(SUBSCRIBE_ACK_TIMEOUT_MS).emitWithAck("subscriptions", { action: "SUBSCRIBE", token: account.token });
  } catch {
    ack = { status: "error", code: "ackTimeout", message: "no acknowledgement" };
  }
  log(`subscribe ${key} -> ${ack.code}`);

  switch (ack.code) {
    case "connected":
    case "alreadyConnectedError": // already subscribed on this connection: nothing to do
      subscribed.add(key);
      return;
    case "clientNotFound": // the connection is going away; the reconnect subscribes again
      return;
    case "invalidJwt":
      await tryRefreshTokens();
      break;
    default:
      if (ack.code.startsWith("validationError")) {
        log(`bug in this client, not retrying: ${ack.message}`);
        return;
      }
    // tooManyRequests, connectionError, unknown, ackTimeout: retry below with backoff
  }
  await retryLater(key, current, attempt);
}

async function retryLater(key: string, current: number, attempt: number): Promise<void> {
  await sleep(backoff(attempt, 60_000));
  await subscribe(key, current, attempt + 1);
}

/** Refreshes tokens; on failure logs and returns false, so the caller backs off and retries. */
async function tryRefreshTokens(): Promise<boolean> {
  try {
    await refreshTokens();
    return true;
  } catch (error) {
    log(`token refresh failed: ${(error as Error).message}`);
    return false;
  }
}

async function unsubscribe(key: string): Promise<void> {
  const account = accounts.get(key);
  if (!account) return;
  unsubscribing.add(key);
  try {
    // PITFALL: brandId and accountId must match the subscription exactly, case included.
    // Use the values from the token claims (or from stream messages), never the user's input.
    const ack: StatusMessage = await socket
      .timeout(UNSUBSCRIBE_ACK_TIMEOUT_MS)
      .emitWithAck("subscriptions", { action: "UNSUBSCRIBE", brandId: account.brandId, accountId: account.accountId });
    log(`unsubscribe ${key} -> ${ack.code}`);
  } catch {
    log(`unsubscribe ${key} -> ackTimeout`);
  }
}

// --- Stream --------------------------------------------------------------------------------

socket.on("stream", (message: StreamMessage) => {
  // Every message carries brandId and accountId: one connection, many accounts.
  const key = `${message.brandId}#${message.accountId}`;
  switch (message.type) {
    case "Property":
      if (message.name === "SyncEnd") {
        awaitingSyncEnd.delete(key);
        log(`sync-end ${key}`);
      } else if (message.name === "Disconnected") {
        subscribed.delete(key);
        if (shuttingDown || unsubscribing.delete(key)) {
          log(`stream ended ${key} (unsubscribed)`);
        } else {
          // The server ended this one account (for example its trading session was closed).
          // Subscribe that account again. PITFALL: do not restart the whole connection for it.
          log(`account-disconnected ${key}`);
          const current = generation;
          void sleep(1_000).then(() => subscribe(key, current));
        }
      }
      break;
    case "AccountStatus":
      log(`AccountStatus ${key} balance=${message.balance} equity=${message.equity ?? "-"} ${message.currency}`);
      break;
    case "Position":
      log(`Position ${key} ${message.positionId} ${message.side} ${message.lots} ${message.instrument} @ ${message.openPrice}`);
      break;
    case "ClosePosition":
      log(`ClosePosition ${key} ${message.positionId} @ ${message.closePrice ?? "-"}`);
      break;
    case "OpenOrder":
      log(`OpenOrder ${key} ${message.orderId} ${message.status ?? "-"} ${message.side ?? "-"} ${message.amount} ${message.instrument}`);
      break;
    default:
    // PITFALL: new message types can be added. Ignore what you do not know.
  }
  // PITFALL: keep this handler fast. The server pings every second and closes the connection after
  // 60 s without an answer; heavy work here stalls the event loop. Hand it to a queue instead.
});

// --- Shutdown ------------------------------------------------------------------------------

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal}: unsubscribing ${subscribed.size} account(s)`);
  socket.io.reconnection(false);
  // Closing the connection ends every subscription on it anyway; unsubscribing first is shown
  // here because it is how you stop a single account while keeping the others.
  if (socket.connected) await Promise.all([...subscribed].map(unsubscribe));
  socket.disconnect();
  process.exit(process.exitCode ?? 0);
}
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

// --- Helpers -------------------------------------------------------------------------------

/** Exponential backoff with jitter: about 1 s, 2 s, 4 s ... up to `cap`. */
function backoff(attempt: number, cap: number): number {
  return Math.min(cap, 1_000 * 2 ** Math.min(attempt, 10)) * (0.5 + Math.random());
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    console.error(`Set ${name} (see README.md).`);
    process.exit(2);
  }
  return value;
}

function log(line: string): void {
  // PITFALL: never log the developer key, the user's password or account tokens.
  console.log(line);
}
