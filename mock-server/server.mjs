// Local mock of the TradeLocker Streams API, for wiring tests without real credentials.
//
// It follows the production contract closely: namespace /streams-api, path /streams-api/socket.io,
// websocket transport only, the `developer-api-key` handshake header, the `connection`,
// `subscriptions` (with acknowledgement) and `stream` events, the same validation rules, result
// codes and message order (snapshot, then SyncEnd, then the SUBSCRIBE acknowledgement). Account
// data is made up. It is not a substitute for testing against the real service.
//
// It also serves POST /backend-api/auth/jwt/accounts/tokens, so an example can run end to end:
//   TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005
//   TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND
//
// Fault injection (environment variables), to exercise reconnect logic:
//   MOCK_DROP_SOCKET_AFTER_MS=n      close the first client socket once, n ms after its first subscribe
//   MOCK_DROP_MODE=namespace|transport  how: a Socket.IO disconnect packet ("io server disconnect",
//                                    default), or a bare WebSocket close as proxies and load
//                                    balancers do it (no Socket.IO packet first)
//   MOCK_END_ACCOUNT_AFTER_MS=n      end one account's stream server-side once (Property Disconnected)
//   MOCK_SUBSCRIBE_DELAY_MS=n        simulated trade-node logon time per SUBSCRIBE (default 200)
//   MOCK_MAX_CONNECTIONS=n           sockets allowed per developer key (default 5)
//   PORT=n                           default 3005
//
// GET /mock/state returns what the server saw (subscribes, validation errors, ...) for test harnesses.
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { Server } from "socket.io";

const PORT = Number(process.env.PORT ?? 3005);
const DEVELOPER_API_KEY = process.env.MOCK_DEVELOPER_API_KEY ?? "tl-mock-developer-key";
const EMAIL = process.env.MOCK_EMAIL ?? "trader@example.com";
const PASSWORD = process.env.MOCK_PASSWORD ?? "mock-password";
const MAX_CONNECTIONS = Number(process.env.MOCK_MAX_CONNECTIONS ?? 5);
const SUBSCRIBE_DELAY_MS = Number(process.env.MOCK_SUBSCRIBE_DELAY_MS ?? 200);
const DROP_SOCKET_AFTER_MS = Number(process.env.MOCK_DROP_SOCKET_AFTER_MS ?? 0);
const DROP_MODE = process.env.MOCK_DROP_MODE ?? "namespace";
const END_ACCOUNT_AFTER_MS = Number(process.env.MOCK_END_ACCOUNT_AFTER_MS ?? 0);
const RATE_LIMIT = { maxRequests: 5000, windowMs: 50 };

const state = {
  connections: 0,
  rejectedHandshakes: {},
  subscribes: {},
  acks: {},
  unsubscribes: {},
  validationErrors: [],
  serverEndedAccounts: [],
  droppedSockets: 0,
};
const count = (map, key) => (map[key] = (map[key] ?? 0) + 1);

// ---------------------------------------------------------------------------------------------
// Account tokens (unsigned JWTs; the real ones are RS256-signed and verified by the server)

const b64url = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64url");

function makeToken(brand, aid, userId) {
  const now = Math.floor(Date.now() / 1000);
  const claims = { iss: "mock", aud: "streams", type: "access", sub: userId, uid: userId, brand, host: "mock", aid, iat: now, exp: now + 3600 };
  return `${b64url({ alg: "none", typ: "JWT" })}.${b64url(claims)}.mock`;
}

function decodeToken(token) {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

function handleTokens(req, res) {
  let body = "";
  req.on("data", (chunk) => (body += chunk));
  req.on("end", () => {
    let creds;
    try {
      creds = JSON.parse(body);
    } catch {
      return json(res, 400, { message: "Invalid JSON body.", statusCode: 400 });
    }
    if (creds.email !== EMAIL || creds.password !== PASSWORD || typeof creds.server !== "string" || !creds.server) {
      return json(res, 403, { message: "Invalid credentials.", statusCode: 403 });
    }
    // Like production: the brand in the token is the canonical (uppercase) brand, whatever case
    // the user typed. Clients must use the token's claim, not the user's input.
    const brand = creds.server.toUpperCase();
    const userId = randomUUID();
    const expireDate = new Date(Date.now() + 3600_000).toISOString();
    const data = ["D#1001", "L#2002"].map((accountId) => ({ accessToken: makeToken(brand, accountId, userId), expireDate, accountId }));
    json(res, 201, { data });
  });
}

function json(res, status, payload) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(payload));
}

// ---------------------------------------------------------------------------------------------
// Validation: same rules, issue codes and messages as the production SubscriptionActionParser

const ACCOUNT_ID = /^[LD]#[0-9]+$/;
const typeName = (v) => (v === undefined ? "undefined" : v === null ? "null" : Array.isArray(v) ? "array" : typeof v);
const invalid = (issue, message) => ({ issue, message });

function parseAction(payload) {
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    return invalid("invalid_type", `Expected object, received ${typeName(payload)}`);
  }
  const discriminator = () => invalid("invalid_union_discriminator", "action: Invalid discriminator value. Expected 'SUBSCRIBE' | 'UNSUBSCRIBE'");
  const requireString = (field) => {
    if (!(field in payload)) return invalid("invalid_type", `${field}: Required`);
    if (typeof payload[field] !== "string") return invalid("invalid_type", `${field}: Expected string, received ${typeName(payload[field])}`);
    return null;
  };
  const rejectUnknown = (allowed) => {
    const unknown = Object.keys(payload).filter((k) => !allowed.includes(k));
    return unknown.length ? invalid("unrecognized_keys", `Unrecognized key(s) in object: ${unknown.map((k) => `'${k}'`).join(", ")}`) : null;
  };
  if (typeof payload.action !== "string") return discriminator();
  if (payload.action === "SUBSCRIBE") {
    return requireString("token") ?? rejectUnknown(["action", "token"]) ?? { action: "SUBSCRIBE", token: payload.token };
  }
  if (payload.action === "UNSUBSCRIBE") {
    const issue = requireString("brandId") ?? requireString("accountId");
    if (issue) return issue;
    if (!ACCOUNT_ID.test(payload.accountId)) return invalid("invalid_string", "accountId: Account ID must be formatted as: <accType>#<accId>");
    return rejectUnknown(["action", "brandId", "accountId"]) ?? { action: "UNSUBSCRIBE", brandId: payload.brandId, accountId: payload.accountId };
  }
  return discriminator();
}

const ok = (code, message, remainingRequests) => ({ status: "ok", code, message, ...(remainingRequests === undefined ? {} : { remainingRequests }) });
const error = (code, message, remainingRequests) => ({ status: "error", code, message, ...(remainingRequests === undefined ? {} : { remainingRequests }) });

// ---------------------------------------------------------------------------------------------
// Made-up account data

function snapshot(account) {
  const ids = { brandId: account.brand, userId: account.userId, accountId: account.aid };
  return [
    accountStatus(account, ids),
    { type: "Position", lots: "0.01", lotSize: "10", units: "0.1", instrument: "ETHUSD", openPrice: "3299.71", openDateTime: "2026-03-19T20:15:51.465Z", positionId: "144115188076017329", openOrderId: "144115188076017328", maintMargin: "3.06485", side: "BUY", ...ids },
    { type: "OpenOrder", orderId: "144115188077153593", instrument: "AUDUSD", amount: "10", lotSize: "100000", side: "BUY", price: "0.62956", status: "STATUS_ACCEPTED", orderType: "LIMIT", tif: "GTC", filledAmount: "0", averageFilledPrice: "0", isOpen: true, createdDateTime: "2026-01-27T13:43:57.327Z", lastUpdate: "2026-01-27T13:43:57.532Z", ...ids },
  ];
}

function accountStatus(account, ids) {
  const pnl = (Math.round((Math.random() * 200 - 100) * 100) / 100).toString();
  const balance = "9888.95";
  return { type: "AccountStatus", currency: "USD", balance, balanceWithoutCredit: balance, equity: (9888.95 + Number(pnl)).toFixed(2), marginAvailable: "3583.45", marginUsed: "6305.5", blockedBalance: "0", credit: "0", positionPnLs: [{ positionId: "144115188076017329", pnl }], ...ids };
}

// ---------------------------------------------------------------------------------------------
// Socket.IO

const http = createServer((req, res) => {
  if (req.method === "POST" && req.url === "/backend-api/auth/jwt/accounts/tokens") return handleTokens(req, res);
  if (req.method === "GET" && req.url === "/mock/state") return json(res, 200, state);
  json(res, 404, { message: "Not found" });
});

const io = new Server(http, {
  path: "/streams-api/socket.io",
  transports: ["websocket"],
  pingInterval: 1000,
  pingTimeout: 60000,
  cors: { origin: "*" },
});

const socketsByKey = new Map();
const rateWindows = new Map();
let accountEnded = false;

function allowSubscribe(developer) {
  const now = Date.now();
  const window = (rateWindows.get(developer) ?? []).filter((t) => now - t < RATE_LIMIT.windowMs);
  if (window.length >= RATE_LIMIT.maxRequests) return { allowed: false, remaining: 0 };
  window.push(now);
  rateWindows.set(developer, window);
  return { allowed: true, remaining: RATE_LIMIT.maxRequests - window.length };
}

io.of("/streams-api").on("connection", (socket) => {
  const header = socket.handshake.headers["developer-api-key"];
  const reject = (code, message) => {
    count(state.rejectedHandshakes, code);
    socket.emit("connection", error(code, message));
    socket.disconnect(true);
  };
  if (typeof header !== "string" || header.length === 0 || header.includes(",")) {
    return reject("unauthorized.streams.invalidApiKey", "Invalid API key provided, make sure header 'developer-api-key' is valid.");
  }
  if (header !== DEVELOPER_API_KEY) {
    return reject("unauthorized.streams.developer.notFound", `Developer with the API key '${header.slice(0, 6)}***' not found.`);
  }
  const open = socketsByKey.get(header) ?? new Set();
  if (open.size >= MAX_CONNECTIONS) {
    return reject("conflict", `Developer MOCK has reached the maximum number of connections: ${open.size} (max: ${MAX_CONNECTIONS}).`);
  }
  open.add(socket);
  socketsByKey.set(header, open);
  state.connections++;

  const accounts = new Map(); // "BRAND#L#1" -> { brand, aid, userId, timer, status: "connecting" | "connected" }
  let dropScheduled = false;

  const emitProperty = (account, name) =>
    socket.emit("stream", { type: "Property", name, brandId: account.brand, userId: account.userId, accountId: account.aid });

  const endAccount = (key, name = "Disconnected") => {
    const account = accounts.get(key);
    if (!account) return false;
    clearInterval(account.timer);
    accounts.delete(key);
    emitProperty(account, name);
    return true;
  };

  socket.emit("connection", ok("connected", "Successfully connected to the streams socket server."));

  socket.on("subscriptions", async (payload, ack) => {
    const reply = (response) => {
      count(state.acks, response.code);
      if (typeof ack === "function") ack(response);
    };
    const action = parseAction(payload);
    if (action.issue) {
      state.validationErrors.push(`${action.issue}: ${action.message}`);
      return reply(error(`validationError - ${action.issue}`, action.message));
    }

    if (action.action === "UNSUBSCRIBE") {
      const key = `${action.brandId}#${action.accountId}`;
      count(state.unsubscribes, key);
      return endAccount(key) ? reply(ok("disconnected", `Disconnected from account ${key}`)) : reply(error("noSuchConnectionError", `No such connection for account ${key}`));
    }

    const claims = decodeToken(action.token);
    if (!claims || typeof claims.brand !== "string" || typeof claims.aid !== "string") return reply(error("invalidJwt", "Failed to decode token"));
    if (claims.exp * 1000 < Date.now()) return reply(error("invalidJwt", "Expired JWT"));
    const rate = allowSubscribe(header);
    if (!rate.allowed) return reply(error("tooManyRequests", `Too many subscribe requests. Please wait. maxRequests=${RATE_LIMIT.maxRequests} windowMs=${RATE_LIMIT.windowMs} ms`));

    const key = `${claims.brand}#${claims.aid}`;
    count(state.subscribes, key);
    if (accounts.has(key)) return reply(error("alreadyConnectedError", `Already connected to account ${key}`));
    const account = { brand: claims.brand, aid: claims.aid, userId: claims.uid, status: "connecting" };
    accounts.set(key, account);

    await new Promise((r) => setTimeout(r, SUBSCRIBE_DELAY_MS));
    if (!socket.connected || accounts.get(key) !== account) return reply(error("clientNotFound", "Connection for developerId not found."));

    // Production order: snapshot, SyncEnd, then the acknowledgement.
    for (const message of snapshot(account)) socket.emit("stream", message);
    emitProperty(account, "SyncEnd");
    account.status = "connected";
    account.timer = setInterval(() => socket.emit("stream", accountStatus(account, { brandId: account.brand, userId: account.userId, accountId: account.aid })), 1000);
    reply(ok("connected", `Connected to account ${key}`, rate.remaining));

    if (END_ACCOUNT_AFTER_MS > 0 && !accountEnded) {
      accountEnded = true;
      setTimeout(() => {
        if (endAccount(key)) state.serverEndedAccounts.push(key);
      }, END_ACCOUNT_AFTER_MS);
    }
    if (DROP_SOCKET_AFTER_MS > 0 && !dropScheduled && state.droppedSockets === 0) {
      dropScheduled = true;
      setTimeout(() => {
        state.droppedSockets++;
        if (DROP_MODE === "transport") socket.conn.close();
        else socket.disconnect(true);
      }, DROP_SOCKET_AFTER_MS);
    }
  });

  socket.on("disconnect", () => {
    for (const account of accounts.values()) clearInterval(account.timer);
    accounts.clear();
    open.delete(socket);
  });
});

http.listen(PORT, () => console.log(`Mock Streams API on http://localhost:${PORT} (developer-api-key ${DEVELOPER_API_KEY})`));
