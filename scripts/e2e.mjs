// End-to-end check of an example against the mock server, with the failures that bite real
// integrations injected: the socket is dropped once, and one account's stream is ended
// server-side once. The example passes when, judged by what the server saw:
//   - every account was subscribed again after the socket drop, and the ended account again after its end;
//   - no payload failed validation and no UNSUBSCRIBE missed (wrong brand case, wrong account format);
//   - on SIGINT it unsubscribed each account and exited with status 0.
// TL_SERVER is passed in lowercase on purpose: the brand must come from the token, not from user input.
//
//   node scripts/e2e.mjs --cwd examples/node -- npm start --silent
//   node scripts/e2e.mjs --drop transport ...   drop the socket with a bare WebSocket close instead
//                                               of a Socket.IO disconnect (what proxies do)
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const sep = args.indexOf("--");
if (sep < 0 || sep === args.length - 1) {
  console.error("usage: node scripts/e2e.mjs [--cwd <dir>] [--timeout <s>] [--drop namespace|transport] -- <command> [args...]");
  process.exit(2);
}
const opt = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 && i < sep ? args[i + 1] : fallback;
};
const cwd = resolve(root, opt("--cwd", "."));
const timeoutMs = Number(opt("--timeout", "60")) * 1000;
const dropMode = opt("--drop", "namespace");
if (!["namespace", "transport"].includes(dropMode)) throw new Error(`--drop must be namespace or transport, not ${dropMode}`);
const [command, ...commandArgs] = args.slice(sep + 1);

const freePort = () =>
  new Promise((ok) => {
    const s = createServer().listen(0, () => {
      const { port } = s.address();
      s.close(() => ok(port));
    });
  });

const port = await freePort();
const base = `http://localhost:${port}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const prefix = (name, stream, sink) => stream.on("data", (d) => String(d).split("\n").filter(Boolean).forEach((l) => sink(`[${name}] ${l}`)));

const mock = spawn(process.execPath, [join(root, "mock-server", "server.mjs")], {
  env: { ...process.env, PORT: String(port), MOCK_DROP_SOCKET_AFTER_MS: "4000", MOCK_END_ACCOUNT_AFTER_MS: "1500", MOCK_SUBSCRIBE_DELAY_MS: "300", MOCK_DROP_MODE: dropMode },
});
prefix("mock", mock.stdout, console.log);
prefix("mock", mock.stderr, console.error);

const stateOf = async () => (await fetch(`${base}/mock/state`)).json();
for (let i = 0; ; i++) {
  try {
    await stateOf();
    break;
  } catch {
    if (i > 50) throw new Error("mock server did not start");
    await sleep(100);
  }
}

let output = "";
const example = spawn(command, commandArgs, {
  cwd,
  env: {
    ...process.env,
    TL_BACKEND_URL: `${base}/backend-api`,
    TL_STREAMS_URL: base,
    TL_DEVELOPER_API_KEY: "tl-mock-developer-key",
    TL_EMAIL: "trader@example.com",
    TL_PASSWORD: "mock-password",
    TL_SERVER: "mockbrand",
  },
});
const collect = (stream, sink) =>
  stream.on("data", (d) => {
    output += d;
    String(d).split("\n").filter(Boolean).forEach((l) => sink(`[example] ${l}`));
  });
collect(example.stdout, console.log);
collect(example.stderr, console.error);
const exited = new Promise((ok) => example.on("exit", (code, signal) => ok({ code, signal })));

const failures = [];
const deadline = Date.now() + timeoutMs;
let state;
// Expected: account A subscribed, ended by the server, subscribed again; the socket dropped;
// both accounts subscribed again on the new socket. So 5 successful SUBSCRIBEs and 5 SyncEnds.
for (;;) {
  state = await stateOf();
  const syncEnds = (output.match(/^sync-end /gm) ?? []).length;
  const subscribed = state.acks.connected ?? 0;
  if (state.droppedSockets >= 1 && state.serverEndedAccounts.length >= 1 && subscribed >= 5 && syncEnds >= 5) break;
  if (Date.now() > deadline) {
    failures.push(`timed out: dropped=${state.droppedSockets} ended=${state.serverEndedAccounts.length} connected acks=${subscribed} sync-end lines=${syncEnds}`);
    break;
  }
  await sleep(250);
}

// Let a few live updates through, then ask the example to stop.
await sleep(1500);
example.kill("SIGINT");
let graceTimer;
const result = await Promise.race([exited, new Promise((ok) => (graceTimer = setTimeout(() => ok(null), 15000)))]);
clearTimeout(graceTimer);
state = await stateOf();

if (!result) {
  failures.push("example did not exit within 15 s of SIGINT");
  example.kill("SIGKILL");
} else if (result.code !== 0) {
  failures.push(`example exited with ${result.code ?? result.signal}`);
}
if (state.validationErrors.length) failures.push(`validation errors: ${state.validationErrors.join("; ")}`);
if (state.acks.noSuchConnectionError) failures.push(`${state.acks.noSuchConnectionError} UNSUBSCRIBE(s) did not match a subscription (brand case or account format)`);
if (Object.keys(state.rejectedHandshakes).length) failures.push(`rejected handshakes: ${JSON.stringify(state.rejectedHandshakes)}`);
const accounts = Object.keys(state.subscribes);
if (accounts.length !== 2) failures.push(`expected 2 accounts subscribed, saw ${JSON.stringify(state.subscribes)}`);
for (const key of accounts) {
  if (!key.startsWith("MOCKBRAND#")) failures.push(`unexpected account key ${key}`);
  if ((state.unsubscribes[key] ?? 0) < 1) failures.push(`${key} was not unsubscribed on shutdown`);
}
const ended = state.serverEndedAccounts[0];
if (ended && (state.subscribes[ended] ?? 0) < 3) failures.push(`${ended} was not subscribed again after its stream ended (subscribes=${state.subscribes[ended]})`);
if (!/AccountStatus /.test(output)) failures.push("no AccountStatus line in the output");

mock.kill();
console.log(`\nmock state: ${JSON.stringify(state)}`);
if (failures.length) {
  console.error(`\nFAIL (drop=${dropMode}) ${command} ${commandArgs.join(" ")}\n  - ${failures.join("\n  - ")}`);
  process.exit(1);
}
console.log(`\nPASS (drop=${dropMode}) ${command} ${commandArgs.join(" ")}`);
