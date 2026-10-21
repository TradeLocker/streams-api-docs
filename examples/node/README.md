# Node.js / TypeScript example

One Socket.IO connection, every account of a user, reconnect-safe. Runs TypeScript directly on
Node.js 22.6 or later (type stripping), so there is no build step.

## Run

```bash
npm install
export TL_DEVELOPER_API_KEY=...   # your developer API key
export TL_EMAIL=... TL_PASSWORD=... TL_SERVER=...   # the user's TradeLocker login
# optional: TL_BACKEND_URL (default https://demo.tradelocker.com/backend-api; live accounts:
#           https://live.tradelocker.com/backend-api), TL_STREAMS_URL (default https://api.tradelocker.com)
npm start
```

Ctrl+C unsubscribes the accounts and exits. `npm run typecheck` checks the types with `tsc`.

Against the local mock (from the repository root: `npm install && npm run mock`):

```bash
TL_BACKEND_URL=http://localhost:3005/backend-api TL_STREAMS_URL=http://localhost:3005 \
TL_DEVELOPER_API_KEY=tl-mock-developer-key TL_EMAIL=trader@example.com TL_PASSWORD=mock-password TL_SERVER=MOCKBRAND \
npm start
```

## Files

- [`src/index.ts`](src/index.ts): the whole client. Every `PITFALL:` comment marks a mistake seen in a real integration.
- [`src/messages.ts`](src/messages.ts): payload types, from the AsyncAPI contract.

## Pitfalls this example handles

- One connection for all accounts, at most 10 SUBSCRIBEs in flight.
- `brandId` / `accountId` from the token claims, not from `TL_SERVER` (try `TL_SERVER=mockbrand` against the mock).
- "Awaiting SyncEnd" registered before the SUBSCRIBE is emitted, because `SyncEnd` arrives before the acknowledgement.
- All accounts subscribed again on every new connection, including after `io server disconnect`, which Socket.IO does not retry by itself.
- One account resubscribed after a server-side `Property Disconnected`, without touching the others; the `Disconnected` that follows an UNSUBSCRIBE is ignored.
- 70 s acknowledgement timeout, exponential backoff with jitter, fresh tokens on `invalidJwt` or before expiry.
- Stops instead of looping when the developer key is rejected.
- Decimal strings and IDs kept as strings; unknown message types ignored; no secrets in logs.

## Using socket.io-client in a browser

Do not. The `developer-api-key` header cannot be set on a browser WebSocket, and the key must stay on
your server. Stream on the server and forward what your front end needs.
