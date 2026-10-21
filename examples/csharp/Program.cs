// TradeLocker Streams API example for .NET 10 (C#, SocketIOClient 4.x).
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
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using SocketIOClient;
using SocketIOClient.Common;
using TradeLocker.Streams.Example;

var config = new Config(
    DeveloperApiKey: Required("TL_DEVELOPER_API_KEY"),
    Email: Required("TL_EMAIL"),
    Password: Required("TL_PASSWORD"),
    Server: Required("TL_SERVER"),
    BackendUrl: Optional("TL_BACKEND_URL", "https://demo.tradelocker.com/backend-api"),
    StreamsUrl: Optional("TL_STREAMS_URL", "https://api.tradelocker.com"));

using var client = new StreamsClient(config);

// Ctrl+C (SIGINT) and SIGTERM (docker stop, Kubernetes): unsubscribe, disconnect, exit 0.
// Cancel = true keeps the runtime from killing the process before the shutdown below has run.
using var sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, signal => { signal.Cancel = true; client.Stop("SIGINT"); });
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal => { signal.Cancel = true; client.Stop("SIGTERM"); });

return await client.RunAsync();

static string Required(string name)
{
    var value = Environment.GetEnvironmentVariable(name);
    if (!string.IsNullOrEmpty(value)) return value;
    Console.Error.WriteLine($"Set {name} (see README.md).");
    Environment.Exit(2);
    return ""; // not reached
}

static string Optional(string name, string fallback) =>
    (Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback).TrimEnd('/');

namespace TradeLocker.Streams.Example
{
    public sealed record Config(string DeveloperApiKey, string Email, string Password, string Server, string BackendUrl, string StreamsUrl)
    {
        // PITFALL: records print every property in ToString(). Keep the key and the password out of logs.
        public override string ToString() => $"Config {{ Email = {Email}, Server = {Server}, BackendUrl = {BackendUrl}, StreamsUrl = {StreamsUrl} }}";
    }

    public sealed record Account(string Key, string BrandId, string AccountId, string Token, DateTimeOffset ExpiresAt)
    {
        public override string ToString() => Key; // never the token
    }

    public sealed class StreamsClient : IDisposable
    {
        // PITFALL: a SUBSCRIBE includes the trade-node logon and the account snapshot and can take up to
        // about 60 s. A shorter acknowledgement timeout makes you retry subscriptions that are succeeding.
        static readonly TimeSpan SubscribeAckTimeout = TimeSpan.FromSeconds(70);
        static readonly TimeSpan UnsubscribeAckTimeout = TimeSpan.FromSeconds(10);
        const int MaxSubscribesInFlight = 10;
        static readonly TimeSpan TokenRefreshMargin = TimeSpan.FromSeconds(60);
        static readonly TimeSpan MaxReconnectDelay = TimeSpan.FromSeconds(30);
        static readonly TimeSpan MaxSubscribeRetryDelay = TimeSpan.FromSeconds(60);
        // The server pings every second and closes the connection after 60 s without a pong.
        static readonly TimeSpan MaxPingSilence = TimeSpan.FromSeconds(61);

        readonly Config config;
        readonly HttpClient http = new() { Timeout = TimeSpan.FromSeconds(30) };
        readonly SocketIO socket;
        readonly CancellationTokenSource stopping = new();
        readonly SemaphoreSlim refreshGate = new(1, 1);
        readonly SemaphoreSlim subscribeSlots = new(MaxSubscribesInFlight, MaxSubscribesInFlight);

        volatile IReadOnlyDictionary<string, Account> accounts = new Dictionary<string, Account>();
        int generation; // bumped on every accepted connection; work for an older connection stops
        volatile bool fatal; // the developer key was rejected: stop reconnecting
        string? stopSignal;
        int reconnectAttempt;
        long lastPingTimestamp;
        TaskCompletionSource<string> connectionClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenSource connectionScope = new(); // cancelled when the socket closes: fails pending acks
        readonly KeySet subscribed = new(); // accounts acknowledged on the current connection
        readonly KeySet awaitingSyncEnd = new();
        readonly KeySet unsubscribing = new();

        public StreamsClient(Config config)
        {
            this.config = config;

            // PITFALL: one connection carries all accounts. Do not open a connection per account: connections
            // per developer key are capped, and above the cap the handshake is rejected with `conflict`.
            // The namespace is the URL path (/streams-api); the handshake path is a separate option.
            socket = new SocketIO(new Uri($"{config.StreamsUrl}/streams-api"), new SocketIOOptions
            {
                Path = "/streams-api/socket.io",
                // PITFALL: the server accepts the websocket transport only; HTTP long-polling is rejected.
                // SocketIOClient starts with polling unless you set this.
                Transport = TransportProtocol.WebSocket,
                ExtraHeaders = new Dictionary<string, string> { ["developer-api-key"] = config.DeveloperApiKey },
                // SocketIOClient's own reconnect gives up after ReconnectionAttempts and never runs after the
                // server closed the connection. RunAsync reconnects instead, after every kind of disconnect.
                Reconnection = false,
            }, services => services.AddSystemTextJson(Messages.Json));

            socket.OnConnected += (_, _) => Log($"socket connected id={socket.Id}");
            socket.OnPing += (_, _) => Volatile.Write(ref lastPingTimestamp, Stopwatch.GetTimestamp());

            // PITFALL: subscriptions belong to the connection. After any disconnect, the client reconnects
            // with a NEW connection that has no subscriptions, and nothing arrives until you subscribe again.
            socket.OnDisconnected += (_, reason) =>
            {
                subscribed.Clear();
                awaitingSyncEnd.Clear();
                connectionScope.Cancel(); // SocketIOClient drops pending acknowledgements without a word
                Log($"socket disconnected reason={reason}");
                // PITFALL: Socket.IO clients reconnect by themselves after network errors, but NOT when the
                // server closed the connection ("io server disconnect"), for example on a deploy. RunAsync
                // reconnects after every disconnect; without that, the process keeps running and every
                // account goes silent.
                connectionClosed.TrySetResult(reason);
            };

            socket.On("connection", ctx =>
            {
                OnConnectionStatus(ctx.GetValue<StatusMessage>(0) ?? new StatusMessage("error", "unknown", "empty status"));
                return Task.CompletedTask;
            });

            socket.On("stream", ctx =>
            {
                // PITFALL: SocketIOClient swallows exceptions thrown in handlers (they go to its ILogger,
                // which prints nothing by default), so a parsing bug would drop messages silently.
                try
                {
                    OnStream(ctx);
                }
                catch (Exception e)
                {
                    Log($"cannot read a stream message: {e.Message}");
                }
                // PITFALL: keep this handler fast. SocketIOClient runs handlers on its receive loop: while one
                // runs, nothing else is read, not even the server's pings (the server closes the connection
                // after 60 s without an answer). Hand heavy work to a queue (System.Threading.Channels).
                return Task.CompletedTask;
            });
        }

        // --- Connection --------------------------------------------------------------------------

        /// <summary>Runs until <see cref="Stop"/> is called or the developer key is rejected. Returns the exit code.</summary>
        public async Task<int> RunAsync()
        {
            try
            {
                await RefreshTokensAsync();
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                return 0;
            }
            catch (Exception e)
            {
                Log($"cannot get account tokens: {e.Message}");
                return 1;
            }
            Background(WatchPingsAsync);

            while (!stopping.IsCancellationRequested && !fatal)
            {
                connectionClosed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                connectionScope = new CancellationTokenSource();
                Volatile.Write(ref lastPingTimestamp, Stopwatch.GetTimestamp());
                try
                {
                    await socket.ConnectAsync(stopping.Token);
                    await connectionClosed.Task.WaitAsync(stopping.Token);
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception e)
                {
                    Log($"socket connect error: {e.GetBaseException().Message}");
                }
                if (stopping.IsCancellationRequested || fatal) break;

                // Exponential backoff with jitter; every accepted connection resets it.
                try
                {
                    await Task.Delay(Backoff(reconnectAttempt++, MaxReconnectDelay), stopping.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            if (stopping.IsCancellationRequested) await ShutdownAsync();
            return fatal ? 1 : 0;
        }

        // The server answers every handshake on the `connection` event. Subscribe only after `connected`.
        void OnConnectionStatus(StatusMessage status)
        {
            if (status is { Status: "ok", Code: "connected" })
            {
                Log("connection ok");
                reconnectAttempt = 0;
                var current = Interlocked.Increment(ref generation);
                // PITFALL: never wait for an acknowledgement inside a SocketIOClient handler. The handler runs
                // on the receive loop, so the acknowledgement it waits for can never be read. Start the work
                // in the background and return.
                Background(() => SubscribeAllAsync(current));
                return;
            }
            if (status is { Status: "ok", Code: "disconnected" })
            {
                // All subscriptions on this connection have ended. PITFALL: SocketIOClient 4.0.5 does not notice
                // a WebSocket close without a Socket.IO disconnect packet, so close it yourself and reconnect.
                Log("server is shutting down this connection; reconnecting");
                Background(DisconnectAsync);
                return;
            }
            Log($"connection error code={status.Code} message={status.Message}");
            if (status.Code.StartsWith("unauthorized.", StringComparison.Ordinal))
            {
                // PITFALL: a wrong or unknown developer key never starts working by retrying. Stop instead of
                // reconnecting in a loop (that loop is visible to us, and rejected handshakes are throttled).
                fatal = true;
                Background(DisconnectAsync);
            }
            // `conflict` (too many connections for the key) and `unknown`: the server closes the socket and
            // RunAsync retries with backoff. Look for connections you leaked elsewhere.
        }

        /// <summary>
        /// PITFALL: SocketIOClient 4.0.5 neither checks that the server's pings keep coming nor notices a
        /// WebSocket close frame that is not preceded by a Socket.IO disconnect packet. Either way the
        /// connection looks alive while nothing arrives. Close it when the pings stop; RunAsync reconnects.
        /// </summary>
        async Task WatchPingsAsync()
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
            while (await timer.WaitForNextTickAsync(stopping.Token))
            {
                if (socket.Connected && Stopwatch.GetElapsedTime(Volatile.Read(ref lastPingTimestamp)) > MaxPingSilence)
                {
                    Log($"no ping from the server for {MaxPingSilence.TotalSeconds:0} s; closing the connection");
                    await DisconnectAsync();
                }
            }
        }

        async Task DisconnectAsync()
        {
            // SocketIOClient waits for the server's close frame without a time limit; give it one.
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            try
            {
                await socket.DisconnectAsync(timeout.Token);
            }
            catch (Exception)
            {
                // the connection is gone either way
            }
        }

        // --- Account tokens ----------------------------------------------------------------------

        /// <summary>
        /// Fetches new tokens for all accounts. One refresh at a time, however many subscriptions ask for
        /// it: when <paramref name="stale"/> (the account whose token was found expiring or rejected) has
        /// already been given a new token by another caller, its tokens are used instead.
        /// </summary>
        async Task RefreshTokensAsync(Account? stale = null)
        {
            await refreshGate.WaitAsync(stopping.Token);
            try
            {
                if (stale is not null && accounts.TryGetValue(stale.Key, out var current) && current.Token != stale.Token) return;

                // PITFALL: this endpoint takes the user's credentials only. Do not send your developer key here.
                using var response = await http.PostAsJsonAsync(
                    $"{config.BackendUrl}/auth/jwt/accounts/tokens",
                    new TokensRequest(config.Email, config.Password, config.Server),
                    Messages.Json,
                    stopping.Token);
                if (!response.IsSuccessStatusCode)
                {
                    throw new HttpRequestException($"account tokens request failed: HTTP {(int)response.StatusCode}");
                }
                var body = await response.Content.ReadFromJsonAsync<TokensResponse>(Messages.Json, stopping.Token);

                var next = new Dictionary<string, Account>();
                foreach (var token in body?.Data ?? [])
                {
                    // PITFALL: the response has no brandId, and the brand the user typed ("Blueg") is not
                    // necessarily the brand the server uses ("BLUEG"). Take brand and account from the token's
                    // claims: they are exactly what the server matches UNSUBSCRIBE and stream messages against.
                    var (brand, aid) = JwtClaims(token.AccessToken);
                    var key = $"{brand}#{aid}";
                    next[key] = new Account(key, brand, aid, token.AccessToken, token.ExpireDate);
                }
                accounts = next;
                Log($"tokens for {next.Count} account(s): {string.Join(", ", next.Keys)}");
            }
            finally
            {
                refreshGate.Release();
            }
        }

        /// <summary>Reads the JWT payload. No signature check needed: the Streams API verifies the token.</summary>
        static (string Brand, string Aid) JwtClaims(string token)
        {
            var payload = token.Split('.') is [_, var middle, _] ? middle : "";
            var claims = JsonSerializer.Deserialize<TokenClaims>(Base64Url.DecodeFromChars(payload), Messages.Json);
            if (claims is not { Brand: { } brand, Aid: { } aid }) throw new FormatException("token has no brand/aid claims");
            return (brand, aid);
        }

        // --- Subscriptions -----------------------------------------------------------------------

        Task SubscribeAllAsync(int current) =>
            Task.WhenAll(accounts.Keys.Select(key => SubscribeAsync(key, current)));

        async Task SubscribeAsync(string key, int current)
        {
            for (var attempt = 0; ; attempt++)
            {
                if (current != Volatile.Read(ref generation) || !socket.Connected || stopping.IsCancellationRequested) return;
                try
                {
                    if (await SubscribeOnceAsync(key, current)) return;
                }
                catch (Exception e) when (!stopping.IsCancellationRequested)
                {
                    Log($"subscribe {key} failed: {e.Message}");
                }
                await Task.Delay(Backoff(attempt, MaxSubscribeRetryDelay), stopping.Token);
            }
        }

        /// <summary>One SUBSCRIBE. Returns false when it should be retried with backoff.</summary>
        async Task<bool> SubscribeOnceAsync(string key, int current)
        {
            if (!accounts.TryGetValue(key, out var account)) return true;
            // PITFALL: a SUBSCRIBE needs a token that has not expired. Tokens are short-lived: fetch new ones
            // before you subscribe again after a reconnect. (Once subscribed, the stream does not depend on
            // the token any more.)
            if (account.ExpiresAt - DateTimeOffset.UtcNow < TokenRefreshMargin)
            {
                await RefreshTokensAsync(stale: account);
                if (!accounts.TryGetValue(key, out account)) return true;
            }

            StatusMessage ack;
            await subscribeSlots.WaitAsync(stopping.Token); // at most MaxSubscribesInFlight at a time
            try
            {
                if (current != Volatile.Read(ref generation) || !socket.Connected) return true;
                // PITFALL: SyncEnd for this account can arrive BEFORE the acknowledgement of this SUBSCRIBE.
                // Start waiting for it before you emit, or you will miss it.
                awaitingSyncEnd.Add(key);
                // PITFALL: always ask for the acknowledgement. It is the only place the result is reported.
                // SUBSCRIBE takes exactly { action, token }; any other key fails validation.
                ack = await EmitWithAckAsync(new SubscribeAction("SUBSCRIBE", account.Token), SubscribeAckTimeout);
            }
            finally
            {
                subscribeSlots.Release();
            }
            Log($"subscribe {key} -> {ack.Code}");

            switch (ack.Code)
            {
                case "connected":
                case "alreadyConnectedError": // already subscribed on this connection: nothing to do
                    subscribed.Add(key);
                    return true;
                case "clientNotFound": // the connection is going away; the reconnect subscribes again
                case "connectionClosed": // (set by EmitWithAckAsync: the socket closed before the ack)
                    return true;
                case "invalidJwt": // malformed, expired or for another environment
                    await RefreshTokensAsync(stale: account);
                    return false;
                case var code when code.StartsWith("validationError", StringComparison.Ordinal):
                    Log($"bug in this client, not retrying: {ack.Message}");
                    return true;
                default: // tooManyRequests, connectionError, unknown, ackTimeout: retry with backoff
                    return false;
            }
        }

        async Task UnsubscribeAsync(string key)
        {
            if (!accounts.TryGetValue(key, out var account)) return;
            unsubscribing.Add(key);
            // PITFALL: brandId and accountId must match the subscription exactly, case included.
            // Use the values from the token claims (or from stream messages), never the user's input.
            var ack = await EmitWithAckAsync(
                new UnsubscribeAction("UNSUBSCRIBE", account.BrandId, account.AccountId), UnsubscribeAckTimeout);
            Log($"unsubscribe {key} -> {ack.Code}");
        }

        /// <summary>
        /// Emits `subscriptions` and waits for the acknowledgement. SocketIOClient has no acknowledgement
        /// timeout, and forgets pending acknowledgements when the socket closes: both are handled here.
        /// </summary>
        async Task<StatusMessage> EmitWithAckAsync(object payload, TimeSpan timeout)
        {
            var scope = connectionScope.Token;
            var result = new TaskCompletionSource<StatusMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                await socket.EmitAsync("subscriptions", [payload], ack =>
                {
                    // PITFALL: SocketIOClient runs this callback on its receive loop, and an exception thrown
                    // here closes the connection. Hand the result over and return.
                    try
                    {
                        result.TrySetResult(ack.GetValue<StatusMessage>(0) ?? new StatusMessage("error", "unknown", "empty acknowledgement"));
                    }
                    catch (Exception e)
                    {
                        result.TrySetException(e);
                    }
                    return Task.CompletedTask;
                }, scope);
                return await result.Task.WaitAsync(timeout, scope);
            }
            catch (TimeoutException)
            {
                return new StatusMessage("error", "ackTimeout", "no acknowledgement");
            }
            catch (Exception) when (scope.IsCancellationRequested || !socket.Connected)
            {
                return new StatusMessage("error", "connectionClosed", "the socket closed before the acknowledgement");
            }
        }

        // --- Stream ------------------------------------------------------------------------------

        void OnStream(IEventContext ctx)
        {
            // Every message carries brandId and accountId: one connection, many accounts.
            var header = ctx.GetValue<StreamMessage>(0);
            if (header is null) return;
            var key = $"{header.BrandId}#{header.AccountId}";

            switch (header.Type)
            {
                case "Property":
                    var property = ctx.GetValue<Property>(0)!;
                    if (property.Name == "SyncEnd")
                    {
                        awaitingSyncEnd.Remove(key);
                        Log($"sync-end {key}");
                    }
                    else if (property.Name == "Disconnected")
                    {
                        subscribed.Remove(key);
                        if (stopping.IsCancellationRequested || unsubscribing.Remove(key))
                        {
                            Log($"stream ended {key} (unsubscribed)");
                        }
                        else
                        {
                            // The server ended this one account (for example its trading session was closed).
                            // Subscribe that account again. PITFALL: do not restart the whole connection for it.
                            Log($"account-disconnected {key}");
                            var current = Volatile.Read(ref generation);
                            Background(async () =>
                            {
                                await Task.Delay(TimeSpan.FromSeconds(1), stopping.Token);
                                await SubscribeAsync(key, current);
                            });
                        }
                    }
                    break;
                case "AccountStatus":
                    var status = ctx.GetValue<AccountStatus>(0)!;
                    Log($"AccountStatus {key} balance={Show(status.Balance)} equity={Show(status.Equity)} {status.Currency}");
                    break;
                case "Position":
                    var position = ctx.GetValue<Position>(0)!;
                    Log($"Position {key} {position.PositionId} {position.Side} {Show(position.Lots)} {position.Instrument} @ {Show(position.OpenPrice)}");
                    break;
                case "ClosePosition":
                    var closed = ctx.GetValue<ClosePosition>(0)!;
                    Log($"ClosePosition {key} {closed.PositionId} @ {Show(closed.ClosePrice)}");
                    break;
                case "OpenOrder":
                    var order = ctx.GetValue<OpenOrder>(0)!;
                    Log($"OpenOrder {key} {order.OrderId} {order.Status ?? "-"} {order.Side ?? "-"} {Show(order.Amount)} {order.Instrument}");
                    break;
                default:
                    // PITFALL: new message types can be added. Ignore what you do not know.
                    break;
            }
        }

        // --- Shutdown ----------------------------------------------------------------------------

        /// <summary>Called from the signal handler: stops reconnecting and lets RunAsync shut down.</summary>
        public void Stop(string signal)
        {
            if (Interlocked.CompareExchange(ref stopSignal, signal, null) is null) stopping.Cancel();
        }

        async Task ShutdownAsync()
        {
            var keys = subscribed.ToArray();
            Log($"{stopSignal}: unsubscribing {keys.Length} account(s)");
            // Closing the connection ends every subscription on it anyway; unsubscribing first is shown
            // here because it is how you stop a single account while keeping the others.
            if (socket.Connected) await Task.WhenAll(keys.Select(UnsubscribeAsync));
            await DisconnectAsync();
        }

        public void Dispose()
        {
            socket.Dispose();
            http.Dispose();
        }

        // --- Helpers -----------------------------------------------------------------------------

        /// <summary>Exponential backoff with jitter: about 1 s, 2 s, 4 s, ... up to <paramref name="max"/>.</summary>
        static TimeSpan Backoff(int attempt, TimeSpan max)
        {
            var ceiling = Math.Min(max.TotalMilliseconds, 1000 * Math.Pow(2, Math.Min(attempt, 16)));
            return TimeSpan.FromMilliseconds(ceiling * (0.5 + Random.Shared.NextDouble() / 2));
        }

        /// <summary>Runs work off the caller's thread (never on SocketIOClient's receive loop) and logs failures.</summary>
        void Background(Func<Task> work) => _ = Task.Run(async () =>
        {
            try
            {
                await work();
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
            }
            catch (Exception e)
            {
                Log($"background task failed: {e}");
            }
        });

        // PITFALL: string interpolation formats decimals with the machine's culture ("9888,95" in de-DE).
        static string Show(decimal? value) => value?.ToString(CultureInfo.InvariantCulture) ?? "-";

        static void Log(string line)
        {
            // PITFALL: never log the developer key, the user's password or account tokens.
            Console.WriteLine(line);
        }
    }

    /// <summary>A thread-safe set of account keys: handlers run on SocketIOClient's threads, the rest on the thread pool.</summary>
    sealed class KeySet
    {
        readonly HashSet<string> keys = [];
        public void Add(string key) { lock (keys) keys.Add(key); }
        public bool Remove(string key) { lock (keys) return keys.Remove(key); }
        public void Clear() { lock (keys) keys.Clear(); }
        public string[] ToArray() { lock (keys) return [.. keys]; }
    }
}
