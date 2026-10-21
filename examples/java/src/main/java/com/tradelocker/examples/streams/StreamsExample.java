// TradeLocker Streams API example for Java 21 (Socket.IO client io.socket:socket.io-client 2.1).
//
// What it does, and what a production integration must do too:
//   1. fetches one account token per account of a user (HTTP),
//   2. opens ONE Socket.IO connection with the developer key,
//   3. subscribes every account on that connection, and subscribes them again after every reconnect,
//   4. routes `stream` messages by brandId + accountId,
//   5. on Ctrl+C unsubscribes the accounts and closes the connection.
// Lines starting with PITFALL: mark mistakes we have seen in real integrations. Lines starting with
// QUIRK: mark behaviour of the Java Socket.IO client (io.socket 2.1) that this example works around.
//
// Configuration (environment variables):
//   TL_DEVELOPER_API_KEY  your developer API key (required)
//   TL_EMAIL, TL_PASSWORD, TL_SERVER   the user's TradeLocker login (required)
//   TL_BACKEND_URL   token endpoint base, default https://demo.tradelocker.com/backend-api
//                    (live accounts: https://live.tradelocker.com/backend-api)
//   TL_STREAMS_URL   Streams API base, default https://api.tradelocker.com
package com.tradelocker.examples.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.tradelocker.examples.streams.StreamMessage.AccountStatus;
import com.tradelocker.examples.streams.StreamMessage.ClosePosition;
import com.tradelocker.examples.streams.StreamMessage.OpenOrder;
import com.tradelocker.examples.streams.StreamMessage.Position;
import com.tradelocker.examples.streams.StreamMessage.Property;
import com.tradelocker.examples.streams.StreamMessage.Unknown;
import io.socket.client.AckWithTimeout;
import io.socket.client.IO;
import io.socket.client.Socket;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;
import org.json.JSONObject;

public final class StreamsExample {

  // PITFALL: a SUBSCRIBE includes the trade-node logon and the account snapshot and can take up to
  // about 60 s. A shorter acknowledgement timeout makes you retry subscriptions that are succeeding.
  private static final Duration SUBSCRIBE_ACK_TIMEOUT = Duration.ofSeconds(70);
  private static final Duration UNSUBSCRIBE_ACK_TIMEOUT = Duration.ofSeconds(10);
  private static final int MAX_SUBSCRIBES_IN_FLIGHT = 10;
  private static final Duration TOKEN_REFRESH_MARGIN = Duration.ofSeconds(60);
  private static final Duration MAX_BACKOFF = Duration.ofSeconds(60);
  private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);

  /** One account of the user. {@code key} is {@code brandId#accountId}, the same key the server uses. */
  record Account(String key, String brandId, String accountId, String token, Instant expiresAt) {
    @Override
    public String toString() { // without the token, see Config.toString()
      return "Account[" + key + ", expiresAt=" + expiresAt + "]";
    }
  }

  private record Claims(String brand, String aid) {}

  private final Config config;
  // HTTP/1.1: over plain http (the local mock), Java's HttpClient otherwise asks to upgrade to HTTP/2
  // ("Upgrade: h2c"), and a Node.js server with Socket.IO attached drops such requests after 1 s.
  private final HttpClient http = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  // PITFALL: io.socket runs every listener and acknowledgement callback on ONE thread of its own
  // (EventThread), the thread that also answers the server's pings. Never block it: no
  // Thread.sleep(), future.get(), HTTP calls or database writes in a listener. A blocked EventThread
  // stalls the stream of every account, and after 60 s without a pong the server closes the
  // connection. This example hands every event to its own single thread, `loop`, and schedules
  // retries with CompletableFuture.delayedExecutor() instead of sleeping. The fields below are read
  // and written on `loop` only, like the state of a Node.js program, so they need no locks.
  private final ExecutorService loop =
      Executors.newSingleThreadExecutor(Thread.ofPlatform().name("streams-example").daemon().factory());
  private final Socket socket;
  private Map<String, Account> accounts = Map.of();
  private CompletableFuture<Void> refreshing;
  private int generation; // bumped on every accepted connection; work for an older connection stops
  private int serverCloses; // connections the server closed in a row, for the reconnect backoff
  private boolean shuttingDown;
  private volatile boolean fatal; // the developer key was rejected: stop reconnecting
  private final Set<String> subscribed = new HashSet<>(); // accounts acknowledged on the current connection
  private final Set<String> awaitingSyncEnd = new HashSet<>();
  private final Set<String> unsubscribing = new HashSet<>();
  private CompletableFuture<Void> nextDisconnect = new CompletableFuture<>();
  private final CompletableFuture<Integer> fatalExit = new CompletableFuture<>();

  public static void main(String[] args) {
    System.exit(new StreamsExample(Config.fromEnv()).run());
  }

  private StreamsExample(Config config) {
    this.config = config;
    this.socket = createSocket();
  }

  /** Returns only on errors that retrying cannot fix. Ctrl+C and SIGTERM end the process in onShutdownSignal(). */
  private int run() {
    try {
      supplyOnLoop(this::refreshTokens).join();
    } catch (CompletionException e) {
      log("cannot get account tokens: " + describe(e));
      return 1;
    }
    Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdownSignal, "shutdown"));
    socket.connect();
    return fatalExit.join();
  }

  // --- Account tokens ------------------------------------------------------------------------

  private CompletableFuture<Void> refreshTokens() {
    // One refresh at a time, however many subscriptions ask for it.
    if (refreshing == null) {
      // PITFALL: this endpoint takes the user's credentials only. Do not send your developer key here.
      var body = new JSONObject()
          .put("email", config.email())
          .put("password", config.password())
          .put("server", config.server());
      var request = HttpRequest.newBuilder(URI.create(config.backendUrl() + "/auth/jwt/accounts/tokens"))
          .header("content-type", "application/json")
          .timeout(Duration.ofSeconds(30))
          .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
          .build();
      refreshing = http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .thenAcceptAsync(this::onTokens, loop)
          .whenCompleteAsync((ignored, error) -> refreshing = null, loop);
    }
    return refreshing;
  }

  private void onTokens(HttpResponse<String> response) {
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException("account tokens request failed: HTTP " + response.statusCode());
    }
    var data = new JSONObject(response.body()).getJSONArray("data");
    var next = new LinkedHashMap<String, Account>();
    for (int i = 0; i < data.length(); i++) {
      var item = data.getJSONObject(i);
      var token = item.getString("accessToken");
      // PITFALL: the response has no brandId, and the brand the user typed ("Blueg") is not
      // necessarily the brand the server uses ("BLUEG"). Take brand and account from the token's
      // claims: they are exactly what the server matches UNSUBSCRIBE and stream messages against.
      var claims = jwtClaims(token);
      var key = claims.brand() + "#" + claims.aid();
      next.put(key, new Account(key, claims.brand(), claims.aid(), token, Instant.parse(item.getString("expireDate"))));
    }
    accounts = next;
    log("tokens for " + next.size() + " account(s): " + String.join(", ", next.keySet()));
  }

  /** Like refreshTokens(), for retries: logs a failure instead of failing. True when the tokens are new. */
  private CompletableFuture<Boolean> tryRefreshTokens() {
    return refreshTokens().handleAsync((ignored, error) -> {
      if (error != null) log("cannot refresh account tokens: " + describe(error));
      return error == null;
    }, loop);
  }

  /** Reads the JWT payload. No signature check needed: the Streams API verifies the token. */
  private static Claims jwtClaims(String token) {
    var parts = token.split("\\.");
    var payload = new JSONObject(new String(Base64.getUrlDecoder().decode(parts.length > 1 ? parts[1] : ""), UTF_8));
    if (!(payload.opt("brand") instanceof String brand) || !(payload.opt("aid") instanceof String aid)) {
      throw new IllegalStateException("token has no brand/aid claims");
    }
    return new Claims(brand, aid);
  }

  // --- Connection ----------------------------------------------------------------------------

  private Socket createSocket() {
    var options = IO.Options.builder()
        .setPath("/streams-api/socket.io")
        // PITFALL: the server accepts the websocket transport only; HTTP long-polling is rejected.
        .setTransports(new String[] {"websocket"})
        .setExtraHeaders(Map.of("developer-api-key", List.of(config.developerApiKey())))
        .setReconnectionDelay(1_000)
        .setReconnectionDelayMax(30_000)
        .setRandomizationFactor(0.5)
        .build();
    // io.socket's own OkHttp settings (1 min read timeout), plus the close-frame fix below.
    options.webSocketFactory = answeringCloseFrames(new OkHttpClient.Builder().readTimeout(1, TimeUnit.MINUTES).build());
    // PITFALL: one connection carries all accounts. Do not open a connection per account: connections
    // per developer key are capped, and above the cap the handshake is rejected with `conflict`.
    var socket = IO.socket(URI.create(config.streamsUrl() + "/streams-api"), options);

    // Every listener below runs on io.socket's EventThread and only hands the event to `loop`.
    socket.on(Socket.EVENT_CONNECT, args -> {
      var id = socket.id();
      runOnLoop(() -> log("socket connected id=" + id));
    });
    socket.on(Socket.EVENT_CONNECT_ERROR, args -> runOnLoop(() -> log("socket connect error: " + describe(args))));
    socket.on(Socket.EVENT_DISCONNECT, args -> runOnLoop(() -> onDisconnect(args.length > 0 ? String.valueOf(args[0]) : "")));
    // The server answers every handshake on the `connection` event. Subscribe only after `connected`.
    socket.on("connection", args -> runOnLoop(() -> onConnectionStatus(StatusMessage.from(args))));
    socket.on("stream", args -> runOnLoop(() -> onStream(args)));
    return socket;
  }

  /**
   * QUIRK: engine.io-client 2.1 does not answer a websocket close frame from the server (it leaves
   * OkHttp's onClosing() empty). When the server, or a proxy in front of it, closes the connection
   * that way, the client notices only at its ping timeout, about 61 s later, and the accounts stay
   * silent until then. Answering the frame ends the connection at once ("transport close"), and the
   * client reconnects right away.
   */
  private static WebSocket.Factory answeringCloseFrames(OkHttpClient client) {
    return (request, listener) -> client.newWebSocket(request, new WebSocketListener() {
      @Override
      public void onOpen(WebSocket webSocket, Response response) {
        listener.onOpen(webSocket, response);
      }

      @Override
      public void onMessage(WebSocket webSocket, String text) {
        listener.onMessage(webSocket, text);
      }

      @Override
      public void onMessage(WebSocket webSocket, ByteString bytes) {
        listener.onMessage(webSocket, bytes);
      }

      @Override
      public void onClosing(WebSocket webSocket, int code, String reason) {
        webSocket.close(1000, null); // OkHttp then calls onClosed()
        listener.onClosing(webSocket, code, reason);
      }

      @Override
      public void onClosed(WebSocket webSocket, int code, String reason) {
        listener.onClosed(webSocket, code, reason);
      }

      @Override
      public void onFailure(WebSocket webSocket, Throwable t, Response response) {
        listener.onFailure(webSocket, t, response);
      }
    });
  }

  // PITFALL: subscriptions belong to the connection. After any disconnect, the client reconnects
  // with a NEW connection that has no subscriptions, and nothing arrives until you subscribe again.
  private void onDisconnect(String reason) {
    subscribed.clear();
    awaitingSyncEnd.clear();
    log("socket disconnected reason=" + reason);
    nextDisconnect.complete(null);
    nextDisconnect = new CompletableFuture<>();
    // PITFALL: io.socket reconnects by itself after network errors and ping timeouts, but NOT when
    // the server closed the connection ("io server disconnect"), for example on a deploy or after it
    // rejected the handshake. Without this, the process keeps running and every account goes silent.
    // The delay grows while the server keeps closing the connection (a `conflict`, for example).
    if (reason.equals("io server disconnect") && !shuttingDown && !fatal) {
      reportFailures(later(backoff(serverCloses++), () -> {
        if (!shuttingDown && !fatal) socket.connect();
        return DONE;
      }));
    }
  }

  private void onConnectionStatus(StatusMessage status) {
    if (status.ok() && status.code().equals("connected")) {
      log("connection ok");
      serverCloses = 0;
      int current = ++generation;
      reportFailures(subscribeAll(current));
      return;
    }
    if (status.ok() && status.code().equals("disconnected")) {
      log("server is shutting down this connection; the client will reconnect");
      return;
    }
    log("connection error code=" + status.code() + " message=" + status.message());
    if (status.code().startsWith("unauthorized.")) {
      // PITFALL: a wrong or unknown developer key never starts working by retrying. Stop instead of
      // reconnecting in a loop (that loop is visible to us, and rejected handshakes are throttled).
      fatal = true;
      socket.io().reconnection(false);
      socket.disconnect();
      fatalExit.complete(1);
    }
    // `conflict` (too many connections for the key) and `unknown`: the server closes the socket and
    // the client retries with backoff. Look for connections you leaked elsewhere.
  }

  // --- Subscriptions -------------------------------------------------------------------------

  private CompletableFuture<Void> subscribeAll(int gen) {
    var queue = new ArrayDeque<>(accounts.keySet());
    int workers = Math.min(MAX_SUBSCRIBES_IN_FLIGHT, queue.size());
    var running = new ArrayList<CompletableFuture<Void>>();
    for (int i = 0; i < workers; i++) running.add(subscribeNext(queue, gen));
    return CompletableFuture.allOf(running.toArray(CompletableFuture[]::new));
  }

  /** One worker of subscribeAll(): subscribes the next account of the queue until none is left. */
  private CompletableFuture<Void> subscribeNext(Queue<String> queue, int gen) {
    var key = queue.poll();
    if (key == null) return DONE;
    return subscribe(key, gen, 0).thenComposeAsync(ignored -> subscribeNext(queue, gen), loop);
  }

  /** Subscribes one account, retrying with backoff. Completes when it is subscribed or given up. */
  private CompletableFuture<Void> subscribe(String key, int gen, int attempt) {
    var account = accounts.get(key);
    if (isStale(gen) || account == null) return DONE;
    // PITFALL: a SUBSCRIBE needs a token that has not expired. Tokens are short-lived: fetch new ones
    // before you subscribe again after a reconnect. (Once subscribed, the stream does not depend on
    // the token any more.)
    if (account.expiresAt().isBefore(Instant.now().plus(TOKEN_REFRESH_MARGIN))) {
      return tryRefreshTokens().thenComposeAsync(
          refreshed -> refreshed ? sendSubscribe(key, gen, attempt) : retryLater(key, gen, attempt), loop);
    }
    return sendSubscribe(key, gen, attempt);
  }

  private CompletableFuture<Void> sendSubscribe(String key, int gen, int attempt) {
    var account = accounts.get(key);
    // QUIRK: io.socket queues what you emit while disconnected and sends it after the next connect,
    // on a connection it was not meant for. Check the connection right before you emit.
    if (isStale(gen) || account == null) return DONE;

    // PITFALL: SyncEnd for this account can arrive BEFORE the acknowledgement of this SUBSCRIBE.
    // Start waiting for it before you emit, or you will miss it.
    awaitingSyncEnd.add(key);

    // SUBSCRIBE takes exactly { action, token }; any other key fails validation.
    var subscribe = new JSONObject().put("action", "SUBSCRIBE").put("token", account.token());
    return emitWithAck(subscribe, SUBSCRIBE_ACK_TIMEOUT)
        .thenComposeAsync(ack -> onSubscribeAck(key, gen, attempt, ack), loop);
  }

  private CompletableFuture<Void> onSubscribeAck(String key, int gen, int attempt, StatusMessage ack) {
    log("subscribe " + key + " -> " + ack.code());
    return switch (ack.code()) {
      case "connected", "alreadyConnectedError" -> { // already subscribed on this connection: nothing to do
        if (gen == generation) subscribed.add(key);
        yield DONE;
      }
      case "clientNotFound" -> DONE; // the connection is going away; the reconnect subscribes again
      case "invalidJwt" -> tryRefreshTokens().thenComposeAsync(refreshed -> retryLater(key, gen, attempt), loop);
      case String code when code.startsWith("validationError") -> {
        log("bug in this client, not retrying: " + ack.message());
        yield DONE;
      }
      // tooManyRequests, connectionError, unknown, ackTimeout: retry with backoff
      default -> retryLater(key, gen, attempt);
    };
  }

  private CompletableFuture<Void> retryLater(String key, int gen, int attempt) {
    return later(backoff(attempt), () -> subscribe(key, gen, attempt + 1));
  }

  private CompletableFuture<Void> unsubscribe(String key) {
    var account = accounts.get(key);
    if (account == null) return DONE;
    unsubscribing.add(key);
    // PITFALL: brandId and accountId must match the subscription exactly, case included.
    // Use the values from the token claims (or from stream messages), never the user's input.
    var unsubscribe = new JSONObject()
        .put("action", "UNSUBSCRIBE")
        .put("brandId", account.brandId())
        .put("accountId", account.accountId());
    return emitWithAck(unsubscribe, UNSUBSCRIBE_ACK_TIMEOUT)
        .thenAcceptAsync(ack -> log("unsubscribe " + key + " -> " + ack.code()), loop);
  }

  /**
   * Emits a {@code subscriptions} action and completes with its acknowledgement, or with
   * ACK_TIMEOUT. Completes on an io.socket thread: continue with a {@code ...Async(..., loop)} stage.
   */
  private CompletableFuture<StatusMessage> emitWithAck(JSONObject action, Duration timeout) {
    var result = new CompletableFuture<StatusMessage>();
    // PITFALL: always ask for the acknowledgement. It is the only place the result is reported.
    socket.emit("subscriptions", new Object[] {action}, new AckWithTimeout(timeout.toMillis()) {
      @Override
      public void onSuccess(Object... args) {
        result.complete(StatusMessage.from(args));
      }

      @Override
      public void onTimeout() {
        // QUIRK: called by the timer, and also at once for every pending acknowledgement when the
        // connection closes. In that second case io.socket 2.1 leaves the timer running, and it would
        // call onTimeout() again later and keep its (non-daemon) timer thread alive until then.
        cancelTimer();
        result.complete(StatusMessage.ACK_TIMEOUT);
      }
    });
    return result;
  }

  /** True when work started for connection {@code gen} must stop: that connection is gone, or we are stopping. */
  private boolean isStale(int gen) {
    return gen != generation || !socket.connected() || shuttingDown;
  }

  // --- Stream --------------------------------------------------------------------------------

  private void onStream(Object... args) {
    if (args.length == 0 || !(args[0] instanceof JSONObject json)) return;
    StreamMessage message;
    try {
      message = StreamMessage.parse(json);
    } catch (RuntimeException e) {
      log("ignoring malformed stream message: " + e.getMessage());
      return;
    }
    // Every message carries brandId and accountId: one connection, many accounts.
    var key = message.key();
    switch (message) {
      case Property p when p.name().equals("SyncEnd") -> {
        awaitingSyncEnd.remove(key);
        log("sync-end " + key);
      }
      case Property p when p.name().equals("Disconnected") -> {
        subscribed.remove(key);
        if (shuttingDown || unsubscribing.remove(key)) {
          log("stream ended " + key + " (unsubscribed)");
        } else {
          // The server ended this one account (for example its trading session was closed).
          // Subscribe that account again. PITFALL: do not restart the whole connection for it.
          log("account-disconnected " + key);
          int current = generation;
          reportFailures(later(Duration.ofSeconds(1), () -> subscribe(key, current, 0)));
        }
      }
      case Property p -> {} // other Property names: ignore
      case AccountStatus s -> log("AccountStatus %s balance=%s equity=%s %s"
          .formatted(key, plain(s.balance()), plain(s.equity()), s.currency()));
      case Position p -> log("Position %s %s %s %s %s @ %s"
          .formatted(key, p.positionId(), p.side(), plain(p.lots()), p.instrument(), plain(p.openPrice())));
      case ClosePosition c -> log("ClosePosition %s %s @ %s".formatted(key, c.positionId(), plain(c.closePrice())));
      case OpenOrder o -> log("OpenOrder %s %s %s %s %s %s"
          .formatted(key, o.orderId(), orDash(o.status()), orDash(o.side()), plain(o.amount()), o.instrument()));
      case Unknown u -> {} // PITFALL: new message types can be added. Ignore what you do not know.
    }
    // PITFALL: keep this handler fast. It runs on `loop`, so a slow one delays every later event of
    // every account. Hand heavy work (database writes, strategies) to a queue or executor of its own.
  }

  // --- Shutdown ------------------------------------------------------------------------------

  /** Runs on the JVM's shutdown-hook thread: after Ctrl+C (SIGINT), SIGTERM or System.exit(). */
  private void onShutdownSignal() {
    if (!fatal) {
      try {
        supplyOnLoop(this::shutdown).get(UNSUBSCRIBE_ACK_TIMEOUT.toMillis() + 2_000, MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log("shutdown: not finished cleanly: " + describe(e));
      }
    }
    System.out.flush();
    // PITFALL: a JVM stopped by a signal exits with status 128 + signal number (130 for Ctrl+C, 143
    // for SIGTERM) even when every hook ran fine, so a clean stop looks like a crash to whatever
    // started it. halt() ends the process with the status we choose. It does not wait for other
    // shutdown hooks: if your application has more (logging, metrics), stop those before this line.
    Runtime.getRuntime().halt(fatal ? 1 : 0);
  }

  /** Runs on `loop`. Completes when the connection is closed. */
  private CompletableFuture<Void> shutdown() {
    shuttingDown = true;
    log("shutdown: unsubscribing " + subscribed.size() + " account(s)");
    socket.io().reconnection(false);
    // Closing the connection ends every subscription on it anyway; unsubscribing first is shown
    // here because it is how you stop a single account while keeping the others.
    var unsubscribes = new ArrayList<CompletableFuture<Void>>();
    if (socket.connected()) {
      for (var key : List.copyOf(subscribed)) unsubscribes.add(unsubscribe(key));
    }
    return CompletableFuture.allOf(unsubscribes.toArray(CompletableFuture[]::new)).thenComposeAsync(ignored -> {
      var closed = socket.connected() ? nextDisconnect : DONE;
      socket.disconnect();
      return closed;
    }, loop);
  }

  // --- Helpers -------------------------------------------------------------------------------

  /** Runs {@code task} on `loop`. A failure is logged: executors would otherwise drop it or kill their thread. */
  private void runOnLoop(Runnable task) {
    loop.execute(() -> {
      try {
        task.run();
      } catch (RuntimeException e) {
        log("unexpected error: " + e);
      }
    });
  }

  /** Runs {@code work} on `loop` and completes with its result. */
  private <T> CompletableFuture<T> supplyOnLoop(Supplier<CompletableFuture<T>> work) {
    return CompletableFuture.supplyAsync(work, loop).thenCompose(future -> future);
  }

  /** Runs {@code next} on `loop` after {@code delay}. No thread waits in the meantime. */
  private <T> CompletableFuture<T> later(Duration delay, Supplier<CompletableFuture<T>> next) {
    var delayed = CompletableFuture.delayedExecutor(delay.toMillis(), MILLISECONDS, loop);
    return CompletableFuture.supplyAsync(next, delayed).thenCompose(future -> future);
  }

  /** For work nobody waits for: makes an unexpected failure visible instead of losing it. */
  private static void reportFailures(CompletableFuture<?> work) {
    work.whenComplete((ignored, error) -> {
      if (error != null) log("unexpected error: " + describe(error));
    });
  }

  /** Exponential backoff with jitter: about 1 s, 2 s, 4 s, ..., never more than 60 s. */
  private static Duration backoff(int attempt) {
    double millis = 1_000 * Math.pow(2, Math.min(attempt, 16)) * (0.5 + ThreadLocalRandom.current().nextDouble());
    return Duration.ofMillis((long) Math.min(millis, MAX_BACKOFF.toMillis()));
  }

  private static String plain(BigDecimal value) {
    return value == null ? "-" : value.toPlainString();
  }

  private static String orDash(String value) {
    return value == null ? "-" : value;
  }

  /** A short description of an error or of io.socket's connect_error argument. */
  private static String describe(Object... args) {
    Object error = args.length > 0 ? args[0] : null;
    while (error instanceof CompletionException || error instanceof ExecutionException) {
      error = ((Throwable) error).getCause();
    }
    if (error instanceof Throwable t) {
      var text = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
      return t.getCause() != null && t.getCause() != t ? text + " (" + describe(t.getCause()) + ")" : text;
    }
    return String.valueOf(error);
  }

  private static void log(String line) {
    // PITFALL: never log the developer key, the user's password or account tokens.
    System.out.println(line);
  }

  /** Configuration from environment variables (see README.md). */
  record Config(String developerApiKey, String email, String password, String server, String backendUrl, String streamsUrl) {

    static Config fromEnv() {
      return new Config(
          required("TL_DEVELOPER_API_KEY"),
          required("TL_EMAIL"),
          required("TL_PASSWORD"),
          required("TL_SERVER"),
          optional("TL_BACKEND_URL", "https://demo.tradelocker.com/backend-api"),
          optional("TL_STREAMS_URL", "https://api.tradelocker.com"));
    }

    // PITFALL: a record's generated toString() prints every component, the developer key and the
    // password included. Override it before the record ends up in a log line or an exception message.
    @Override
    public String toString() {
      return "Config[server=%s, backendUrl=%s, streamsUrl=%s]".formatted(server, backendUrl, streamsUrl);
    }

    private static String required(String name) {
      var value = System.getenv(name);
      if (value == null || value.isEmpty()) {
        System.err.println("Set " + name + " (see README.md).");
        System.exit(2);
      }
      return value;
    }

    private static String optional(String name, String fallback) {
      var value = System.getenv(name);
      return (value == null || value.isEmpty() ? fallback : value).replaceAll("/+$", "");
    }
  }
}
