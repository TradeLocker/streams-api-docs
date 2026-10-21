package com.tradelocker.examples.streams;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Payloads of the {@code stream} event (asyncapi/streams-api.asyncapi.yaml is the contract).
 * One event carries one message; {@code type} says which.
 *
 * <p>PITFALL: numbers are decimal strings ("10628.95") and IDs are numeric strings above 2^53
 * ("144115188076017329"), which a double silently rounds. Read amounts into {@link BigDecimal}
 * and keep IDs as {@link String}. Never {@code Double.parseDouble()} money or an ID.
 *
 * <p>PITFALL: fields without a value are omitted, not sent as null. Optional fields are
 * {@code null} in these records. New fields and message types can appear at any time: ignore what
 * you do not know instead of rejecting the message ({@link Unknown}).
 */
public sealed interface StreamMessage {

  String brandId();

  String userId();

  String accountId();

  /** Routing key, the same one the server uses: {@code brandId#accountId}, for example {@code BAPIA#L#737031}. */
  default String key() {
    return brandId() + "#" + accountId();
  }

  /** Balance, equity, margin and per-position PnL of one account. {@code equity} is optional. */
  record AccountStatus(
      String brandId, String userId, String accountId,
      String currency,
      BigDecimal balance, BigDecimal balanceWithoutCredit, BigDecimal equity,
      BigDecimal marginAvailable, BigDecimal marginUsed, BigDecimal blockedBalance, BigDecimal credit,
      List<PositionPnL> positionPnLs) implements StreamMessage {}

  record PositionPnL(String positionId, BigDecimal pnl) {}

  /** An open position. {@code openDateTime}, {@code stopLossOrderId} and {@code takeProfitOrderId} are optional. */
  record Position(
      String brandId, String userId, String accountId,
      String positionId, String instrument, String side,
      BigDecimal lots, BigDecimal lotSize, BigDecimal units, BigDecimal openPrice, Instant openDateTime,
      String openOrderId, String stopLossOrderId, String takeProfitOrderId,
      BigDecimal maintMargin) implements StreamMessage {}

  /** A position was closed. {@code closePrice} and {@code closeDateTime} are optional. */
  record ClosePosition(
      String brandId, String userId, String accountId,
      String positionId, BigDecimal closePrice, Instant closeDateTime) implements StreamMessage {}

  /** An order. Only orderId, instrument, amount, lotSize, filledAmount, averageFilledPrice and isOpen are always present. */
  record OpenOrder(
      String brandId, String userId, String accountId,
      String orderId, String instrument, String side,
      BigDecimal amount, BigDecimal lotSize, BigDecimal price,
      String status, String orderType, String tif,
      BigDecimal filledAmount, BigDecimal averageFilledPrice,
      BigDecimal stopPrice, BigDecimal slPrice, String slPriceType, BigDecimal slLimitPrice,
      BigDecimal tpPrice, String tpPriceType, BigDecimal trStopOffset,
      String positionId, boolean isOpen,
      Instant createdDateTime, Instant lastUpdate, Instant expireDateTime) implements StreamMessage {}

  /** {@code SyncEnd}: snapshot complete, live updates follow. {@code Disconnected}: this account's stream ended. */
  record Property(String brandId, String userId, String accountId, String name) implements StreamMessage {}

  /** A message type this example does not know (yet). */
  record Unknown(String brandId, String userId, String accountId, String type) implements StreamMessage {}

  /** Reads one {@code stream} payload. Throws {@link org.json.JSONException} when a required field is missing. */
  static StreamMessage parse(JSONObject m) {
    String brandId = m.getString("brandId");
    String userId = m.optString("userId", null);
    String accountId = m.getString("accountId");
    return switch (m.optString("type")) {
      case "AccountStatus" -> new AccountStatus(brandId, userId, accountId,
          m.getString("currency"),
          decimal(m, "balance"), decimal(m, "balanceWithoutCredit"), optDecimal(m, "equity"),
          decimal(m, "marginAvailable"), decimal(m, "marginUsed"), decimal(m, "blockedBalance"), decimal(m, "credit"),
          positionPnLs(m.getJSONArray("positionPnLs")));
      case "Position" -> new Position(brandId, userId, accountId,
          m.getString("positionId"), m.getString("instrument"), m.getString("side"),
          decimal(m, "lots"), decimal(m, "lotSize"), decimal(m, "units"), decimal(m, "openPrice"), optInstant(m, "openDateTime"),
          m.getString("openOrderId"), optText(m, "stopLossOrderId"), optText(m, "takeProfitOrderId"),
          decimal(m, "maintMargin"));
      case "ClosePosition" -> new ClosePosition(brandId, userId, accountId,
          m.getString("positionId"), optDecimal(m, "closePrice"), optInstant(m, "closeDateTime"));
      case "OpenOrder" -> new OpenOrder(brandId, userId, accountId,
          m.getString("orderId"), m.getString("instrument"), optText(m, "side"),
          decimal(m, "amount"), decimal(m, "lotSize"), optDecimal(m, "price"),
          optText(m, "status"), optText(m, "orderType"), optText(m, "tif"),
          decimal(m, "filledAmount"), decimal(m, "averageFilledPrice"),
          optDecimal(m, "stopPrice"), optDecimal(m, "slPrice"), optText(m, "slPriceType"), optDecimal(m, "slLimitPrice"),
          optDecimal(m, "tpPrice"), optText(m, "tpPriceType"), optDecimal(m, "trStopOffset"),
          optText(m, "positionId"), m.getBoolean("isOpen"),
          optInstant(m, "createdDateTime"), optInstant(m, "lastUpdate"), optInstant(m, "expireDateTime"));
      case "Property" -> new Property(brandId, userId, accountId, m.getString("name"));
      default -> new Unknown(brandId, userId, accountId, m.optString("type"));
    };
  }

  private static List<PositionPnL> positionPnLs(JSONArray items) {
    var pnls = new ArrayList<PositionPnL>(items.length());
    for (int i = 0; i < items.length(); i++) {
      var item = items.getJSONObject(i);
      pnls.add(new PositionPnL(item.getString("positionId"), decimal(item, "pnl")));
    }
    return List.copyOf(pnls);
  }

  private static BigDecimal decimal(JSONObject m, String field) {
    return new BigDecimal(m.getString(field));
  }

  private static BigDecimal optDecimal(JSONObject m, String field) {
    return m.isNull(field) ? null : decimal(m, field);
  }

  private static String optText(JSONObject m, String field) {
    return m.isNull(field) ? null : m.getString(field);
  }

  private static Instant optInstant(JSONObject m, String field) {
    return m.isNull(field) ? null : Instant.parse(m.getString(field));
  }
}
