// Types of the Streams API payloads (asyncapi/streams-api.asyncapi.yaml is the contract).
//
// PITFALL: numbers are decimal strings ("10628.95") and IDs are numeric strings that do not fit in
// a double ("144115188076017329"). Read numbers into decimal, never double or float, and keep IDs
// as strings.
//
// PITFALL: fields without a value are omitted, not null. New fields and message types can appear
// at any time: ignore what you do not know instead of rejecting the message.
using System.Text.Json;
using System.Text.Json.Serialization;

namespace TradeLocker.Streams.Example;

public static class Messages
{
    /// <summary>
    /// JSON settings for everything on the wire: camelCase names, case-insensitive matching, and
    /// numbers read from strings. The last one reads "9888.95" straight into a decimal: exact, with
    /// no double in between, and independent of the machine's culture. Unknown fields are ignored.
    /// </summary>
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
}

/// <summary>`connection` event payload, and `subscriptions` acknowledgement payload.</summary>
public sealed record StatusMessage(string Status, string Code, string? Message = null, int? RemainingRequests = null);

/// <summary>SUBSCRIBE takes exactly `action` and `token`; any other key fails validation.</summary>
public sealed record SubscribeAction(string Action, string Token);

/// <summary>UNSUBSCRIBE takes exactly `action`, `brandId` and `accountId`, matched case-sensitively.</summary>
public sealed record UnsubscribeAction(string Action, string BrandId, string AccountId);

/// <summary>Fields every `stream` message has. Read these first, then the full message by <see cref="Type"/>.</summary>
public record StreamMessage
{
    public string Type { get; init; } = "";
    public string BrandId { get; init; } = "";
    public string UserId { get; init; } = "";
    public string AccountId { get; init; } = "";
}

public sealed record AccountStatus : StreamMessage
{
    public string Currency { get; init; } = "";
    public decimal Balance { get; init; }
    public decimal BalanceWithoutCredit { get; init; }
    public decimal? Equity { get; init; }
    public decimal MarginAvailable { get; init; }
    public decimal MarginUsed { get; init; }
    public decimal BlockedBalance { get; init; }
    public decimal Credit { get; init; }
    public IReadOnlyList<PositionPnL> PositionPnLs { get; init; } = [];
}

public sealed record PositionPnL(string PositionId, decimal Pnl);

public sealed record Position : StreamMessage
{
    public string PositionId { get; init; } = "";
    public string Instrument { get; init; } = "";
    public string Side { get; init; } = ""; // BUY or SELL
    public decimal Lots { get; init; }
    public decimal LotSize { get; init; }
    public decimal Units { get; init; }
    public decimal OpenPrice { get; init; }
    public DateTimeOffset? OpenDateTime { get; init; }
    public string OpenOrderId { get; init; } = "";
    public string? StopLossOrderId { get; init; }
    public string? TakeProfitOrderId { get; init; }
    public decimal MaintMargin { get; init; }
}

public sealed record ClosePosition : StreamMessage
{
    public string PositionId { get; init; } = "";
    public decimal? ClosePrice { get; init; }
    public DateTimeOffset? CloseDateTime { get; init; }
}

public sealed record OpenOrder : StreamMessage
{
    public string OrderId { get; init; } = "";
    public string Instrument { get; init; } = "";
    public string? Side { get; init; }
    public decimal Amount { get; init; }
    public decimal LotSize { get; init; }
    public decimal? Price { get; init; }
    public string? Status { get; init; }
    public string? OrderType { get; init; }
    public string? Tif { get; init; }
    public decimal FilledAmount { get; init; }
    public decimal AverageFilledPrice { get; init; }
    public string? PositionId { get; init; }
    public bool IsOpen { get; init; }
    public DateTimeOffset? CreatedDateTime { get; init; }
    public DateTimeOffset? LastUpdate { get; init; }

    /// <summary>The other optional fields (stopPrice, slPrice, tpPrice, ...) and any new ones.</summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? OtherFields { get; init; }
}

/// <summary>Stream lifecycle marker for one account: `SyncEnd`, `Disconnected`, or a name added later.</summary>
public sealed record Property : StreamMessage
{
    public string Name { get; init; } = "";
}

// --- POST <backend>/auth/jwt/accounts/tokens ----------------------------------------------------

public sealed record TokensRequest(string Email, string Password, string Server)
{
    // PITFALL: records print every property in ToString(). Keep the password out of logs.
    public override string ToString() => $"TokensRequest {{ Email = {Email}, Server = {Server} }}";
}

public sealed record TokensResponse(IReadOnlyList<AccountToken> Data);

public sealed record AccountToken(string AccessToken, DateTimeOffset ExpireDate, string AccountId)
{
    public override string ToString() => $"AccountToken {{ AccountId = {AccountId}, ExpireDate = {ExpireDate:O} }}";
}

/// <summary>The JWT claims this example needs: `brand` is the brandId, `aid` the accountId.</summary>
public sealed record TokenClaims(string? Brand, string? Aid);
