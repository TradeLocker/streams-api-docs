"""Types of the Streams API payloads (asyncapi/streams-api.asyncapi.yaml is the contract).

PITFALL: numbers are decimal strings ("10628.95") and IDs are numeric strings
("144115188076017329"). Parse amounts with decimal.Decimal, never float(): binary floats do not
add up money exactly. Keep IDs as strings: they are identifiers, not numbers, and the server
matches them as text.

PITFALL: fields without a value are omitted, not null. Use message.get("field") or
"field" in message for the optional ones. New fields and message types can appear at any time:
ignore what you do not know instead of rejecting the message.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict, Union

if TYPE_CHECKING:
    # typing.NotRequired exists from Python 3.11 on. Imported for type checkers only, so that
    # Python 3.10 needs no extra package at run time.
    from typing_extensions import NotRequired

Decimal = str  # a decimal number as a string, e.g. "10628.95"; parse with decimal.Decimal


class StatusMessage(TypedDict):
    """`connection` event payload, and `subscriptions` acknowledgement payload."""

    status: Literal["ok", "error"]
    code: str
    message: str
    remainingRequests: NotRequired[int]


class SubscribeAction(TypedDict):
    """SUBSCRIBE takes exactly these keys; any other key fails validation (`unrecognized_keys`)."""

    action: Literal["SUBSCRIBE"]
    token: str


class UnsubscribeAction(TypedDict):
    """UNSUBSCRIBE takes exactly these keys. accountId looks like "L#123" or "D#123"."""

    action: Literal["UNSUBSCRIBE"]
    brandId: str
    accountId: str


SubscriptionAction = Union[SubscribeAction, UnsubscribeAction]


class PositionPnL(TypedDict):
    positionId: str
    pnl: Decimal


class AccountStatus(TypedDict):
    type: Literal["AccountStatus"]
    brandId: str
    userId: str
    accountId: str
    currency: str
    balance: Decimal
    balanceWithoutCredit: Decimal
    equity: NotRequired[Decimal]
    marginAvailable: Decimal
    marginUsed: Decimal
    blockedBalance: Decimal
    credit: Decimal
    positionPnLs: list[PositionPnL]


class Position(TypedDict):
    type: Literal["Position"]
    brandId: str
    userId: str
    accountId: str
    positionId: str
    instrument: str
    side: Literal["BUY", "SELL"]
    lots: Decimal
    lotSize: Decimal
    units: Decimal
    openPrice: Decimal
    openDateTime: NotRequired[str]
    openOrderId: str
    stopLossOrderId: NotRequired[str]
    takeProfitOrderId: NotRequired[str]
    maintMargin: Decimal


class ClosePosition(TypedDict):
    type: Literal["ClosePosition"]
    brandId: str
    userId: str
    accountId: str
    positionId: str
    closePrice: NotRequired[Decimal]
    closeDateTime: NotRequired[str]


class OpenOrder(TypedDict):
    type: Literal["OpenOrder"]
    brandId: str
    userId: str
    accountId: str
    orderId: str
    instrument: str
    side: NotRequired[Literal["BUY", "SELL"]]
    amount: Decimal
    lotSize: Decimal
    price: NotRequired[Decimal]
    status: NotRequired[str]
    orderType: NotRequired[str]
    tif: NotRequired[str]
    filledAmount: Decimal
    averageFilledPrice: Decimal
    positionId: NotRequired[str]
    isOpen: bool
    createdDateTime: NotRequired[str]
    lastUpdate: NotRequired[str]


class Property(TypedDict):
    type: Literal["Property"]
    brandId: str
    userId: str
    accountId: str
    name: str  # "SyncEnd", "Disconnected", or a name added later


StreamMessage = Union[AccountStatus, Position, ClosePosition, OpenOrder, Property]
