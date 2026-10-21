// Types of the Streams API payloads (asyncapi/streams-api.asyncapi.yaml is the contract).
//
// PITFALL: numbers are decimal strings ("10628.95") and IDs are numeric strings that do not fit in
// a double ("144115188076017329"). Keep them as strings, or use a decimal library (decimal.js,
// big.js) for arithmetic. Never parseFloat() money or Number() an ID.
//
// PITFALL: fields without a value are omitted, not null. New fields and message types can appear
// at any time: ignore what you do not know instead of rejecting the message.

export type Decimal = string;

/** `connection` event payload, and `subscriptions` acknowledgement payload. */
export interface StatusMessage {
  status: "ok" | "error";
  code: string;
  message: string;
  remainingRequests?: number;
}

interface StreamBase {
  brandId: string;
  userId: string;
  accountId: string;
}

export interface AccountStatus extends StreamBase {
  type: "AccountStatus";
  currency: string;
  balance: Decimal;
  balanceWithoutCredit: Decimal;
  equity?: Decimal;
  marginAvailable: Decimal;
  marginUsed: Decimal;
  blockedBalance: Decimal;
  credit: Decimal;
  positionPnLs: { positionId: string; pnl: Decimal }[];
}

export interface Position extends StreamBase {
  type: "Position";
  positionId: string;
  instrument: string;
  side: "BUY" | "SELL";
  lots: Decimal;
  lotSize: Decimal;
  units: Decimal;
  openPrice: Decimal;
  openDateTime?: string;
  openOrderId: string;
  stopLossOrderId?: string;
  takeProfitOrderId?: string;
  maintMargin: Decimal;
}

export interface ClosePosition extends StreamBase {
  type: "ClosePosition";
  positionId: string;
  closePrice?: Decimal;
  closeDateTime?: string;
}

export interface OpenOrder extends StreamBase {
  type: "OpenOrder";
  orderId: string;
  instrument: string;
  side?: "BUY" | "SELL";
  amount: Decimal;
  lotSize: Decimal;
  price?: Decimal;
  status?: string;
  orderType?: string;
  tif?: string;
  filledAmount: Decimal;
  averageFilledPrice: Decimal;
  positionId?: string;
  isOpen: boolean;
  createdDateTime?: string;
  lastUpdate?: string;
  [field: string]: unknown;
}

export interface Property extends StreamBase {
  type: "Property";
  name: "SyncEnd" | "Disconnected" | (string & {});
}

export type StreamMessage = AccountStatus | Position | ClosePosition | OpenOrder | Property;
