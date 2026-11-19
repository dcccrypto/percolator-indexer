/**
 * Bounded env parsing for the trade indexer and webhook route (built on envInt, strict decimal integers only).
 * Anything outside the bounds falls back to the default, so a typo can neither spin a timer, disable a limit or
 * alert, nor put a delivery deadline in the past.
 */
import { envInt } from "./envNumbers.js";

type Warn = (msg: string, ctx?: Record<string, unknown>) => void;

const MAX_TIMER_MS = 2 ** 31 - 1;
const DAY_MS = 86_400_000;

export const parseMaxWriteHolds = (raw: string | undefined, warn?: Warn): number =>
  envInt(raw, { name: "INDEXER_MAX_WRITE_HOLDS", fallback: 5, min: 1, max: 100, warn });

/** setInterval: below 1 s hammers Helius, above 2^31-1 Node fires every 1 ms. */
export const parsePollIntervalMs = (raw: string | undefined, warn?: Warn): number =>
  envInt(raw, { name: "TRADE_POLL_INTERVAL_MS", fallback: 5 * 60_000, min: 1000, max: MAX_TIMER_MS, warn });

export const parseWriteOutageAlertMs = (raw: string | undefined, warn?: Warn): number =>
  envInt(raw, { name: "INDEXER_WRITE_OUTAGE_ALERT_MS", fallback: 60_000, min: 1, max: DAY_MS, warn });

export const parseWriteOutageRealertMs = (raw: string | undefined, warn?: Warn): number =>
  envInt(raw, { name: "INDEXER_WRITE_OUTAGE_REALERT_MS", fallback: 10 * 60_000, min: 1, max: DAY_MS, warn });

/** 0 disables pacing (documented); capped at 60 s. */
export const parseRpcMinIntervalMs = (raw: string | undefined, warn?: Warn): number =>
  envInt(raw, { name: "TRADE_RPC_MIN_INTERVAL_MS", fallback: 125, min: 0, max: 60_000, warn });

export const parseDeliveryBudgetMs = (raw: string | undefined, fallback: number, warn?: Warn): number =>
  envInt(raw, { name: "TRADECPI_DELIVERY_BUDGET_MS", fallback, min: 100, max: 60_000, warn });
