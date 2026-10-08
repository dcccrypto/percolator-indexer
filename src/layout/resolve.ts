/**
 * VERSION-keyed account geometry for the indexer.
 *
 * The wrapper's account layout changes with its VERSION (u16 at account offset 8): v2.1 is VERSION 18
 * (group header 758 B, slot 2,325 B, portfolio 9,563 B, leg 152 B) and v2.2 is VERSION 19 (806 / 2,661 /
 * 10,603 / 217). Reading one with the other's offsets does not fail, it returns plausible garbage. So no
 * offset in the indexer is a literal any more: every market read goes through {@link readMarketGroupFields}
 * and every portfolio read through `layout/portfolio.ts`, both of which take their numbers from the SDK's
 * `LAYOUTS_BY_VERSION` table for the account's VERSION.
 *
 * An account whose VERSION the SDK does not know raises the SDK's `UnknownLayoutError`. Callers handle it
 * PER ACCOUNT with {@link reportUnknownLayout}: loud (error log, counter, Sentry once) and skipped for that
 * market only. It must never propagate out of a collect/discovery loop and wedge the other markets.
 */
import {
  ACCOUNT_KIND,
  UnknownLayoutError,
  WRAPPER_ACCOUNT_MAGIC,
  parseWrapperConfigV17,
  readWrapperHeader,
  resolveLayout,
  resolveMarketGeometry,
  V17_HEADER_LEN,
  type LayoutTable,
  type MarketGeometry,
} from "@percolatorct/sdk";
import { createLogger, captureException } from "@percolatorct/shared";

const logger = createLogger("indexer:layout");

/** True when the buffer carries the wrapper account magic (any VERSION). Legacy (v1) accounts return false. */
export function hasWrapperMagic(data: Uint8Array): boolean {
  if (data.length < 11) return false;
  return readWrapperHeader(data, "hasWrapperMagic").magic === WRAPPER_ACCOUNT_MAGIC;
}

/** True when the buffer is a wrapper-magic account of the given kind byte (any VERSION). */
export function isWrapperKind(data: Uint8Array, kind: number): boolean {
  return hasWrapperMagic(data) && data[10] === kind;
}

function u128(data: Uint8Array, off: number): bigint {
  const dv = new DataView(data.buffer, data.byteOffset + off, 16);
  return dv.getBigUint64(0, true) | (dv.getBigUint64(8, true) << 64n);
}

/** What the indexer reads from a market account's group header and slot 0. */
export interface MarketGroupFields {
  layout: LayoutTable;
  geometry: MarketGeometry;
  vault: bigint;
  insurance: bigint;
  cTot: bigint;
  materializedPortfolioCount: bigint;
  /** Absolute offset of asset slot 0 (where its AssetOracleProfile starts), or null when no whole slot is present. */
  asset0ProfileOff: number | null;
}

/**
 * Read the group header of a market account using the geometry of its VERSION.
 *
 * Length is deliberately NOT used to pick the layout (VERSION does); `strictLength: false` only means a market
 * whose tail is not a whole number of slots is still read, header first, instead of dropped.
 *
 * @throws UnknownLayoutError `TOO_SHORT`, `BAD_MAGIC`, `UNKNOWN_VERSION` or `WRONG_KIND`.
 */
export function readMarketGroupFields(data: Uint8Array, parser: string): MarketGroupFields {
  const geometry = resolveMarketGeometry(data, { parser, strictLength: false });
  const L = geometry.layout;
  const g = geometry.groupOff;
  return {
    layout: L,
    geometry,
    vault: u128(data, g + L.group.vault),
    insurance: u128(data, g + L.group.insurance),
    cTot: u128(data, g + L.group.cTot),
    materializedPortfolioCount: u128(data, g + L.group.materializedPortfolioCount),
    asset0ProfileOff: geometry.slotCount >= 1 ? geometry.slotOff(0) : null,
  };
}

/**
 * `mark_ewma_e6` of a wrapper market account (the fill-price proxy), VERSION-guarded: the wrapper config block
 * is the same 576 B in v2.1 and v2.2, but it is only read once the VERSION is known. Returns the raw e6 value.
 *
 * @throws UnknownLayoutError for an unknown VERSION / wrong kind.
 */
export function readMarkEwmaE6(data: Uint8Array, parser: string): bigint {
  resolveLayout(data, { parser, kind: ACCOUNT_KIND.Market });
  return parseWrapperConfigV17(data, V17_HEADER_LEN).markEwmaE6;
}

let unknownLayoutTotal = 0;
const reported = new Set<string>();

/** Counter metric `indexer_unknown_layout_total` (process lifetime). Alert on any increase. */
export function getUnknownLayoutCount(): number {
  return unknownLayoutTotal;
}
/** Test hook. */
export function resetUnknownLayoutState(): void {
  unknownLayoutTotal = 0;
  reported.clear();
}

/**
 * Report an account that was skipped because its layout is not known. Never throws.
 * Logged at error level EVERY time the code/VERSION combination is first seen for the account (then once per
 * process, so a 60 s collect loop does not spam), counted, and sent to Sentry once.
 *
 * @returns true when `err` was an UnknownLayoutError (handled), false for any other error (caller decides).
 */
export function reportUnknownLayout(account: string, err: unknown, parser: string): boolean {
  if (!(err instanceof UnknownLayoutError)) return false;
  unknownLayoutTotal++;
  const key = `${account}:${err.code}:${err.version ?? "?"}`;
  if (reported.has(key)) return true;
  reported.add(key);
  logger.error("SKIPPED account with an unknown layout: this market is NOT indexed until the SDK knows its VERSION", {
    metric: "indexer_unknown_layout_total",
    account,
    parser,
    code: err.code,
    version: err.version,
    error: err.message.slice(0, 200),
  });
  try {
    captureException(err, { tags: { context: "indexer-unknown-layout" }, extra: { account, parser, code: err.code, version: err.version } });
  } catch {
    /* alerting must not break ingestion */
  }
  return true;
}

export { ACCOUNT_KIND };
