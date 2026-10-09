/**
 * Real-shaped fixtures for the v2.2 tests.
 *
 *  - v2.1 (VERSION 18) accounts are REAL devnet captures (tests/fixtures/sdk/*.json, copied from the SDK repo).
 *  - v2.2 (VERSION 19) accounts are stamped from the SDK's LAYOUT_V22 table (no v2.2 account exists on any cluster yet).
 *  - Instructions are built by the SDK's own builders (`build*IxV22`) and converted to the two wire shapes the
 *    indexer ingests (jsonParsed / Atlas, and Helius enhanced).
 */
import { readFileSync } from "node:fs";
import { Keypair, PublicKey, type TransactionInstruction } from "@solana/web3.js";
import { LAYOUT_V21, LAYOUT_V22, type LayoutTable } from "@percolatorct/sdk";
import { encodeBase58 } from "../../src/lib/base58.js";

export const REAL_PORTFOLIO_V21 = new Uint8Array(Buffer.from(JSON.parse(readFileSync(new URL("../fixtures/sdk/portfolio-v18-active-leg.json", import.meta.url), "utf8")).dataBase64, "base64"));
export const REAL_MARKET_V21 = new Uint8Array(Buffer.from(JSON.parse(readFileSync(new URL("../fixtures/sdk/market-v18-live-budgets.json", import.meta.url), "utf8")).dataBase64, "base64"));

export const pk = (): PublicKey => Keypair.generate().publicKey;

export function stampHeader(buf: Uint8Array, kind: number, version: number, discriminator?: number): Uint8Array {
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  v.setBigUint64(0, 0x5045_5243_5631_3600n, true);
  v.setUint16(8, version, true);
  buf[10] = kind;
  if (kind === 2) {
    v.setUint16(112, 1, true);
    v.setUint16(114, discriminator ?? version, true);
  }
  return buf;
}

export function put128(d: Uint8Array, o: number, v: bigint): void {
  const x = new DataView(d.buffer, d.byteOffset, d.byteLength);
  x.setBigUint64(o, v & ((1n << 64n) - 1n), true);
  x.setBigUint64(o + 8, v >> 64n, true);
}

export interface MarketSpec {
  slots?: number;
  vault?: bigint;
  insurance?: bigint;
  cTot?: bigint;
  materialized?: bigint;
  initialMarginBps?: bigint;
  markEwmaE6?: bigint;
  oracleAuthority?: PublicKey;
}

/** A market account of `layout` (default v2.2), stamped from the SDK table. */
export function buildMarket(layout: LayoutTable = LAYOUT_V22, spec: MarketSpec = {}, version: number = layout.version): Uint8Array {
  const slots = spec.slots ?? 2;
  const d = stampHeader(new Uint8Array(layout.marketGroupOff + layout.marketGroupLen + slots * layout.assetSlotStride), 1, version);
  const g = layout.marketGroupOff;
  if (spec.vault !== undefined) put128(d, g + layout.group.vault, spec.vault);
  if (spec.insurance !== undefined) put128(d, g + layout.group.insurance, spec.insurance);
  if (spec.cTot !== undefined) put128(d, g + layout.group.cTot, spec.cTot);
  if (spec.materialized !== undefined) put128(d, g + layout.group.materializedPortfolioCount, spec.materialized);
  const dv = new DataView(d.buffer);
  if (spec.initialMarginBps !== undefined) dv.setBigUint64(g + layout.group.config + 62, spec.initialMarginBps, true);
  // WrapperConfigV17.mark_ewma_e6 sits at config offset 232 (account offset 16 + 232), identical in both layouts.
  if (spec.markEwmaE6 !== undefined) dv.setBigUint64(16 + 232, spec.markEwmaE6, true);
  if (spec.oracleAuthority) d.set(spec.oracleAuthority.toBytes(), layout.marketGroupOff + layout.marketGroupLen + 120);
  return d;
}

export interface LegSpec { index: number; assetIndex: number; marketId: bigint; side: 0 | 1; basisPosQ: bigint; }

/** A portfolio of `layout` with the given active legs, stamped from the SDK table (exact PORTFOLIO_ACCOUNT_LEN). */
export function buildPortfolio(layout: LayoutTable = LAYOUT_V22, legs: LegSpec[] = [], owner: PublicKey = pk()): Uint8Array {
  const d = stampHeader(new Uint8Array(layout.portfolio.accountLen), 2, layout.version);
  d.set(owner.toBytes(), 116);
  const g = layout.portfolio;
  const dv = new DataView(d.buffer);
  for (const l of legs) {
    const b = g.legsOff + l.index * g.legStride;
    d[b + g.leg.active] = 1;
    dv.setUint32(b + g.leg.assetIndex, l.assetIndex, true);
    dv.setBigUint64(b + g.leg.marketId, l.marketId, true);
    d[b + g.leg.side] = l.side;
    put128(d, b + g.leg.basisPosQ, l.basisPosQ);
  }
  return d;
}

export { LAYOUT_V21, LAYOUT_V22, encodeBase58 };

/** A jsonParsed-shaped transaction (getTransaction / Atlas transactionSubscribe). */
export function parsedTx(ixs: TransactionInstruction[], opts: { err?: unknown; inner?: TransactionInstruction[] } = {}): {
  transaction: { message: { instructions: unknown[] } };
  meta: { err: unknown; innerInstructions: Array<{ index: number; instructions: unknown[] }> };
  slot: number;
  blockTime: number;
} {
  const conv = (i: TransactionInstruction): unknown => ({
    programId: i.programId.toBase58(),
    accounts: i.keys.map((k) => k.pubkey.toBase58()),
    data: encodeBase58(new Uint8Array(i.data)),
  });
  return {
    transaction: { message: { instructions: ixs.map(conv) } },
    meta: { err: opts.err ?? null, innerInstructions: opts.inner ? [{ index: 0, instructions: opts.inner.map(conv) }] : [] },
    slot: 4242,
    blockTime: 1_790_000_000,
  };
}

/** A Helius enhanced-shaped transaction. */
export function enhancedTx(ixs: TransactionInstruction[], opts: { nested?: TransactionInstruction[] } = {}): {
  instructions: Array<{ programId: string; accounts: string[]; data: string; innerInstructions: unknown[] }>;
} {
  const conv = (i: TransactionInstruction) => ({
    programId: i.programId.toBase58(),
    accounts: i.keys.map((k) => k.pubkey.toBase58()),
    data: encodeBase58(new Uint8Array(i.data)),
    innerInstructions: [] as unknown[],
  });
  const out = ixs.map(conv);
  if (opts.nested && out.length > 0) out[0].innerInstructions = opts.nested.map(conv);
  return { instructions: out };
}
