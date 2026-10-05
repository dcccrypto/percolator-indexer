import { PublicKey } from "@solana/web3.js";
import { isPoisonTxError, MAX_SKIPS_ABSOLUTE } from "../lib/tolerantTxFetch.js";
import { recordSkippedSignatures, type SkippedSignature } from "./../lib/skippedSignatures.js";
import { V17_MARKET_GROUP_OFF } from "@percolatorct/sdk";
import type {
  ConfirmedSignatureInfo,
  SignaturesForAddressOptions,
  VersionedTransactionResponse,
} from "@solana/web3.js";

/**
 * Field resolution for the `markets` rows the indexer auto-inserts for a newly
 * discovered slab (StatsCollector.syncMarkets).
 *
 * Before 2026-10-01 those rows were wrong in four ways. Every v17/v18 market had
 * them (measured on ntcn: 9EPm8nB8… and the six 09-29/09-30 auto rows):
 *  - `deployer` was the COLLATERAL MINT. configV17 has no header.admin and no
 *    oracleAuthority, so `header.admin ?? (oracleAuthority || mint)` always fell
 *    through to the mint.
 *  - `oracle_authority` was "" (an empty string, not NULL).
 *  - `initial_price_e6` was 0 (v17 has no authorityPriceE6 / markEwmaE6 on config).
 *  - `trading_fee_bps` was a hard-coded 10. On chain 9EPm8nB8… is 5.
 *  - `max_leverage` was always the GH#1748 fallback 10: v17 discovery has no
 *    `params`, so initialMarginBps read as 0. On chain 9EPm8nB8… is 1819 bps (5x).
 */

/**
 * Absolute offset of the engine's `initial_margin_bps` (u64 LE) in a v17/v18 slab:
 * the V16ConfigAccount starts 32 bytes into the market-group region and the field
 * sits at +62. Same layout the app reads (percolator-launch lib/v17-engine-config.ts),
 * verified 2026-10-01 on 9EPm8nB8… (1819) and 8WC8vALs… (1000).
 */
export const V17_INITIAL_MARGIN_BPS_OFF = V17_MARKET_GROUP_OFF + 32 + 62;

/** The engine's initial margin from raw v17/v18 slab bytes; NULL if too short or zero. */
export function v17InitialMarginBps(data: Uint8Array): bigint | null {
  if (data.length < V17_INITIAL_MARGIN_BPS_OFF + 8) return null;
  const dv = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const v = dv.getBigUint64(V17_INITIAL_MARGIN_BPS_OFF, true);
  return v > 0n ? v : null;
}

/** The subset of a DiscoveredMarket the resolver reads. v17 slabs carry `configV17`;
 *  v12 slabs carry header/config/params. */
export interface AutoRowMarketInput {
  header?: { admin?: PublicKey } | null;
  config?: { oracleAuthority?: PublicKey; authorityPriceE6?: bigint; markEwmaE6?: bigint } | null;
  params?: { tradingFeeBps?: bigint } | null;
  configV17?: {
    marketauth?: PublicKey;
    tradeFeeBps?: bigint;
    oracleTargetPriceE6?: bigint;
    markEwmaE6?: bigint;
  } | null;
}

export interface AutoMarketFields {
  /** Market creator. NULL only when nothing on chain names anyone (caller skips the row). */
  deployer: string | null;
  oracle_authority: string | null;
  initial_price_e6: number | null;
  trading_fee_bps: number;
}

/** Same fallback the indexer always used for a slab with no readable fee. */
export const DEFAULT_TRADING_FEE_BPS = 10;

const DEFAULT_PUBKEY = PublicKey.default.toBase58();

function realKey(k: PublicKey | undefined | null): string | null {
  if (!k) return null;
  const s = k.toBase58();
  return s === DEFAULT_PUBKEY ? null : s;
}

function positiveNumber(v: bigint | undefined | null): number | null {
  if (v === undefined || v === null || v <= 0n) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Resolve the auto-row fields that depend on who created the market and on its
 * on-chain config.
 *
 * deployer precedence:
 *   1. `creator`: the signer of the slab's creation transaction (findSlabCreator),
 *      which is the wallet that launched the market.
 *   2. v12 `header.admin`.
 *   3. v17 `configV17.marketauth`. For wizard markets this is a keyless PDA, so it
 *      names the market authority rather than a person, but it is on chain and
 *      tied to the market.
 * The collateral mint is never used: it identifies the collateral, not who
 * created the market.
 */
export function resolveAutoMarketFields(
  market: AutoRowMarketInput,
  creator: string | null,
): AutoMarketFields {
  const v17 = market.configV17 ?? null;
  const deployer =
    creator ??
    realKey(market.header?.admin) ??
    realKey(v17?.marketauth) ??
    null;

  const oracle_authority = v17 ? null : realKey(market.config?.oracleAuthority);

  const initial_price_e6 = v17
    ? positiveNumber(v17.oracleTargetPriceE6) ?? positiveNumber(v17.markEwmaE6)
    : positiveNumber(market.config?.authorityPriceE6) ?? positiveNumber(market.config?.markEwmaE6);

  const rawFee = v17 ? v17.tradeFeeBps : market.params?.tradingFeeBps;
  const fee = rawFee === undefined || rawFee === null ? null : Number(rawFee);
  const trading_fee_bps =
    fee !== null && Number.isInteger(fee) && fee >= 0 && fee <= 10_000 ? fee : DEFAULT_TRADING_FEE_BPS;

  return { deployer, oracle_authority, initial_price_e6, trading_fee_bps };
}

/**
 * The creator named by a slab's creation transaction: the first account of the
 * wrapper instruction that references the slab, when that account signed. If the
 * wrapper instruction is there but its first account didn't sign, the fee payer.
 * NULL when the transaction has no wrapper instruction touching the slab (it is
 * not the creation transaction).
 */
export function creatorFromCreationTx(
  tx: VersionedTransactionResponse,
  slab: PublicKey,
  programId: PublicKey,
): string | null {
  const message = tx.transaction.message;
  const keys = message.getAccountKeys({
    accountKeysFromLookups: tx.meta?.loadedAddresses ?? undefined,
  });
  for (const ix of message.compiledInstructions) {
    const program = keys.get(ix.programIdIndex);
    if (!program || !program.equals(programId)) continue;
    const touchesSlab = ix.accountKeyIndexes.some((i) => keys.get(i)?.equals(slab) ?? false);
    if (!touchesSlab) continue;
    const first = ix.accountKeyIndexes[0];
    if (first !== undefined && message.isAccountSigner(first)) {
      const k = keys.get(first);
      if (k) return k.toBase58();
    }
    return keys.get(0)?.toBase58() ?? null;
  }
  return null;
}

/** The two RPC calls findSlabCreator needs. StatsCollector adapts a web3 Connection. */
export interface CreatorLookupRpc {
  getSignaturesForAddress(
    address: PublicKey,
    options: SignaturesForAddressOptions,
  ): Promise<ConfirmedSignatureInfo[]>;
  getTransaction(signature: string): Promise<VersionedTransactionResponse | null>;
}

/** History pages (of 1000 signatures) walked back before giving up on finding the start. */
export const CREATOR_LOOKUP_MAX_PAGES = 10;
/** How many of the OLDEST successful signatures are tried as the creation transaction. */
export const CREATOR_LOOKUP_MAX_TX = 5;

/**
 * Find a slab's creator from its creation transaction.
 *
 * Runs once per newly discovered market (registration only inserts missing
 * rows). A fresh slab has a short history, so this is usually one signature page
 * and one getTransaction. NULL when the history's start is out of reach (more
 * than CREATOR_LOOKUP_MAX_PAGES pages), when no early transaction is the
 * creation, or when the RPC fails. The caller then falls back to on-chain config.
 */
export async function findSlabCreator(
  rpc: CreatorLookupRpc,
  slab: PublicKey,
  programId: PublicKey,
): Promise<string | null> {
  let before: string | undefined;
  let oldest: ConfirmedSignatureInfo[] = [];
  let exhausted = false;
  for (let page = 0; page < CREATOR_LOOKUP_MAX_PAGES; page++) {
    const sigs = await rpc.getSignaturesForAddress(slab, { before, limit: 1000 });
    if (sigs.length === 0) {
      exhausted = true;
      break;
    }
    oldest = sigs;
    before = sigs[sigs.length - 1].signature;
    if (sigs.length < 1000) {
      exhausted = true;
      break;
    }
  }
  if (!exhausted) return null;

  const candidates = oldest
    .filter((s) => s.err === null)
    .slice(-CREATOR_LOOKUP_MAX_TX)
    .reverse(); // oldest first
  const skipped: SkippedSignature[] = [];
  for (const sig of candidates) {
    let tx;
    try {
      tx = await rpc.getTransaction(sig.signature);
    } catch (err) {
      if (!isPoisonTxError(err)) throw err;
      // X-1: skip a transaction this client cannot return (recorded below, once the lookup is known not to be broken).
      skipped.push({ signature: sig.signature, source: "creator-lookup", slab: slab.toBase58(), error: err instanceof Error ? err.message : String(err) });
      if (skipped.length > MAX_SKIPS_ABSOLUTE) throw new Error(`creator lookup for ${slab.toBase58()}: mass skip refused (${skipped.length} unreadable signatures)`);
      continue;
    }
    if (!tx) continue;
    const creator = creatorFromCreationTx(tx, slab, programId);
    if (creator) {
      await recordSkippedSignatures(skipped);
      return creator;
    }
  }
  // Successful-sibling rule: skipping is only acceptable if at least one candidate was read; all unreadable = broken reader.
  if (skipped.length > 0 && skipped.length === candidates.length) {
    throw new Error(`creator lookup for ${slab.toBase58()}: mass skip refused (every candidate unreadable)`);
  }
  await recordSkippedSignatures(skipped);
  return null;
}
