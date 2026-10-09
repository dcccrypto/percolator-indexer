/**
 * v17 market discovery helpers.
 *
 * The SDK's discoverMarkets() and getMarketsByAddress() both check for the v12
 * PERCOLAT magic (TALOCREP, 0x504552434f4c4154 LE) and reject v17 accounts
 * (PERCV16\0 magic, 0x5045524356313600 LE).
 *
 * This module provides discoverV17Markets() which:
 *   - Queries getProgramAccounts with the v17 memcmp magic filter, OR
 *   - Fetches known addresses via getMultipleAccountsInfo (when addresses are provided)
 *   - Parses each account with parseWrapperConfigV17 (v17-correct offsets)
 *   - Returns DiscoveredMarket objects with v17 fields mapped to the v12 shape
 *     (v12-only fields that have no v17 equivalent are zero-valued stubs)
 *
 * Desync fix: finding 1 (MarketDiscovery) — v17 market discovery with correct magic.
 * Desync fix: finding 2/3 (StatsCollector.syncMarkets) — v17 config fields correctly mapped.
 * Desync fix: finding 4 (InsuranceLPService) — insurance balance read from v17 market group header.
 */

import { Connection, PublicKey } from "@solana/web3.js";
import {
  parseWrapperConfigV17,
  parseAssetOracleProfileV17,
  V17_HEADER_LEN,
  V17_ASSET_ORACLE_PROFILE_LEN,
  type DiscoveredMarket,
  type SlabHeader,
  type MarketConfig,
  type EngineState,
  type RiskParams,
  type InsuranceFund,
} from "@percolatorct/sdk";
import { createLogger } from "@percolatorct/shared";
import { encodeBase58 } from "../lib/base58.js";
import { isWrapperKind, readMarketGroupFields, registrationSliceLen, reportUnknownLayout, type MarketGroupFields } from "../layout/resolve.js";

/**
 * V17 magic bytes in base58 for RPC memcmp filter.
 * 0x5045524356313600 stored as little-endian u64:
 * bytes = [0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50]
 */
const V17_MAGIC_BYTES = new Uint8Array([0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50]);

const logger = createLogger("indexer:v17-discovery");

// Programmatic verification of the base58 magic string (M-1)
const computedMagic = encodeBase58(V17_MAGIC_BYTES);
if (computedMagic !== "1347Wxtvn4w") {
  throw new Error(`v17 magic bytes base58 encoding mismatch: expected '1347Wxtvn4w', computed '${computedMagic}'`);
}

/*
 * Market group geometry (header length, vault / insurance / c_tot offsets, slot stride) is NOT hard-coded here.
 * It comes from the SDK layout table of the account's wrapper VERSION via readMarketGroupFields (layout/resolve.ts):
 * v2.1 (VERSION 18) group header 758 B, v2.2 (VERSION 19) 806 B with different in-header offsets.
 */

/** Zero pubkey sentinel. */
const ZERO_PUBKEY = new PublicKey(new Uint8Array(32));

/** Read u64 little-endian from a Uint8Array. Returns BigInt. */
function readU64LE(data: Uint8Array, offset: number): bigint {
  const dv = new DataView(data.buffer, data.byteOffset + offset, 8);
  return dv.getBigUint64(0, true);
}

/**
 * Stub EngineState for v17 accounts.
 * All v12-engine fields are zeroed. The vault and insurance values come from
 * the v17 market group header at V17_MARKET_GROUP_OFF.
 *
 * `insuranceFund.balance` IS read from the real v17 on-chain layout
 * (V17_MG_INSURANCE_OFF, below).
 *
 * `lastCrankSlot` is NOT. It is zeroed with the other v12-only fields, so every
 * v17 row lands with snapshot_slot = 0 — see #166. This comment previously
 * claimed both were populated from chain, which was true of only the first.
 * Deriving a real crank/current-slot offset for the v17 market-group header is
 * the open half of #166; guessing one would put plausible garbage into a
 * freshness column, which is worse than an honest zero.
 */
function makeV17EngineStub(fields: MarketGroupFields): EngineState {
  const { vault, insurance, cTot } = fields;

  const insuranceFund: InsuranceFund = {
    balance: insurance,
    feeRevenue: 0n,
    isolatedBalance: 0n,
    isolationBps: 0,
  };

  // Stub — v17 has no v12 engine block. All fields required by EngineState interface are zeroed.
  return {
    vault,
    insuranceFund,
    currentSlot: 0n,
    fundingIndexQpbE6: 0n,
    lastFundingSlot: 0n,
    fundingRateBpsPerSlotLast: 0n,
    fundingRateE9: 0n,
    marketMode: null,
    lastCrankSlot: 0n,
    maxCrankStalenessSlots: 0n,
    totalOpenInterest: 0n,
    longOi: 0n,
    shortOi: 0n,
    cTot,
    pnlPosTot: 0n,
    pnlMaturedPosTot: 0n,
    liqCursor: 0,
    gcCursor: 0,
    lastSweepStartSlot: 0n,
    lastSweepCompleteSlot: 0n,
    crankCursor: 0,
    sweepStartIdx: 0,
    lifetimeLiquidations: 0n,
    lifetimeForceCloses: 0n,
    netLpPos: 0n,
    lpSumAbs: 0n,
    lpMaxAbs: 0n,
    lpMaxAbsSweep: 0n,
    emergencyOiMode: false,
    emergencyStartSlot: 0n,
    lastBreakerSlot: 0n,
    numUsedAccounts: 0,
    nextAccountId: 0n,
    markPriceE6: 0n,
    oraclePriceE6: 0n,
    fLongNum: 0n,
    fShortNum: 0n,
    negPnlAccountCount: 0n,
    fundPxLast: 0n,
    resolvedKLongTerminalDelta: 0n,
    resolvedKShortTerminalDelta: 0n,
    resolvedLivePrice: 0n,
  };
}

/**
 * Build a v12-compatible SlabHeader from v17 account header bytes.
 *
 * v17 header layout (16 bytes):
 *   [0..8]  magic u64 LE
 *   [8..10] version u16 LE
 *   [10]    kind u8
 *   [11]    pad u8
 *   [12..16] reserved [u8;4]
 *
 * v12 SlabHeader.admin = WrapperConfigV17.marketauth (first 32 bytes of config block at offset 16).
 * Other v12 SlabHeader fields that have no v17 equivalent are zero-valued stubs.
 */
function makeV17SlabHeader(data: Uint8Array, _programId: PublicKey): SlabHeader {
  const magic = readU64LE(data, 0);
  const version = new DataView(data.buffer, data.byteOffset + 8, 2).getUint16(0, true);

  // WrapperConfigV17.marketauth is the first 32 bytes at offset V17_HEADER_LEN (16)
  let admin = ZERO_PUBKEY;
  if (data.length >= V17_HEADER_LEN + 32) {
    admin = new PublicKey(data.subarray(V17_HEADER_LEN, V17_HEADER_LEN + 32));
  }

  return {
    magic,
    version,
    bump: 0,
    flags: 0,
    resolved: false,
    paused: false,
    admin,
    nonce: 0n,
    lastThrUpdateSlot: 0n,
  };
}

/**
 * Build a v12-compatible MarketConfig from v17 WrapperConfigV17.
 *
 * Maps v17 fields to the v12 MarketConfig shape. Fields with no v17 equivalent
 * are zeroed. The oracleAuthority comes from AssetOracleProfileV17 (asset 0)
 * at V17_MARKET_GROUP_OFF + market_group_header_len + 0 * V17_ASSET_ORACLE_PROFILE_LEN.
 *
 * The v17 WrapperConfigV17 does not have:
 *   - indexFeedId (Pyth feed) → zeroed (treated as hyperp mode by old code)
 *   - oracleAuthority at the global level → comes from AssetOracleProfileV17 asset-0
 *   - authorityPriceE6 → read from AssetOracleProfileV17.oracleTargetPriceE6
 *   - dexPool → null
 */
function makeV17MarketConfig(data: Uint8Array, fields: MarketGroupFields): MarketConfig {
  const cfg = parseWrapperConfigV17(data, V17_HEADER_LEN);

  // Asset-0 oracle profile: the start of slot 0 of THIS account's VERSION (group offset + group length).
  const asset0ProfileOff = fields.asset0ProfileOff;

  let oracleAuthority = ZERO_PUBKEY;
  let authorityPriceE6 = 0n;
  if (asset0ProfileOff !== null && data.length >= asset0ProfileOff + V17_ASSET_ORACLE_PROFILE_LEN) {
    try {
      const oracleProfile = parseAssetOracleProfileV17(data, asset0ProfileOff);
      oracleAuthority = oracleProfile.oracleAuthority;
      authorityPriceE6 = oracleProfile.oracleTargetPriceE6;
    } catch {
      // Asset oracle profile not present — use zeroed authority (Pyth-pinned behavior)
    }
  }

  // markEwmaE6 in WrapperConfigV17 is at offset 232 within the config block (absolute: 16+232=248)
  const lastEffectivePriceE6 = cfg.markEwmaE6;

  return {
    collateralMint: cfg.collateralMint,
    vaultPubkey: ZERO_PUBKEY,         // no separate vault pubkey in v17
    indexFeedId: ZERO_PUBKEY,         // no global index feed in v17; treat as hyperp-mode stub
    maxStalenessSlots: cfg.maxStalenessSecs,
    confFilterBps: cfg.confFilterBps,
    vaultAuthorityBump: 0,
    invert: cfg.invert,
    unitScale: cfg.unitScale,
    fundingHorizonSlots: 0n,          // not in WrapperConfigV17
    fundingKBps: 0n,
    fundingInvScaleNotionalE6: 0n,
    fundingMaxPremiumBps: 0n,
    fundingMaxBpsPerSlot: 0n,
    threshFloor: 0n,
    threshRiskBps: 0n,
    threshUpdateIntervalSlots: 0n,
    threshStepBps: 0n,
    threshAlphaBps: 0n,
    threshMin: 0n,
    threshMax: 0n,
    threshMinStep: 0n,
    oracleAuthority,
    authorityPriceE6,
    authorityTimestamp: 0n,
    oraclePriceCapE2bps: 0n,
    lastEffectivePriceE6,
    oiCapMultiplierBps: 0n,
    maxPnlCap: 0n,
    adaptiveFundingEnabled: false,
    adaptiveScaleBps: 0,
    adaptiveMaxFundingBps: 0n,
    marketCreatedSlot: 0n,
    oiRampSlots: 0n,
    resolvedSlot: 0n,
    insuranceIsolationBps: 0,
    oraclePhase: 0,
    cumulativeVolumeE6: 0n,
    phase2DeltaSlots: 0,
    dexPool: null,
  };
}

/**
 * Stub RiskParams for v17 accounts.
 * v17 risk params are encoded in WrapperConfigV17 fields and per-asset oracle profiles.
 * StatsCollector reads params.warmupPeriodSlots, params.liquidationFeeBps, etc. —
 * these are zeroed for v17 (devnet bring-up only; full param extraction is a Phase 7 item).
 */
function makeV17RiskParamsStub(): RiskParams {
  return {
    warmupPeriodSlots: 0n,
    maintenanceMarginBps: 0n,
    initialMarginBps: 500n,   // default 20x (500 bps) — used for maxLeverage calc in syncMarkets
    tradingFeeBps: 0n,
    maxAccounts: 0n,
    newAccountFee: 0n,
    riskReductionThreshold: 0n,
    maintenanceFeePerSlot: 0n,
    maxCrankStalenessSlots: 0n,
    liquidationFeeBps: 0n,
    liquidationFeeCap: 0n,
    liquidationBufferBps: 0n,
    minLiquidationAbs: 0n,
    minInitialDeposit: 0n,
    minNonzeroMmReq: 0n,
    minNonzeroImReq: 0n,
    insuranceFloor: 0n,
    hMin: 0n,
    hMax: 0n,
  };
}

/**
 * Parse a single v17 market account into a DiscoveredMarket.
 * Returns null if the account is not a valid v17 market account OR is not
 * a KIND_MARKET (kind=1) account.
 *
 * The program discriminates account kinds at byte[10] (check_header @v16_program.rs:986):
 *   KIND_MARKET=1, KIND_PORTFOLIO=2, KIND_BACKING_DOMAIN_LEDGER=3, KIND_INSURANCE_LEDGER=4.
 * Without this guard, PORTFOLIO accounts (9347 bytes pre-v18; 9563 bytes v2.1; 10,603 bytes v2.2) and other non-market v17 accounts (any
 * >=448-byte account with the v17 magic) would pass isV17Account and be parsed
 * as markets, producing bogus rows via StatsCollector.insertMarket. The guard
 * is size-independent (kind byte only), so it is unaffected by that growth.
 */
function parseV17Account(
  pubkey: PublicKey,
  programId: PublicKey,
  data: Uint8Array,
): DiscoveredMarket | null {
  // Not a wrapper account (legacy v1 `PERCOLAT` slab, foreign data) or not a market: silently not ours.
  // KIND_MARKET = 1 at byte[10] (v16_program.rs:46, check_header @v16_program.rs:986). Kind is checked for ANY
  // VERSION so a future-VERSION portfolio is still not mistaken for a market.
  if (!isWrapperKind(data, 1)) return null;

  try {
    // VERSION-keyed geometry (v2.1 = 18, v2.2 = 19). An unknown VERSION throws UnknownLayoutError here: loud,
    // and this ONE market is skipped; the caller's loop carries on with the others.
    const fields = readMarketGroupFields(data, "discoverV17Markets");
    const header = makeV17SlabHeader(data, programId);
    const config = makeV17MarketConfig(data, fields);
    const engine = makeV17EngineStub(fields);
    const params = makeV17RiskParamsStub();

    return { slabAddress: pubkey, programId, header, config, engine, params };
  } catch (err) {
    if (!reportUnknownLayout(pubkey.toBase58(), err, "discoverV17Markets")) {
      logger.warn("v17 market account could not be parsed; skipped", {
        slab: pubkey.toBase58(),
        error: err instanceof Error ? err.message : String(err),
      });
    }
    return null;
  }
}

/**
 * Bytes of a market account that registration reads: everything up to the end of the asset-0
 * oracle profile (marketauth + config at the front, market-group header, oracle authority/price).
 * parseV17Account touches nothing beyond this, so a dataSlice of this length yields a market
 * identical to the one parsed from the whole (multi-KB to MB) slab.
 */
export const V17_REGISTRATION_SLICE_LEN = registrationSliceLen();

/**
 * Discover v17 markets for a given program.
 *
 * When `knownAddresses` is provided, fetches those specific accounts via
 * getMultipleAccountsInfo (avoids getProgramAccounts RPC restriction).
 *
 * Without `knownAddresses`, queries getProgramAccounts with a v17 magic memcmp
 * filter (bytes [0x00,0x36,0x31,0x56,0x43,0x52,0x45,0x50] at offset 0).
 *
 * @param connection    Solana RPC connection
 * @param programId     Program that owns the market accounts
 * @param knownAddresses Optional specific addresses to fetch (MARKETS_FILTER path)
 * @param opts.light     Sliced scan (header bytes only) for the fast discovery pass
 * @returns Parsed v17 DiscoveredMarket array
 */
export async function discoverV17Markets(
  connection: Connection,
  programId: PublicKey,
  knownAddresses?: PublicKey[],
  opts: { light?: boolean } = {},
): Promise<DiscoveredMarket[]> {
  const markets: DiscoveredMarket[] = [];

  if (knownAddresses && knownAddresses.length > 0) {
    // Fetch known addresses in batches of 100 (Solana getMultipleAccounts limit)
    const BATCH = 100;
    for (let i = 0; i < knownAddresses.length; i += BATCH) {
      const batch = knownAddresses.slice(i, i + BATCH);
      const infos = await connection.getMultipleAccountsInfo(batch);
      for (let j = 0; j < batch.length; j++) {
        const info = infos[j];
        if (!info?.data) continue;
        if (!info.owner.equals(programId)) continue;
        const data = new Uint8Array(info.data);
        const market = parseV17Account(batch[j], programId, data);
        if (market) markets.push(market);
      }
    }
    return markets;
  }

  // Full program account scan: memcmp on v17 magic bytes at offset 0
  // This is the v17 equivalent of discoverMarkets() MAGIC_BYTES check.
  // Note: getProgramAccounts is disabled on many public RPC endpoints;
  // Helius supports it on devnet/mainnet for our program.
  try {
    // Convert v17 magic bytes to base58 for the RPC memcmp filter
    // [0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50]
    // The RPC accepts raw bytes or base58 — use raw bytes array form.
    //
    // Also add a memcmp at offset 10 for KIND_MARKET=0x01 so only market-group
    // accounts are returned. Portfolio (kind=2), backing-domain-ledger (kind=3),
    // and insurance-ledger (kind=4) accounts share the same v17 magic and would
    // otherwise be fetched and silently parsed as bogus market rows.
    // This mirrors the keeper's discoverV17Markets KIND_MARKET memcmp (crank.ts).
    // Reference: v16_program.rs:46 (KIND_MARKET=1), v16_program.rs:986 (check_header byte[10]).
    // `light` (#223): fetch only the header bytes registration needs, not the whole slab.
    const results = await connection.getProgramAccounts(programId, {
      ...(opts.light ? { dataSlice: { offset: 0, length: V17_REGISTRATION_SLICE_LEN } } : {}),
      filters: [
        {
          memcmp: {
            offset: 0,
            bytes: "1347Wxtvn4w", // base58 of [0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50] (V17_MAGIC LE)
          },
        },
        {
          memcmp: {
            offset: 10,
            bytes: "2", // base58 of [0x01] = KIND_MARKET
          },
        },
      ],
    });

    for (const { pubkey, account } of results) {
      const data = new Uint8Array(account.data);
      const market = parseV17Account(pubkey, programId, data);
      if (market) markets.push(market);
    }
  } catch {
    // getProgramAccounts may be rejected by the RPC — caller falls back to discoverMarkets()
  }

  return markets;
}
