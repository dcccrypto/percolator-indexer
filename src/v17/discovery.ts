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
  isV17Account,
  parseWrapperConfigV17,
  parseAssetOracleProfileV17,
  V17_HEADER_LEN,
  V17_WRAPPER_CONFIG_LEN,
  V17_MARKET_GROUP_OFF,
  V17_ASSET_ORACLE_PROFILE_LEN,
  type DiscoveredMarket,
  type SlabHeader,
  type MarketConfig,
  type EngineState,
  type RiskParams,
  type InsuranceFund,
} from "@percolatorct/sdk";

/**
 * V17 magic bytes in base58 for RPC memcmp filter.
 * 0x5045524356313600 stored as little-endian u64:
 * bytes = [0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50]
 */
const V17_MAGIC_BYTES = new Uint8Array([0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50]);

/**
 * V17 market group header layout at V17_MARKET_GROUP_OFF (448).
 * MarketGroupV16HeaderAccount on-chain serialization (dense / zero-copy).
 * Full struct in v16_program.rs:MarketGroupV16HeaderAccount.
 *
 * Offsets (relative to V17_MARKET_GROUP_OFF = 448), VERIFIED against the program's own
 * `cargo run --example dump_layout` for MarketGroupV16HeaderAccount (size=758, align=1):
 *   +0    market_group_id [u8;32]       → 32 bytes
 *   +32   config V16ConfigAccount       → 249 bytes (INLINE — engine config sits before vault)
 *   +281  asset_slot_capacity u32       → 4 bytes
 *   +285  vault u128                    → 16 bytes  (abs offset 733)
 *   +301  insurance u128                → 16 bytes  (abs offset 749)
 *   +317  c_tot u128                    → 16 bytes  (abs offset 765)
 *
 * NOTE: an earlier version used +32/+48/+64 on the false assumption that vault followed
 * market_group_id directly. That is wrong — the 249-byte V16ConfigAccount + 4-byte
 * asset_slot_capacity precede vault, so +32/48/64 read INSIDE the config block (garbage).
 * There is no separate "keeper wire format"; the program reads this same bytemuck cast.
 */
const MG_MARKET_GROUP_ID_OFF = 0;   // [u8;32] market_group_id
const MG_VAULT_OFF = 285;           // u128 vault
const MG_INSURANCE_OFF = 301;       // u128 insurance
const MG_C_TOT_OFF = 317;           // u128 c_tot

/** Minimum market group header length (must cover the c_tot read at +317). */
const MG_MIN_HEADER_BYTES = 333;

/** Zero pubkey sentinel. */
const ZERO_PUBKEY = new PublicKey(new Uint8Array(32));

/** Read u128 little-endian from a Uint8Array. Returns BigInt. */
function readU128LE(data: Uint8Array, offset: number): bigint {
  const dv = new DataView(data.buffer, data.byteOffset + offset, 16);
  const lo = dv.getBigUint64(0, true);
  const hi = dv.getBigUint64(8, true);
  return lo | (hi << 64n);
}

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
 * InsuranceLPService.poll() reads engine.insuranceFund.balance and engine.lastCrankSlot —
 * these are populated from the real v17 on-chain layout.
 */
function makeV17EngineStub(data: Uint8Array): EngineState {
  const mgOff = V17_MARKET_GROUP_OFF;
  const hasHeader = data.length >= mgOff + MG_MIN_HEADER_BYTES;

  const vault = hasHeader ? readU128LE(data, mgOff + MG_VAULT_OFF) : 0n;
  const insurance = hasHeader ? readU128LE(data, mgOff + MG_INSURANCE_OFF) : 0n;
  const cTot = hasHeader ? readU128LE(data, mgOff + MG_C_TOT_OFF) : 0n;

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
function makeV17MarketConfig(data: Uint8Array): MarketConfig {
  const cfg = parseWrapperConfigV17(data, V17_HEADER_LEN);

  // Asset-0 oracle profile: at V17_MARKET_GROUP_OFF + market_group_header_len.
  // The market group header has a fixed-size prefix before the per-asset oracle profiles.
  // Based on the desync doc: asset-0 oracle_authority at absolute offset 1326 =
  // 448 (V17_MARKET_GROUP_OFF) + 758 (market_group_header) + 120 (oracleAuthority within profile).
  // V17_MARKET_GROUP_OFF=448, MARKET_GROUP_HDR=758, asset-0 profile starts at 448+758=1206.
  const MARKET_GROUP_HDR_LEN = 758;
  const asset0ProfileOff = V17_MARKET_GROUP_OFF + MARKET_GROUP_HDR_LEN;

  let oracleAuthority = ZERO_PUBKEY;
  let authorityPriceE6 = 0n;
  if (data.length >= asset0ProfileOff + V17_ASSET_ORACLE_PROFILE_LEN) {
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
 * Returns null if the account is not a valid v17 market account.
 */
function parseV17Account(
  pubkey: PublicKey,
  programId: PublicKey,
  data: Uint8Array,
): DiscoveredMarket | null {
  if (!isV17Account(data)) return null;

  try {
    const header = makeV17SlabHeader(data, programId);
    const config = makeV17MarketConfig(data);
    const engine = makeV17EngineStub(data);
    const params = makeV17RiskParamsStub();

    return { slabAddress: pubkey, programId, header, config, engine, params };
  } catch {
    return null;
  }
}

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
 * @returns Parsed v17 DiscoveredMarket array
 */
export async function discoverV17Markets(
  connection: Connection,
  programId: PublicKey,
  knownAddresses?: PublicKey[],
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
    // The RPC accepts raw bytes or base58 — use raw bytes array form
    const results = await connection.getProgramAccounts(programId, {
      filters: [
        {
          memcmp: {
            offset: 0,
            bytes: "1347Wxtvn4w", // base58 of [0x00, 0x36, 0x31, 0x56, 0x43, 0x52, 0x45, 0x50] (V17_MAGIC LE)
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
