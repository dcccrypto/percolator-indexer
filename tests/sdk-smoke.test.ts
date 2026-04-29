/**
 * SDK publish smoke test — runs against the *installed* @percolatorct/sdk package.
 *
 * Purpose: catch publish-time regressions (missing exports, bad tarball, files: glob
 * mistakes, dist/ not regenerated) that are invisible when pnpm uses a workspace link.
 *
 * This test does NOT make RPC calls or DB connections. Everything is pure in-process
 * computation so it runs reliably in CI without any environment secrets.
 *
 * Pinned version: @percolatorct/sdk@2.0.4 (v12.19 mainnet alignment)
 * Update this comment when the workflow pins a new version.
 */

import { describe, it, expect } from "vitest";

// ── 1. Named-export existence ─────────────────────────────────────────────────
// Every symbol the indexer actually imports must resolve without throwing.
//
// Sources:
//   webhook.ts                    — IX_TAG, detectSlabLayout
//   AdlIndexer.ts                 — IX_TAG
//   NftIndexer.ts                 — IX_TAG
//   TradeIndexer.ts               — IX_TAG, detectSlabLayout
//   StatsCollector.ts             — parseEngine, parseConfig, parseParams,
//                                   parseAllAccounts, detectDexType, parseDexPool,
//                                   EngineState (type), MarketConfig (type),
//                                   RiskParams (type), DiscoveredMarket (type)
//   MarketDiscovery.ts            — discoverMarkets, DiscoveredMarket (type)
//   InsuranceLPService.ts         — DiscoveredMarket (type)
//   backfill-price-zero-trades.ts — detectSlabLayout
import {
  IX_TAG,
  detectSlabLayout,
  parseEngine,
  parseConfig,
  parseParams,
  parseAllAccounts,
  detectDexType,
  parseDexPool,
  discoverMarkets,
  SLAB_TIERS_V12_17,
} from "@percolatorct/sdk";

// Type-only imports — these exercise the .d.ts surface without runtime cost.
import type {
  EngineState,
  MarketConfig,
  RiskParams,
  DiscoveredMarket,
} from "@percolatorct/sdk";

// ── 2. IX_TAG structure ───────────────────────────────────────────────────────

describe("@percolatorct/sdk exports — IX_TAG (indexer)", () => {
  it("IX_TAG is an object", () => {
    expect(typeof IX_TAG).toBe("object");
    expect(IX_TAG).not.toBeNull();
  });

  it("IX_TAG.KeeperCrank is 5", () => {
    expect(IX_TAG.KeeperCrank).toBe(5);
  });

  it("IX_TAG.LiquidateAtOracle is 7", () => {
    expect(IX_TAG.LiquidateAtOracle).toBe(7);
  });

  it("IX_TAG.ExecuteAdl is 50", () => {
    expect(IX_TAG.ExecuteAdl).toBe(50);
  });

  it("IX_TAG has at least 10 instruction tags", () => {
    expect(Object.keys(IX_TAG).length).toBeGreaterThanOrEqual(10);
  });
});

// ── 3. detectSlabLayout round-trip ───────────────────────────────────────────
//
// The indexer uses detectSlabLayout(data.length) to distinguish V0 vs V1 slab
// account layouts in webhook and TradeIndexer. Feeding the real V1 data length
// should return a non-null layout; an arbitrary unknown length should return null.

describe("@percolatorct/sdk exports — detectSlabLayout (indexer)", () => {
  it("detectSlabLayout is a function", () => {
    expect(typeof detectSlabLayout).toBe("function");
  });

  it("returns null for length 1 (unknown layout)", () => {
    expect(detectSlabLayout(1)).toBeNull();
  });

  it("returns null for length 0", () => {
    expect(detectSlabLayout(0)).toBeNull();
  });

  it("returns a V12_19 small layout for 96760 bytes (mainnet 2026-04-28 upgrade)", () => {
    // v12.19 small slab — what live mainnet program produces under --features small.
    // Wrapper anchor: percolator-prog post v12.19 upgrade.
    const layout = detectSlabLayout(96760);
    expect(layout).not.toBeNull();
    if (layout !== null) {
      expect(layout.maxAccounts).toBe(256);
    }
  });

  it("returns a SlabLayout object for a known tier length if any tier is registered", () => {
    // SLAB_TIERS_V12_17 maps maxAccounts → { dataSize, ... }.
    // Pick the first registered size from that map and verify detectSlabLayout is consistent.
    const tiers: Array<{ dataSize: number }> = Object.values(
      SLAB_TIERS_V12_17 as Record<string, { dataSize: number }>
    );
    if (tiers.length === 0) return; // nothing to check
    const knownSize = tiers[0]!.dataSize;
    const layout = detectSlabLayout(knownSize);
    // A registered tier must produce a non-null layout
    expect(layout).not.toBeNull();
    if (layout !== null) {
      expect(typeof layout.bitmapWords).toBe("number");
      expect(typeof layout.accountsOff).toBe("number");
      expect(typeof layout.maxAccounts).toBe("number");
    }
  });
});

// ── 4. market discovery / dex oracle (shape-only, no network) ────────────────

describe("@percolatorct/sdk exports — market discovery (indexer)", () => {
  it("discoverMarkets is a function", () => {
    expect(typeof discoverMarkets).toBe("function");
  });

  it("detectDexType is a function", () => {
    expect(typeof detectDexType).toBe("function");
  });

  it("parseDexPool is a function", () => {
    expect(typeof parseDexPool).toBe("function");
  });
});

// ── 5. parse function shapes ──────────────────────────────────────────────────

describe("@percolatorct/sdk exports — parse function shapes (indexer)", () => {
  it("parseEngine is a function", () => {
    expect(typeof parseEngine).toBe("function");
  });

  it("parseConfig is a function", () => {
    expect(typeof parseConfig).toBe("function");
  });

  it("parseParams is a function", () => {
    expect(typeof parseParams).toBe("function");
  });

  it("parseAllAccounts is a function", () => {
    expect(typeof parseAllAccounts).toBe("function");
  });
});
