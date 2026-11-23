/**
 * SDK publish smoke test — runs against the *installed* @percolatorct/sdk package.
 *
 * Purpose: catch publish-time regressions (missing exports, bad tarball, files: glob
 * mistakes, dist/ not regenerated) that are invisible when pnpm uses a workspace link.
 *
 * This test does NOT make RPC calls or DB connections. Everything is pure in-process
 * computation so it runs reliably in CI without any environment secrets.
 *
 * Pinned version: @percolatorct/sdk@6.0.0 (v18 wire migration)
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
  LAYOUT_V21,
  LAYOUT_V22,
  LAYOUTS_BY_VERSION,
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

  it("IX_TAG.ExecuteAdl is 101 in v17 (deprecated alias — removed from wrapper decoder)", () => {
    // v17 rearchitected ADL: ExecuteAdl moved to deprecated tag 101.
    // Tag 50 in v17 is ForceCloseExpiredPosition (different instruction).
    // AdlIndexer.ts uses an empty ADL_TAGS set in v17 — no wrapper ADL instruction.
    expect(IX_TAG.ExecuteAdl).toBe(101);
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

  it("returns a V12_19 small layout for 96784 bytes (mainnet 2026-05-01 upgrade)", () => {
    // v12.19 small slab — what live mainnet program produces under --features small.
    // Wrapper anchor: percolator-prog post v12.19 upgrade.
    const layout = detectSlabLayout(96784);
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

// ── 6. v2.2 layout pins (vendored copy must match the deployed devnet program) ─
//
// The checks above pass on both the vendored SDK (cb444db) and SDK e1fe6692,
// so they cannot see layout drift. e1fe6692 ships "variant C": engine
// discriminator 20, market group 790 B (every group offset 16 B earlier) and
// 10,607 B portfolios. Devnet still runs variant B (disc 19, 806 B group,
// 10,603 B portfolios: 211/211 live portfolios on wrapper 6kpg2wi7…, slot
// 525,544,627). With variant C, light discovery reads vault/cTot/insurance/
// oracle authority 16 B off on 71/71 devnet markets and every portfolio decode
// throws UnknownLayoutError. A re-vendor must change these pins on purpose,
// only once devnet runs the new layout.
//
// Source: report idx-sdksmoke-vendor-drift-20261123 (J-20261123-IDX-REPORT-SDKSMOKEVENDORDRIFT).

describe("@percolatorct/sdk layout pins — v2.2 variant B (devnet)", () => {
  it("LAYOUT_V22 is wrapper VERSION 19 with engine discriminator 19", () => {
    expect(LAYOUT_V22.version).toBe(19);
    expect(LAYOUT_V22.engineDiscriminator).toBe(19);
  });

  it("LAYOUT_V22 market group is 806 B with vault at +333", () => {
    expect(LAYOUT_V22.marketGroupLen).toBe(806);
    expect(LAYOUT_V22.group.vault).toBe(333);
  });

  it("LAYOUT_V22 portfolio account is 10,603 B", () => {
    expect(LAYOUT_V22.portfolio.accountLen).toBe(10603);
  });

  it("LAYOUT_V22 and LAYOUT_V21 bond tranche body is 128 B", () => {
    expect(LAYOUT_V22.accounts.bondTrancheBody).toBe(128);
    expect(LAYOUT_V21.accounts.bondTrancheBody).toBe(128);
  });

  it("LAYOUTS_BY_VERSION[19] (the table resolve.ts reads) carries the same pins", () => {
    const v19 = LAYOUTS_BY_VERSION.get(19);
    expect(v19).toBeDefined();
    expect(v19?.engineDiscriminator).toBe(19);
    expect(v19?.marketGroupLen).toBe(806);
    expect(v19?.group.vault).toBe(333);
    expect(v19?.portfolio.accountLen).toBe(10603);
  });
});
