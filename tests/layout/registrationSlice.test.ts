import { beforeEach, describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { LAYOUTS_BY_VERSION, V17_ASSET_ORACLE_PROFILE_LEN } from "@percolatorct/sdk";
import { discoverV17Markets, V17_REGISTRATION_SLICE_LEN } from "../../src/v17/discovery.js";
import { readMarketGroupFields, registrationSliceLenFor, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { LAYOUT_V21, LAYOUT_V22, buildMarket, pk, stampHeader } from "../helpers/v22Fixtures.js";

const PROGRAM = new PublicKey("11111111111111111111111111111111");

/** getProgramAccounts that honours dataSlice exactly like an RPC node does. */
function slicingConn(accts: Array<{ key: PublicKey; data: Uint8Array }>, forceLen?: number) {
  return {
    getProgramAccounts: vi.fn(async (_p: PublicKey, o: { dataSlice?: { offset: number; length: number } }) =>
      accts.map((a) => {
        const len = forceLen ?? o.dataSlice?.length;
        return { pubkey: a.key, account: { data: Buffer.from(len === undefined ? a.data : a.data.subarray(0, len)) } };
      })),
  } as never;
}

beforeEach(() => resetUnknownLayoutState());

describe("light registration slice covers every layout VERSION", () => {
  it("is the max over the SDK table, with no literal: v2.1 1,862 B, v2.2 1,910 B (592 + 806 + 512)", () => {
    expect(registrationSliceLenFor(LAYOUT_V21)).toBe(592 + 758 + 512); // 1,862: the figure #226 pinned
    expect(registrationSliceLenFor(LAYOUT_V22)).toBe(592 + 806 + 512); // 1,910
    expect(V17_ASSET_ORACLE_PROFILE_LEN).toBe(512);
    expect(V17_REGISTRATION_SLICE_LEN).toBe(Math.max(...[...LAYOUTS_BY_VERSION.values()].map(registrationSliceLenFor)));
    expect(V17_REGISTRATION_SLICE_LEN).toBeGreaterThanOrEqual(1910);
    expect(V17_REGISTRATION_SLICE_LEN).toBeLessThan(4059); // still much smaller than the smallest v2.2 market
  });

  for (const [name, layout] of [["v2.1", LAYOUT_V21], ["v2.2", LAYOUT_V22]] as const) {
    it(`${name}: a sliced one-slot market registers with the SAME fields as the whole account (oracle authority, vault)`, async () => {
      const authority = pk();
      const data = buildMarket(layout, { slots: 1, vault: 4_000_000n, insurance: 7n, oracleAuthority: authority, markEwmaE6: 123n });
      const m = { key: pk(), data };
      const [light] = await discoverV17Markets(slicingConn([m]), PROGRAM, undefined, { light: true });
      const [full] = await discoverV17Markets(slicingConn([m]), PROGRAM);
      expect(light.config.oracleAuthority.toBase58()).toBe(authority.toBase58());
      expect(light.config.oracleAuthority.toBase58()).toBe(full.config.oracleAuthority.toBase58());
      expect(light.engine.vault).toBe(4_000_000n);
      expect(light.engine.vault).toBe(full.engine.vault);
      expect(light.engine.insuranceFund.balance).toBe(full.engine.insuranceFund.balance);
    });
  }

  it("the slice is exactly the market's own registration prefix for a 4,059 B v2.2 one-slot market", () => {
    expect(buildMarket(LAYOUT_V22, { slots: 1 }).length).toBe(1398 + 2661); // 4,059
    expect(buildMarket(LAYOUT_V22, { slots: 5 }).length).toBe(1398 + 5 * 2661); // 14,703
  });

  it("NEGATIVE CONTROL: the old v2.1-sized 1,862 B slice cuts the v2.2 profile off and registers a ZERO oracle authority", async () => {
    const authority = pk();
    const m = { key: pk(), data: buildMarket(LAYOUT_V22, { slots: 1, oracleAuthority: authority }) };
    const [cut] = await discoverV17Markets(slicingConn([m], 1862), PROGRAM, undefined, { light: true });
    expect(cut.config.oracleAuthority.toBase58()).not.toBe(authority.toBase58());
    // ... and the slice the code now uses does not.
    const [ok] = await discoverV17Markets(slicingConn([m]), PROGRAM, undefined, { light: true });
    expect(ok.config.oracleAuthority.toBase58()).toBe(authority.toBase58());
  });

  it("asset0ProfileOff is set from a slice that holds the profile but no whole slot (the pre-fix code returned null)", () => {
    const data = buildMarket(LAYOUT_V22, { slots: 1 }).subarray(0, V17_REGISTRATION_SLICE_LEN);
    expect(readMarketGroupFields(data, "t").asset0ProfileOff).toBe(1398);
    expect(readMarketGroupFields(data, "t").geometry.slotCount).toBe(0);
  });
});

describe("the v2.2 market lengths (4,059 / 6,720 / 9,381 / 12,042 / 14,703 by capacity) and the other v2.2 account kinds", () => {
  // literals from the RC's layout-v22.json (account_lengths.market_account_len_by_capacity)
  const LENS = [4_059, 6_720, 9_381, 12_042, 14_703];

  for (const [i, len] of LENS.entries()) {
    it(`capacity ${i + 1} = ${len} B: built from the SDK table, discovered by the full AND the light pass with identical registration fields`, async () => {
      const authority = pk();
      const data = buildMarket(LAYOUT_V22, { slots: i + 1, vault: 1_234n, oracleAuthority: authority });
      expect(data.length).toBe(len);
      const m = { key: pk(), data };
      const [full] = await discoverV17Markets(slicingConn([m]), PROGRAM);
      const [light] = await discoverV17Markets(slicingConn([m]), PROGRAM, undefined, { light: true });
      for (const d of [full, light]) {
        expect(d.config.oracleAuthority.toBase58()).toBe(authority.toBase58());
        expect(d.engine.vault).toBe(1_234n);
      }
      expect(readMarketGroupFields(data, "t").geometry.slotCount).toBe(i + 1);
    });
  }

  it("the light slice (1,910 B) is shorter than the smallest v2.2 market (4,059 B) and than the smallest v2.1 slab, so it is always a strict prefix", () => {
    expect(V17_REGISTRATION_SLICE_LEN).toBe(1_910);
    expect(V17_REGISTRATION_SLICE_LEN).toBeLessThan(4_059);
    expect(registrationSliceLenFor(LAYOUT_V21)).toBe(1_862);
    expect(V17_REGISTRATION_SLICE_LEN).toBeLessThan(buildMarket(LAYOUT_V21, { slots: 1 }).length);
  });

  it("accounts of the other v2.2 kinds are not markets: the G9 allowlist (kind 15, 2,080 B), bond tranche, insurance units, a portfolio never register", async () => {
    const kinds: Array<[string, number, number]> = [["g9 allowlist", 15, 2_080], ["bond tranche", 11, 16 + 128], ["bond position", 12, 16 + 96], ["insurance units", 13, 16 + 192], ["portfolio", 2, LAYOUT_V22.portfolio.accountLen]];
    for (const [name, kind, len] of kinds) {
      const a = { key: pk(), data: stampHeader(new Uint8Array(len), kind, 19) };
      expect(await discoverV17Markets(slicingConn([a]), PROGRAM), name).toEqual([]);
    }
  });
});
