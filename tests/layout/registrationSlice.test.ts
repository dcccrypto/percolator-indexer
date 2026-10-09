import { beforeEach, describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { LAYOUTS_BY_VERSION, V17_ASSET_ORACLE_PROFILE_LEN } from "@percolatorct/sdk";
import { discoverV17Markets, V17_REGISTRATION_SLICE_LEN } from "../../src/v17/discovery.js";
import { readMarketGroupFields, registrationSliceLenFor, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { LAYOUT_V21, LAYOUT_V22, buildMarket, pk } from "../helpers/v22Fixtures.js";

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
