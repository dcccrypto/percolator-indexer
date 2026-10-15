import { beforeEach, describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { discoverV17Markets } from "../../src/v17/discovery.js";
import { getUnknownLayoutCount, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { LAYOUT_V21, LAYOUT_V22, REAL_MARKET_V21, REAL_PORTFOLIO_V21, buildMarket, buildPortfolio, pk } from "../helpers/v22Fixtures.js";

const PROGRAM = new PublicKey("11111111111111111111111111111111");
const u128 = (d: Uint8Array, o: number): bigint => { const v = new DataView(d.buffer, d.byteOffset); return v.getBigUint64(o, true) | (v.getBigUint64(o + 8, true) << 64n); };

function conn(accts: Array<{ key: PublicKey; data: Uint8Array }>) {
  return {
    getMultipleAccountsInfo: vi.fn(async (keys: PublicKey[]) =>
      keys.map((k) => { const a = accts.find((x) => x.key.equals(k)); return a ? { data: Buffer.from(a.data), owner: PROGRAM } : null; })),
    getProgramAccounts: vi.fn(async () => accts.map((a) => ({ pubkey: a.key, account: { data: Buffer.from(a.data) } }))),
  } as never;
}

beforeEach(() => resetUnknownLayoutState());

describe("discoverV17Markets: v2.1 and v2.2 side by side, an unknown VERSION skipped alone", () => {
  const v21 = { key: pk(), data: REAL_MARKET_V21 };
  const v22 = { key: pk(), data: buildMarket(LAYOUT_V22, { vault: 4_000_000n, insurance: 12n, cTot: 8n, slots: 1 }) };
  const unknown = { key: pk(), data: buildMarket(LAYOUT_V22, { vault: 9n }, 20) };
  const portfolio22 = { key: pk(), data: buildPortfolio(LAYOUT_V22) };
  const portfolio21 = { key: pk(), data: REAL_PORTFOLIO_V21 };

  it("MARKETS_FILTER path: both known layouts discovered with correct vault; VERSION 20 skipped loudly; portfolios ignored", async () => {
    const all = [unknown, v21, portfolio22, v22, portfolio21];
    const out = await discoverV17Markets(conn(all), PROGRAM, all.map((a) => a.key));
    expect(out.map((m) => m.slabAddress.toBase58()).sort()).toEqual([v21.key.toBase58(), v22.key.toBase58()].sort());
    const m21 = out.find((m) => m.slabAddress.equals(v21.key))!;
    const m22 = out.find((m) => m.slabAddress.equals(v22.key))!;
    expect(m21.engine?.vault).toBe(u128(REAL_MARKET_V21, 592 + 285)); // v2.1 unchanged
    expect(m22.engine?.vault).toBe(4_000_000n);
    expect(m22.engine?.insuranceFund.balance).toBe(12n);
    expect(m22.engine?.cTot).toBe(8n);
    expect(getUnknownLayoutCount()).toBe(1);
  });

  it("scan path (getProgramAccounts) behaves the same, and the unknown market does not wedge the others", async () => {
    const out = await discoverV17Markets(conn([v22, unknown, v21]), PROGRAM);
    expect(out).toHaveLength(2);
    expect(getUnknownLayoutCount()).toBe(1);
  });

  it("NEGATIVE CONTROL: with only the unknown-VERSION market the result is empty AND the skip is counted (not silent)", async () => {
    const out = await discoverV17Markets(conn([unknown]), PROGRAM, [unknown.key]);
    expect(out).toEqual([]);
    expect(getUnknownLayoutCount()).toBe(1);
  });

  it("asset-0 oracle authority comes from slot 0 of the account's own VERSION (v2.2 slot 0 is at 1,398)", async () => {
    const authority = pk();
    const m = { key: pk(), data: buildMarket(LAYOUT_V22, { slots: 1, oracleAuthority: authority }) };
    const [d] = await discoverV17Markets(conn([m]), PROGRAM, [m.key]);
    expect(d.config.oracleAuthority.toBase58()).toBe(authority.toBase58());
    expect(LAYOUT_V21.marketGroupLen).toBe(758); // the old literal; v2.2 would have read 48 B early
  });
});
