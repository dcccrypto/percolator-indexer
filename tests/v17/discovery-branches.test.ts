import { beforeEach, describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { discoverV17Markets, V17_REGISTRATION_SLICE_LEN } from "../../src/v17/discovery.js";
import { getUnknownLayoutCount, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { LAYOUT_V22, buildMarket, pk } from "../helpers/v22Fixtures.js";

const PROGRAM = new PublicKey("11111111111111111111111111111111");
const OTHER = pk();

type Info = { data?: Buffer | Uint8Array | null; owner: PublicKey } | null;

/** A connection whose getMultipleAccountsInfo answers from a map; unknown keys come back null. */
function conn(accts: Map<string, Info>) {
  return {
    getMultipleAccountsInfo: vi.fn(async (keys: PublicKey[]) => keys.map((k) => accts.get(k.toBase58()) ?? null)),
    getProgramAccounts: vi.fn(),
  };
}

const market = (spec = {}) => buildMarket(LAYOUT_V22, { slots: 1, ...spec });

beforeEach(() => resetUnknownLayoutState());

describe("discoverV17Markets: MARKETS_FILTER (knownAddresses) path", () => {
  it("an empty address list falls through to the program scan, not the multi-account fetch", async () => {
    const c = conn(new Map());
    c.getProgramAccounts.mockResolvedValue([]);
    expect(await discoverV17Markets(c as never, PROGRAM, [])).toEqual([]);
    expect(c.getMultipleAccountsInfo).not.toHaveBeenCalled();
    expect(c.getProgramAccounts).toHaveBeenCalledTimes(1);
  });

  it("skips a missing account, an account with no data, and an account owned by another program", async () => {
    const missing = pk(), noData = pk(), foreign = pk(), good = pk();
    const c = conn(new Map<string, Info>([
      [noData.toBase58(), { data: null, owner: PROGRAM }],
      [foreign.toBase58(), { data: Buffer.from(market()), owner: OTHER }],
      [good.toBase58(), { data: Buffer.from(market({ vault: 7n })), owner: PROGRAM }],
    ]));
    const out = await discoverV17Markets(c as never, PROGRAM, [missing, noData, foreign, good]);
    expect(out.map((m) => m.slabAddress.toBase58())).toEqual([good.toBase58()]);
    expect(out[0].programId.equals(PROGRAM)).toBe(true);
    expect(out[0].engine?.vault).toBe(7n);
  });

  it("batches by 100: 250 addresses -> 3 RPC calls of 100/100/50, results kept in order across batches", async () => {
    const keys = Array.from({ length: 250 }, () => pk());
    const accts = new Map<string, Info>();
    // markets at positions 0, 99 (end of batch 1), 100 (start of batch 2) and 249 (last of batch 3)
    const live = [0, 99, 100, 249];
    for (const i of live) accts.set(keys[i].toBase58(), { data: Buffer.from(market({ vault: BigInt(i + 1) })), owner: PROGRAM });
    const c = conn(accts);
    const out = await discoverV17Markets(c as never, PROGRAM, keys);
    expect(c.getMultipleAccountsInfo.mock.calls.map((a) => a[0].length)).toEqual([100, 100, 50]);
    expect(out.map((m) => m.slabAddress.toBase58())).toEqual(live.map((i) => keys[i].toBase58()));
    expect(out.map((m) => m.engine?.vault)).toEqual(live.map((i) => BigInt(i + 1)));
  });

  it("exactly 100 addresses is a single batch", async () => {
    const keys = Array.from({ length: 100 }, () => pk());
    const c = conn(new Map());
    expect(await discoverV17Markets(c as never, PROGRAM, keys)).toEqual([]);
    expect(c.getMultipleAccountsInfo).toHaveBeenCalledTimes(1);
  });

  it("a repeated address is parsed once per occurrence (no dedupe): the caller owns uniqueness", async () => {
    const k = pk();
    const c = conn(new Map<string, Info>([[k.toBase58(), { data: Buffer.from(market()), owner: PROGRAM }]]));
    const out = await discoverV17Markets(c as never, PROGRAM, [k, k]);
    expect(out).toHaveLength(2);
    expect(out[0].slabAddress.equals(out[1].slabAddress)).toBe(true);
  });

  it("accounts that are not v17 markets (random bytes, wrong kind, too short) are silently dropped without counting as unknown layouts", async () => {
    const junk = pk(), portfolioKind = pk(), tiny = pk();
    const wrongKind = market();
    wrongKind[10] = 2;
    const c = conn(new Map<string, Info>([
      [junk.toBase58(), { data: Buffer.alloc(1000, 0xab), owner: PROGRAM }],
      [portfolioKind.toBase58(), { data: Buffer.from(wrongKind), owner: PROGRAM }],
      [tiny.toBase58(), { data: Buffer.alloc(4), owner: PROGRAM }],
    ]));
    expect(await discoverV17Markets(c as never, PROGRAM, [junk, portfolioKind, tiny])).toEqual([]);
    expect(getUnknownLayoutCount()).toBe(0);
  });

  it("a rejection in a later batch rejects the whole call (no partial list returned)", async () => {
    const keys = Array.from({ length: 150 }, () => pk());
    const c = {
      getMultipleAccountsInfo: vi.fn()
        .mockResolvedValueOnce(Array(100).fill(null))
        .mockRejectedValueOnce(new Error("rpc down")),
    };
    await expect(discoverV17Markets(c as never, PROGRAM, keys)).rejects.toThrow("rpc down");
  });
});

describe("discoverV17Markets: scan path", () => {
  const scan = (...rows: Array<{ key: PublicKey; data: Uint8Array }>) => ({
    getProgramAccounts: vi.fn(async () => rows.map((r) => ({ pubkey: r.key, account: { data: Buffer.from(r.data) } }))),
  });

  it("the same slab returned twice by the scan yields two entries (dedupe is the caller's job)", async () => {
    const key = pk();
    const out = await discoverV17Markets(scan({ key, data: market() }, { key, data: market() }) as never, PROGRAM);
    expect(out).toHaveLength(2);
  });

  it("scan results are tagged with the scanned program id, even when the pubkey list mixes accounts", async () => {
    const a = pk(), b = pk();
    const out = await discoverV17Markets(scan({ key: a, data: market() }, { key: b, data: market() }) as never, OTHER);
    expect(out.every((m) => m.programId.equals(OTHER))).toBe(true);
    expect(out.map((m) => m.slabAddress.toBase58())).toEqual([a.toBase58(), b.toBase58()]);
  });

  it("light scan: a header-only slice still parses and yields the asset-0 oracle authority", async () => {
    const authority = pk();
    const full = market({ oracleAuthority: authority, vault: 5n });
    const sliced = full.slice(0, V17_REGISTRATION_SLICE_LEN);
    const key = pk();
    const out = await discoverV17Markets(scan({ key, data: sliced }) as never, PROGRAM, undefined, { light: true });
    expect(out).toHaveLength(1);
    expect(out[0].config.oracleAuthority.toBase58()).toBe(authority.toBase58());
    expect(out[0].engine?.vault).toBe(5n);
  });

  it("no asset slot present: market still returned with a zero oracle authority and price", async () => {
    const key = pk();
    const header = buildMarket(LAYOUT_V22, { slots: 0, vault: 3n });
    const out = await discoverV17Markets(scan({ key, data: header }) as never, PROGRAM);
    expect(out).toHaveLength(1);
    expect(out[0].config.oracleAuthority.equals(PublicKey.default)).toBe(true);
    expect(out[0].config.authorityPriceE6).toBe(0n);
    expect(out[0].engine?.vault).toBe(3n);
  });

  it("a slice that ends inside the asset-0 oracle profile keeps the market with a zero authority", async () => {
    const full = market({ oracleAuthority: pk() });
    const cut = full.slice(0, LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + 40);
    const out = await discoverV17Markets(scan({ key: pk(), data: cut }) as never, PROGRAM);
    expect(out).toHaveLength(1);
    expect(out[0].config.oracleAuthority.equals(PublicKey.default)).toBe(true);
  });

  it("buffer shorter than the group header: skipped (TOO_SHORT), counted as an unknown layout, never thrown", async () => {
    const stubs = [market().slice(0, 20), market().slice(0, 40)];
    for (const data of stubs) {
      const out = await discoverV17Markets(scan({ key: pk(), data }) as never, PROGRAM);
      expect(out).toEqual([]);
    }
    expect(getUnknownLayoutCount()).toBe(2);
  });

  it("one unparseable account does not hide the good ones around it (per-account partial results)", async () => {
    const good1 = pk(), bad = pk(), good2 = pk();
    const unknown = buildMarket(LAYOUT_V22, { slots: 1 }, 20);
    const out = await discoverV17Markets(
      scan({ key: good1, data: market() }, { key: bad, data: unknown }, { key: good2, data: market() }) as never,
      PROGRAM,
    );
    expect(out.map((m) => m.slabAddress.toBase58())).toEqual([good1.toBase58(), good2.toBase58()]);
    expect(getUnknownLayoutCount()).toBe(1);
  });

  it("a scan RPC rejection propagates unchanged (behaviour owned by the scan-failure fix)", async () => {
    const c = { getProgramAccounts: vi.fn().mockRejectedValue(new Error("429 Too Many Requests")) };
    await expect(discoverV17Markets(c as never, PROGRAM)).rejects.toThrow("429 Too Many Requests");
  });
});
