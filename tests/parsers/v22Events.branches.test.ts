import { describe, expect, it } from "vitest";
import { BackstopMode, IX_TAG_V22, encodeTradeCpi } from "@percolatorct/sdk";
import { encodeBase58 } from "../../src/lib/base58.js";
import {
  V22_EVENT_TAGS, decodeV22Event, decodeV22Events, rawInstructionsFromEnhancedTx, rawInstructionsFromParsedTx,
  type RawInstruction,
} from "../../src/parsers/v22Events.js";

// Hand-built wire payloads so every length guard, account guard and u64/u128 edge is hit without the SDK builders.
const U64_MAX = 2n ** 64n - 1n;
const U128_MAX = 2n ** 128n - 1n;
const ctx = { signature: "sigB" };
const accts = Array.from({ length: 24 }, (_, i) => `Acct${i}`);

function le(v: bigint, bytes: number): number[] {
  const out: number[] = [];
  for (let i = 0; i < bytes; i++) out.push(Number((v >> BigInt(8 * i)) & 0xffn));
  return out;
}
const ix = (data: number[], accounts: string[] = accts): RawInstruction => ({ programId: "P", accounts, data: Uint8Array.from(data), ixIndex: 2, innerIndex: -1 });

const T = IX_TAG_V22;
// [tag, payload-builder] per fixed-size kind
const kinds: Array<[string, number[]]> = [
  ["bond_deposit", [T.BondDeposit, ...le(1n, 8), ...le(2n, 16)]],
  ["bond_withdraw_request", [T.BondRequestWithdraw, ...le(5n, 16)]],
  ["bond_withdraw_execute", [T.BondExecuteWithdraw, ...le(5n, 8), ...le(1n, 2)]],
  ["rescue_deposit", [T.RescueDeposit, 1, ...le(5n, 8), ...le(9n, 16)]],
  ["backstop_draw", [T.InsuranceBackstopDraw, BackstopMode.Draw, ...le(5n, 16)]],
  ["holding_rent_settled", [T.SettleHoldingRent, ...le(3n, 2), ...le(5n, 8)]],
  ["band_dust_swept", [T.SweepBandDustLeg, ...le(3n, 2)]],
];

describe("decodeV22Event branch coverage", () => {
  it("empty data and unknown tags are dropped", () => {
    expect(decodeV22Event(ix([]), ctx)).toBeNull();
    expect(decodeV22Event(ix([0]), ctx)).toBeNull();
    expect(decodeV22Event(ix([255, 1, 2, 3]), ctx)).toBeNull();
  });

  for (const [kind, data] of kinds) {
    it(`${kind}: every truncation of the payload is dropped, the full payload decodes`, () => {
      for (let n = 1; n < data.length; n++) expect(decodeV22Event(ix(data.slice(0, n)), ctx), `len ${n}`).toBeNull();
      const e = decodeV22Event(ix(data), ctx);
      expect(e?.kind).toBe(kind);
    });
    it(`${kind}: no accounts means no row`, () => {
      expect(decodeV22Event(ix(data, []), ctx)).toBeNull();
    });
  }

  it("a blank (unresolvable) market account drops the row; a blank actor is kept as null", () => {
    const data = kinds[0][1];
    const full = decodeV22Event(ix(data), ctx)!;
    const marketIdx = accts.indexOf(full.slab_address!);
    const noMarket = accts.slice(); noMarket[marketIdx] = "";
    expect(decodeV22Event(ix(data, noMarket), ctx)).toBeNull();
    const actorIdx = accts.indexOf(full.actor!);
    const noActor = accts.slice(); noActor[actorIdx] = "";
    const e = decodeV22Event(ix(data, noActor), ctx)!;
    expect(e.actor).toBeNull();
    expect(e.slab_address).toBe(full.slab_address);
  });

  it("zero and max u64 / u128 values round-trip exactly", () => {
    const zero = decodeV22Event(ix([T.BondDeposit, ...le(0n, 8), ...le(0n, 16)]), ctx)!;
    expect([zero.amount, zero.detail.min_shares]).toEqual(["0", "0"]);
    const max = decodeV22Event(ix([T.BondDeposit, ...le(U64_MAX, 8), ...le(U128_MAX, 16)]), ctx)!;
    expect([max.amount, max.detail.min_shares]).toEqual([U64_MAX.toString(), U128_MAX.toString()]);
    const rent = decodeV22Event(ix([T.SettleHoldingRent, 0xff, 0xff, ...le(U64_MAX, 8)]), ctx)!;
    expect([rent.asset_index, rent.detail.now_slot]).toEqual([65535, U64_MAX.toString()]);
    const exec = decodeV22Event(ix([T.BondExecuteWithdraw, ...le(U64_MAX, 8), 0xff, 0xff]), ctx)!;
    expect([exec.detail.min_out, exec.detail.source_domain]).toEqual([U64_MAX.toString(), 65535]);
    const req = decodeV22Event(ix([T.BondRequestWithdraw, ...le(U128_MAX, 16)]), ctx)!;
    expect([req.detail.shares, req.detail.cancel]).toEqual([U128_MAX.toString(), false]);
    const zeroReq = decodeV22Event(ix([T.BondRequestWithdraw, ...le(0n, 16)]), ctx)!;
    expect(zeroReq.detail.cancel).toBe(true);
  });

  it("insurance_units_init needs only the tag byte", () => {
    const e = decodeV22Event(ix([T.InitInsuranceUnits]), ctx)!;
    expect(e.kind).toBe("insurance_units_init");
    expect(e.amount).toBeNull();
    expect(decodeV22Event(ix([T.InitInsuranceUnits], []), ctx)).toBeNull();
  });

  it("backstop modes: propose/draw/restore map to kinds, any other mode is dropped", () => {
    const mk = (mode: number) => decodeV22Event(ix([T.InsuranceBackstopDraw, mode, ...le(7n, 16)]), ctx);
    expect(mk(BackstopMode.Propose)?.kind).toBe("backstop_propose");
    expect(mk(BackstopMode.Draw)?.kind).toBe("backstop_draw");
    expect(mk(BackstopMode.Restore)?.kind).toBe("backstop_restore");
    for (const bad of [3, 4, 100, 255]) expect(mk(bad)).toBeNull();
  });

  it("block_time is ISO when blockTimeSec is given (including 0) and null otherwise; slot defaults to null", () => {
    const data = [T.InitInsuranceUnits];
    expect(decodeV22Event(ix(data), { signature: "s", blockTimeSec: 0, slot: 0 })).toMatchObject({ block_time: "1970-01-01T00:00:00.000Z", slot: 0 });
    expect(decodeV22Event(ix(data), { signature: "s", blockTimeSec: null, slot: null })).toMatchObject({ block_time: null, slot: null });
    expect(decodeV22Event(ix(data), ctx)).toMatchObject({ block_time: null, slot: null, ix_index: 2, inner_index: -1 });
  });

  it("eviction: each guard (valid wrapped TradeCpi body) drops the row on its own", () => {
    const body = encodeTradeCpi({
      accountAPortfolioId: 1n, accountAPositionEpoch: 2n, accountBPortfolioId: 3n, accountBPositionEpoch: 4n, accountBMatcherSequence: 5n,
      assetIndex: 4, marketId: 9n, sizeQ: 1_000_000n, feeBps: 5n, limitPrice: 100_000_000n, backingFeeCapBps: 0,
    });
    const data = [T.EvictAndTradeCpi, ...body.slice(1)]; // the wrapper replaces the inner tag byte
    const good = ["victim", "trader", "market"];
    const e = decodeV22Event(ix(data, good), ctx)!;
    expect([e.kind, e.subject, e.actor, e.slab_address, e.asset_index, e.detail.entrant_side]).toEqual(["eviction", "victim", "trader", "market", 4, "long"]);
    expect(decodeV22Event(ix(data, []), ctx)).toBeNull(); // no accounts
    expect(decodeV22Event(ix(data, ["victim"]), ctx)).toBeNull(); // fewer than 2 accounts
    expect(decodeV22Event(ix(data, ["", "trader", "market"]), ctx)).toBeNull(); // blank victim
    expect(decodeV22Event(ix(data, ["victim", "trader"]), ctx)).toBeNull(); // no market account
    expect(decodeV22Event(ix(data, ["victim", "trader", ""]), ctx)).toBeNull(); // blank market
    expect(decodeV22Event(ix([T.EvictAndTradeCpi], good), ctx)).toBeNull(); // no wrapped body
    expect(decodeV22Event(ix([T.EvictAndTradeCpi, 1, 2, 3], good), ctx)).toBeNull(); // truncated body
  });

  it("the switch default is unreachable for known tags, but a tag added to the set without a case is dropped", () => {
    const set = V22_EVENT_TAGS as Set<number>;
    set.add(200);
    try {
      expect(decodeV22Event(ix([200, 0, 0]), ctx)).toBeNull();
    } finally {
      set.delete(200);
    }
    expect(V22_EVENT_TAGS.has(200)).toBe(false);
  });
});

describe("decodeV22Events", () => {
  it("skips non-events and swallows a throwing instruction, keeping the rest", () => {
    const good = ix([T.InitInsuranceUnits]);
    const throwing = { ...good, get data(): Uint8Array { throw new Error("boom"); } } as unknown as RawInstruction;
    const out = decodeV22Events([ix([255]), throwing, good], ctx);
    expect(out).toHaveLength(1);
    expect(out[0].kind).toBe("insurance_units_init");
  });
});

describe("rawInstructionsFromParsedTx edge cases", () => {
  const prog = "Prog1";
  const data = encodeBase58(Uint8Array.from([T.InitInsuranceUnits]));
  const ids = [prog];

  it("missing meta, failed tx, and empty tx yield nothing", () => {
    expect(rawInstructionsFromParsedTx({}, ids)).toEqual([]);
    expect(rawInstructionsFromParsedTx({ meta: null }, ids)).toEqual([]);
    expect(rawInstructionsFromParsedTx({ meta: { err: { InstructionError: [0, "x"] } } }, ids)).toEqual([]);
    expect(rawInstructionsFromParsedTx({ meta: { err: null } }, ids)).toEqual([]);
  });

  it("skips null, non-object, parsed, foreign, keyless and dataless instructions", () => {
    const keyObj = { toBase58: () => prog };
    const wrapped = { pubkey: { toBase58: () => "Wrapped" } };
    const tx = {
      transaction: { message: { instructions: [
        null, "str", { parsed: {}, programId: prog, data },
        { programId: "Other", data }, { data }, { programId: prog, data: 123 },
        { programId: keyObj, data, accounts: ["a", keyObj, wrapped, null, {}] },
        { programId: { pubkey: keyObj }, data }, // nested pubkey, no accounts
      ] } },
      meta: { err: null, innerInstructions: [{ index: 1, instructions: [{ programId: prog, data, accounts: ["z"] }] }, { index: 3, instructions: undefined as never }] },
    };
    const out = rawInstructionsFromParsedTx(tx, ids);
    expect(out.map((o) => [o.ixIndex, o.innerIndex])).toEqual([[6, -1], [7, -1], [1, 0]]);
    expect(out[0].accounts).toEqual(["a", prog, "Wrapped", "", ""]);
    expect(out[1].accounts).toEqual([]);
    expect(out[2].accounts).toEqual(["z"]);
  });

  it("works with no instruction arrays at all", () => {
    expect(rawInstructionsFromParsedTx({ meta: { err: null } }, ids)).toEqual([]);
    expect(rawInstructionsFromParsedTx({ transaction: {}, meta: { err: null, innerInstructions: [] } }, ids)).toEqual([]);
  });
});

describe("rawInstructionsFromEnhancedTx edge cases", () => {
  const prog = "Prog1";
  const data = encodeBase58(Uint8Array.from([T.InitInsuranceUnits]));
  const set = new Set([prog]);

  it("empty tx, foreign program, missing data and non-array accounts", () => {
    expect(rawInstructionsFromEnhancedTx({}, set)).toEqual([]);
    const out = rawInstructionsFromEnhancedTx({ instructions: [
      { programId: "Other", data }, { programId: prog }, { data }, { programId: prog, data, accounts: "nope" as never },
    ] }, set);
    expect(out).toHaveLength(1);
    expect(out[0].accounts).toEqual([]);
  });

  it("nested inner instructions win; the flat groups are then ignored", () => {
    const out = rawInstructionsFromEnhancedTx({
      instructions: [{ programId: prog, data, innerInstructions: [{ programId: prog, data }] }],
      innerInstructions: [{ instructions: [{ programId: prog, data }] }],
    }, set);
    expect(out.map((o) => [o.ixIndex, o.innerIndex])).toEqual([[0, -1], [0, 0]]);
  });

  it("flat inner groups are read (ix_index 1000+group) when nothing is nested", () => {
    const out = rawInstructionsFromEnhancedTx({
      instructions: [{ programId: prog, data }],
      innerInstructions: [{ instructions: [{ programId: prog, data }, { programId: "Other", data }] }, {}],
    }, set);
    expect(out.map((o) => [o.ixIndex, o.innerIndex])).toEqual([[0, -1], [1000, 0]]);
  });
});
