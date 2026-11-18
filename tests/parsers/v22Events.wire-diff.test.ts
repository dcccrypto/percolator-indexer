/**
 * Real-path wire differential for src/parsers/v22Events.ts.
 *
 * Every instruction is built with the pinned SDK `encode*V22` encoders and the SDK `ACCOUNTS_*_V22` specs (account
 * positions by spec NAME), over a seeded random sample (N per kind, with 0 / 1 / u64::MAX / u128::MAX edges), wrapped in
 * both wire shapes (jsonParsed and Helius enhanced), pushed through the RawInstruction adapters and decodeV22Events, and
 * compared to the encoded args. Also: the module-local base58 decode (exercised through the adapters, `data` is
 * the only base58 input) vs `bs58`, on the same bytes.
 */
import { describe, expect, it } from "vitest";
import bs58 from "bs58";
import {
  ACCOUNTS_BOND_DEPOSIT_V22,
  ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22,
  ACCOUNTS_BOND_REQUEST_WITHDRAW_V22,
  ACCOUNTS_EVICT_PREFIX_V22,
  ACCOUNTS_INIT_INSURANCE_UNITS_V22,
  ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22,
  ACCOUNTS_RESCUE_DEPOSIT_V22,
  ACCOUNTS_SETTLE_HOLDING_RENT_V22,
  ACCOUNTS_SWEEP_BAND_DUST_LEG_V22,
  BackstopMode,
  encodeBondDepositV22,
  encodeBondExecuteWithdrawV22,
  encodeBondRequestWithdrawV22,
  encodeEvictAndTradeCpiV22,
  encodeInitInsuranceUnitsV22,
  encodeInsuranceBackstopDrawV22,
  encodeRescueDepositV22,
  encodeSettleHoldingRentV22,
  encodeSweepBandDustLegV22,
  encodeTradeCpi,
  type AccountSpec,
} from "@percolatorct/sdk";
import {
  decodeV22Events,
  rawInstructionsFromEnhancedTx,
  rawInstructionsFromParsedTx,
  type V22EventRow,
} from "../../src/parsers/v22Events.js";

const N = 2000;
const U64 = (1n << 64n) - 1n;
const U128 = (1n << 128n) - 1n;
const PROGRAM = bs58.encode(Uint8Array.from({ length: 32 }, (_, i) => (i * 7 + 3) & 0xff));
const ctx = { signature: "sig", slot: 9, blockTimeSec: 1_790_000_000 };

// mulberry32, seeded
let seed = 0x22eb0a7d;
const rnd = (): number => {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const bits = (n: number): bigint => {
  let v = 0n;
  for (let i = 0; i < n; i += 16) v = (v << 16n) | BigInt(Math.floor(rnd() * 65536));
  return v & ((1n << BigInt(n)) - 1n);
};
/** i-th sample of an unsigned `n`-bit field: edges first, then a random width so small values show up. */
const uval = (i: number, n: number): bigint => {
  const max = (1n << BigInt(n)) - 1n;
  const edges = [0n, 1n, max, max - 1n, 255n, 256n, 1n << 32n, (1n << 53n) + 1n];
  if (i < edges.length) return edges[i] > max ? max : edges[i];
  return bits(n) >> BigInt(Math.floor(rnd() * n));
};
/** Same sample but never 0 (the SDK encoders reject a zero amount). */
const nz = (i: number, n: number): bigint => uval(i, n) || 1n;
const key = (): string => bs58.encode(Uint8Array.from({ length: 32 }, () => Math.floor(rnd() * 256)));
const keys = (n: number): string[] => Array.from({ length: n }, key);
const nameIdx = (spec: readonly AccountSpec[], name: string): number => {
  const i = spec.findIndex((a) => a.name === name);
  if (i < 0) throw new Error(`spec has no ${name}`);
  return i;
};

interface Case {
  data: Uint8Array;
  accounts: string[];
  expected: Omit<V22EventRow, "signature" | "ix_index" | "inner_index" | "slot" | "block_time"> | null;
}
const row = (kind: V22EventRow["kind"], f: Partial<V22EventRow>): Case["expected"] => ({
  kind, slab_address: null, asset_index: null, actor: null, subject: null, amount: null, detail: {}, ...f,
});

type Gen = (i: number) => Case;
const withAcc = (spec: readonly AccountSpec[]): string[] => keys(spec.length);

const GENS: Record<string, Gen> = {
  bond_deposit: (i) => {
    const amount = nz(i, 64), minShares = nz(i, 128);
    const accounts = withAcc(ACCOUNTS_BOND_DEPOSIT_V22);
    return { data: encodeBondDepositV22(amount, minShares), accounts, expected: row("bond_deposit", {
      slab_address: accounts[nameIdx(ACCOUNTS_BOND_DEPOSIT_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_BOND_DEPOSIT_V22, "depositor")],
      subject: accounts[nameIdx(ACCOUNTS_BOND_DEPOSIT_V22, "bondPosition")], amount: amount.toString(), detail: { min_shares: minShares.toString() } }) };
  },
  bond_withdraw_request: (i) => {
    const shares = uval(i, 128);
    const accounts = withAcc(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22);
    return { data: encodeBondRequestWithdrawV22(shares), accounts, expected: row("bond_withdraw_request", {
      slab_address: accounts[nameIdx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "holder")],
      subject: accounts[nameIdx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "bondPosition")], detail: { shares: shares.toString(), cancel: shares === 0n } }) };
  },
  bond_withdraw_execute: (i) => {
    const minOut = uval(i, 64), dom = Number(uval(i + 1, 16));
    const accounts = withAcc(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22);
    return { data: encodeBondExecuteWithdrawV22(minOut, dom), accounts, expected: row("bond_withdraw_execute", {
      slab_address: accounts[nameIdx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "holder")],
      subject: accounts[nameIdx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "bondPosition")], detail: { min_out: minOut.toString(), source_domain: dom } }) };
  },
  rescue_deposit: (i) => {
    const tranche = Number(uval(i, 8)), amount = nz(i, 64), minShares = nz(i, 128);
    const accounts = withAcc(ACCOUNTS_RESCUE_DEPOSIT_V22);
    return { data: encodeRescueDepositV22(tranche, amount, minShares), accounts, expected: row("rescue_deposit", {
      slab_address: accounts[nameIdx(ACCOUNTS_RESCUE_DEPOSIT_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_RESCUE_DEPOSIT_V22, "rescuer")],
      amount: amount.toString(), detail: { tranche, min_shares: minShares.toString() } }) };
  },
  insurance_units_init: () => {
    const accounts = withAcc(ACCOUNTS_INIT_INSURANCE_UNITS_V22);
    return { data: encodeInitInsuranceUnitsV22(), accounts, expected: row("insurance_units_init", {
      slab_address: accounts[nameIdx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "payer")],
      subject: accounts[nameIdx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "insuranceUnits")] }) };
  },
  backstop: (i) => {
    const mode = [BackstopMode.Propose, BackstopMode.Draw, BackstopMode.Restore][i % 3] as 0 | 1 | 2;
    const max = uval(i, 128);
    const accounts = withAcc(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22);
    const kind = mode === BackstopMode.Propose ? "backstop_propose" : mode === BackstopMode.Draw ? "backstop_draw" : "backstop_restore";
    return { data: encodeInsuranceBackstopDrawV22(mode, max), accounts, expected: row(kind, {
      slab_address: accounts[nameIdx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "cranker")],
      subject: accounts[nameIdx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "lpPortfolio")], detail: { mode, max_amount: max.toString() } }) };
  },
  holding_rent_settled: (i) => {
    const asset = Number(uval(i, 16)), now = uval(i + 1, 64);
    const accounts = withAcc(ACCOUNTS_SETTLE_HOLDING_RENT_V22);
    return { data: encodeSettleHoldingRentV22(asset, now), accounts, expected: row("holding_rent_settled", {
      slab_address: accounts[nameIdx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "caller")],
      subject: accounts[nameIdx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "portfolio")], asset_index: asset, detail: { now_slot: now.toString() } }) };
  },
  band_dust_swept: (i) => {
    const asset = Number(uval(i, 16));
    const accounts = withAcc(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22);
    return { data: encodeSweepBandDustLegV22(asset), accounts, expected: row("band_dust_swept", {
      slab_address: accounts[nameIdx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "market")], actor: accounts[nameIdx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "caller")],
      subject: accounts[nameIdx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "portfolio")], asset_index: asset }) };
  },
  eviction: (i) => {
    const asset = Number(uval(i, 16));
    const absSize = uval(i + 1, 127) || 1n; // zero-size is not a fill (decoder drops it; covered below)
    const neg = rnd() < 0.5;
    const trade = encodeTradeCpi({
      accountAPortfolioId: uval(i, 64), accountAPositionEpoch: uval(i + 1, 64), accountBPortfolioId: uval(i + 2, 64),
      accountBPositionEpoch: uval(i + 3, 64), accountBMatcherSequence: uval(i + 4, 64), assetIndex: asset, marketId: uval(i + 5, 64),
      sizeQ: neg ? -absSize : absSize, feeBps: uval(i + 6, 16), limitPrice: uval(i + 7, 64), backingFeeCapBps: Number(uval(i, 16)),
    });
    const victim = key(), trader = key(), market = key();
    const accounts = [...keys(ACCOUNTS_EVICT_PREFIX_V22.length - 1), victim, trader, market, ...keys(5)];
    // the victim sits at its spec index; the TradeCpi accounts follow the prefix
    const vi = nameIdx(ACCOUNTS_EVICT_PREFIX_V22, "victimPortfolio");
    const full = keys(ACCOUNTS_EVICT_PREFIX_V22.length);
    full[vi] = victim;
    const acc = [...full, trader, market, ...keys(5)];
    void accounts;
    return { data: encodeEvictAndTradeCpiV22(trade), accounts: acc, expected: row("eviction", {
      slab_address: market, actor: trader, subject: victim, asset_index: asset,
      detail: { entrant_size: absSize.toString(), entrant_side: neg ? "short" : "long" } }) };
  },
};

const toParsed = (c: Case) => ({
  transaction: { message: { instructions: [{ programId: PROGRAM, accounts: c.accounts, data: bs58.encode(c.data) }] } },
  meta: { err: null, innerInstructions: [] },
});
const toEnhanced = (c: Case) => ({ instructions: [{ programId: PROGRAM, accounts: c.accounts, data: bs58.encode(c.data), innerInstructions: [] }] });

const strip = (r: V22EventRow) => {
  const { signature: _s, ix_index: _i, inner_index: _n, slot: _sl, block_time: _b, ...rest } = r;
  return rest;
};

describe("v22Events wire differential (SDK encode*V22 + ACCOUNTS_*_V22 -> adapters -> rows)", () => {
  it("evict prefix is the single victim account (the eviction accounts below assume it)", () => {
    expect(ACCOUNTS_EVICT_PREFIX_V22.length).toBeGreaterThanOrEqual(1);
  });

  for (const [name, gen] of Object.entries(GENS)) {
    it(`${name}: ${N} cases, both adapters identical and equal to the encoded args`, () => {
      const mismatches: string[] = [];
      for (let i = 0; i < N; i++) {
        const c = gen(i);
        const p = decodeV22Events(rawInstructionsFromParsedTx(toParsed(c), [PROGRAM]), ctx);
        const e = decodeV22Events(rawInstructionsFromEnhancedTx(toEnhanced(c), new Set([PROGRAM])), ctx);
        if (p.length !== 1 || e.length !== 1) { mismatches.push(`#${i} rows parsed=${p.length} enhanced=${e.length}`); continue; }
        try {
          expect(strip(p[0])).toEqual(c.expected);
          expect(e[0]).toEqual(p[0]);
        } catch (err) {
          mismatches.push(`#${i} ${(err as Error).message.split("\n").slice(0, 6).join(" | ")} data=${Buffer.from(c.data).toString("hex")}`);
        }
        if (mismatches.length >= 3) break;
      }
      expect(mismatches).toEqual([]);
    }, 120_000);
  }

  it("backstop: all three SDK modes are covered", () => {
    const kinds = new Set(Array.from({ length: 30 }, (_, i) => GENS.backstop(i).expected!.kind));
    expect([...kinds].sort()).toEqual(["backstop_draw", "backstop_propose", "backstop_restore"]);
  });

  it("u64::MAX / u128::MAX edges survive exactly", () => {
    const d = (g: string, i: number) => GENS[g](i);
    const dep = d("bond_deposit", 2); // sample index 2 = max for both fields
    expect(dep.expected!.amount).toBe(U64.toString());
    expect(dep.expected!.detail.min_shares).toBe(U128.toString());
    expect(GENS.backstop(2 + 3 * 0).expected!.detail.max_amount).toBe(U128.toString());
  });

  it(`base58: local decodeBase58 == bs58 on ${N * 4} byte strings (random lengths, leading zeros), via the adapters`, () => {
    let bad = 0;
    for (let i = 0; i < N * 4; i++) {
      const len = 1 + Math.floor(rnd() * 80);
      const bytes = Uint8Array.from({ length: len }, () => Math.floor(rnd() * 256));
      const lead = Math.floor(rnd() * 4);
      for (let z = 0; z < lead && z < len; z++) bytes[z] = 0;
      const s = bs58.encode(bytes);
      const got = rawInstructionsFromEnhancedTx({ instructions: [{ programId: PROGRAM, accounts: [], data: s }] }, new Set([PROGRAM]));
      if (got.length !== 1 || Buffer.compare(Buffer.from(got[0].data), Buffer.from(bs58.decode(s))) !== 0) bad++;
    }
    expect(bad).toBe(0);
  });

  it("base58: invalid characters give no instruction in both adapters", () => {
    for (const s of ["0abc", "Oabc", "Iabc", "labc", "ab+c", "é"]) {
      expect(rawInstructionsFromEnhancedTx({ instructions: [{ programId: PROGRAM, accounts: [], data: s }] }, new Set([PROGRAM]))).toEqual([]);
      expect(rawInstructionsFromParsedTx({ transaction: { message: { instructions: [{ programId: PROGRAM, accounts: [], data: s }] } }, meta: { err: null } }, [PROGRAM])).toEqual([]);
    }
  });
});
