import { describe, expect, it } from "vitest";
import {
  BackstopMode, IX_TAG, buildBondDepositIxV22, buildBondExecuteWithdrawIxV22, buildBondRequestWithdrawIxV22, buildEvictAndTradeCpiIxV22,
  buildInitInsuranceUnitsIxV22, buildInsuranceBackstopDrawIxV22, buildRescueDepositIxV22, buildSettleHoldingRentIxV22, buildSweepBandDustLegIxV22,
  encodeTradeCpi,
} from "@percolatorct/sdk";
import { TransactionInstruction } from "@solana/web3.js";
import { V22_EVENT_KINDS, V22_EVENT_TAGS, decodeV22Events, rawInstructionsFromEnhancedTx, rawInstructionsFromParsedTx } from "../../src/parsers/v22Events.js";
import { parsePercolatorFills } from "../../src/parsers/percolatorTxParser.js";
import { enhancedTx, parsedTx, pk } from "../helpers/v22Fixtures.js";

const PROGRAM = pk();
const market = pk(), lp = pk(), user = pk(), keeper = pk(), portfolio = pk(), ata = pk(), vault = pk(), victim = pk(), taker = pk();
const m = { programId: PROGRAM, market, registryDomain: 0, lpPortfolio: lp };
const ctx = { signature: "sigV22", slot: 4242, blockTimeSec: 1_790_000_000 };

const built = {
  bondDeposit: buildBondDepositIxV22(m, user, ata, vault, 5_000_000n, 4_900_000n),
  bondRequest: buildBondRequestWithdrawIxV22(m, user, 777n),
  bondCancel: buildBondRequestWithdrawIxV22(m, user, 0n),
  bondExecute: buildBondExecuteWithdrawIxV22(m, user, ata, vault, 123n, 1),
  rescue: buildRescueDepositIxV22(m, user, ata, ata, vault, 100_000_000n, 90_000_000n),
  units: buildInitInsuranceUnitsIxV22(m, keeper),
  propose: buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Propose, 0n),
  draw: buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Draw, 2n ** 128n - 1n),
  restore: buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Restore, 55n),
  rent: buildSettleHoldingRentIxV22(m, keeper, portfolio, 3, 123_456_789n),
  dust: buildSweepBandDustLegIxV22(m, keeper, portfolio, 2),
};

function tradeCpi(): TransactionInstruction {
  // a TradeCpi built by the SDK's own encoder; account order per the indexer's TradeCpi convention: [0] trader, [1] market
  const data = encodeTradeCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 2n, accountBPortfolioId: 3n, accountBPositionEpoch: 4n, accountBMatcherSequence: 5n,
    assetIndex: 4, marketId: 9n, sizeQ: -1_000_000n, feeBps: 5n, limitPrice: 100_000_000n, backingFeeCapBps: 0,
  });
  return new TransactionInstruction({ programId: PROGRAM, keys: [{ pubkey: taker, isSigner: true, isWritable: true }, { pubkey: market, isSigner: false, isWritable: true }], data: Buffer.from(data) });
}

describe("decodeV22Events (SDK-built instructions, both ingestion shapes)", () => {
  const cases: Array<[string, TransactionInstruction, (e: ReturnType<typeof decodeV22Events>[number]) => void]> = [
    ["bond_deposit", built.bondDeposit, (e) => { expect([e.actor, e.amount, e.detail.min_shares]).toEqual([user.toBase58(), "5000000", "4900000"]); expect(e.subject).not.toBeNull(); }],
    ["bond_withdraw_request", built.bondRequest, (e) => expect([e.detail.shares, e.detail.cancel]).toEqual(["777", false])],
    ["bond_withdraw_request", built.bondCancel, (e) => expect([e.detail.shares, e.detail.cancel]).toEqual(["0", true])],
    ["bond_withdraw_execute", built.bondExecute, (e) => expect([e.detail.min_out, e.detail.source_domain]).toEqual(["123", 1])],
    ["rescue_deposit", built.rescue, (e) => expect([e.actor, e.amount, e.detail.tranche, e.detail.min_shares]).toEqual([user.toBase58(), "100000000", 0, "90000000"])],
    ["insurance_units_init", built.units, (e) => expect(e.actor).toBe(keeper.toBase58())],
    ["backstop_propose", built.propose, (e) => expect(e.detail.mode).toBe(2)],
    ["backstop_draw", built.draw, (e) => expect(e.detail.max_amount).toBe((2n ** 128n - 1n).toString())],
    ["backstop_restore", built.restore, (e) => expect(e.detail.max_amount).toBe("55")],
    ["holding_rent_settled", built.rent, (e) => { expect([e.asset_index, e.subject, e.detail.now_slot]).toEqual([3, portfolio.toBase58(), "123456789"]); }],
    ["band_dust_swept", built.dust, (e) => expect([e.asset_index, e.subject]).toEqual([2, portfolio.toBase58()])],
  ];
  for (const [kind, ix, check] of cases) {
    it(`${kind} (tag ${ix.data[0]})`, () => {
      for (const raw of [rawInstructionsFromParsedTx(parsedTx([ix]), [PROGRAM.toBase58()]), rawInstructionsFromEnhancedTx(enhancedTx([ix]), new Set([PROGRAM.toBase58()]))]) {
        const [e, ...rest] = decodeV22Events(raw, ctx);
        expect(rest).toHaveLength(0);
        expect(e.kind).toBe(kind);
        expect(e.slab_address).toBe(market.toBase58());
        expect([e.signature, e.ix_index, e.inner_index, e.slot]).toEqual(["sigV22", 0, -1, 4242]);
        check(e);
      }
    });
  }

  it("eviction (119): victim = subject, entrant = actor, entrant size/side from the wrapped TradeCpi, and the fill itself is still indexed", () => {
    const evict = buildEvictAndTradeCpiIxV22(tradeCpi(), victim);
    expect(evict.data[0]).toBe(119);
    const [e] = decodeV22Events(rawInstructionsFromParsedTx(parsedTx([evict]), [PROGRAM.toBase58()]), ctx);
    expect([e.kind, e.subject, e.actor, e.slab_address, e.asset_index, e.detail.entrant_size, e.detail.entrant_side]).toEqual(["eviction", victim.toBase58(), taker.toBase58(), market.toBase58(), 4, "1000000", "short"]);
    const fills = parsePercolatorFills(parsedTx([evict]) as never, "sigV22", [PROGRAM.toBase58()]);
    expect(fills).toHaveLength(1);
    expect([fills[0].trader, fills[0].slabAddress, fills[0].assetIndex, fills[0].sizeAbs, fills[0].side]).toEqual([taker.toBase58(), market.toBase58(), 4, 1_000_000n, "short"]);
  });

  it("an eviction is rejected (not misread) when its accounts are only the victim", () => {
    const evict = buildEvictAndTradeCpiIxV22(tradeCpi(), victim);
    const short = new TransactionInstruction({ programId: PROGRAM, keys: [evict.keys[0]], data: evict.data });
    expect(decodeV22Events(rawInstructionsFromParsedTx(parsedTx([short]), [PROGRAM.toBase58()]), ctx)).toEqual([]);
    expect(parsePercolatorFills(parsedTx([short]) as never, "s", [PROGRAM.toBase58()])).toEqual([]);
  });

  it("inner instructions (CPI) are decoded with their own inner_index; the nested enhanced shape is not double counted", () => {
    const rawP = rawInstructionsFromParsedTx(parsedTx([built.units], { inner: [built.bondDeposit] }), [PROGRAM.toBase58()]);
    const ev = decodeV22Events(rawP, ctx);
    expect(ev.map((e) => [e.kind, e.ix_index, e.inner_index])).toEqual([["insurance_units_init", 0, -1], ["bond_deposit", 0, 0]]);
    const ev2 = decodeV22Events(rawInstructionsFromEnhancedTx(enhancedTx([built.units], { nested: [built.bondDeposit] }), new Set([PROGRAM.toBase58()])), ctx);
    expect(ev2.map((e) => [e.kind, e.ix_index, e.inner_index])).toEqual([["insurance_units_init", 0, -1], ["bond_deposit", 0, 0]]);
  });

  it("NEGATIVE CONTROLS: failed tx, foreign program, truncated data, refused backstop mode, non-event tags, unrelated tag 105", () => {
    expect(rawInstructionsFromParsedTx(parsedTx([built.bondDeposit], { err: { InstructionError: [0, "Custom"] } }), [PROGRAM.toBase58()])).toEqual([]);
    expect(rawInstructionsFromParsedTx(parsedTx([built.bondDeposit]), [pk().toBase58()])).toEqual([]);
    const trunc = new TransactionInstruction({ programId: PROGRAM, keys: built.bondDeposit.keys, data: built.bondDeposit.data.subarray(0, 12) });
    expect(decodeV22Events(rawInstructionsFromParsedTx(parsedTx([trunc]), [PROGRAM.toBase58()]), ctx)).toEqual([]);
    const badMode = Buffer.from(built.propose.data); badMode[1] = 3;
    const bm = new TransactionInstruction({ programId: PROGRAM, keys: built.propose.keys, data: badMode });
    expect(decodeV22Events(rawInstructionsFromParsedTx(parsedTx([bm]), [PROGRAM.toBase58()]), ctx)).toEqual([]);
    const other = new TransactionInstruction({ programId: PROGRAM, keys: built.rent.keys, data: Buffer.from([105, 0, 0]) });
    expect(decodeV22Events(rawInstructionsFromParsedTx(parsedTx([other]), [PROGRAM.toBase58()]), ctx)).toEqual([]);
    const noMarket = new TransactionInstruction({ programId: PROGRAM, keys: built.rent.keys.slice(0, 1), data: built.rent.data });
    expect(decodeV22Events(rawInstructionsFromParsedTx(parsedTx([noMarket]), [PROGRAM.toBase58()]), ctx)).toEqual([]);
  });

  it("one malformed instruction does not lose the others in the same tx", () => {
    const trunc = new TransactionInstruction({ programId: PROGRAM, keys: built.bondDeposit.keys, data: built.bondDeposit.data.subarray(0, 5) });
    const ev = decodeV22Events(rawInstructionsFromParsedTx(parsedTx([trunc, built.dust]), [PROGRAM.toBase58()]), ctx);
    expect(ev.map((e) => e.kind)).toEqual(["band_dust_swept"]);
  });

  it("tag set and kind list are exactly the nine tags / eleven kinds documented", () => {
    expect([...V22_EVENT_TAGS].sort((a, b) => a - b)).toEqual([106, 108, 109, 110, 111, 112, 116, 118, 119]);
    expect(V22_EVENT_KINDS).toHaveLength(11);
    void IX_TAG;
  });
});
