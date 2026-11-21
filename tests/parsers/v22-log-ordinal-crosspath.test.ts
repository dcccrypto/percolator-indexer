/**
 * Regression for LIQ-2 (report idx-liquidations-vs-chain-20261120).
 *
 * A keeper sweep that liquidates on TWO markets in one tx (376 of 3,148 recorded devnet txs touch more than one market; this
 * fixture is SYNTHETIC: the real liquidations 3cD7owbs… on 6TfAsN… and 679TEtTy… on 7f52gF… concatenated) is stored TWICE
 * for the second market when both the poll path and the Atlas stream see it. The log-event key is
 * (signature, ix_index, inner_index), with inner_index = 1_000_000 + ordinal (src/parsers/v22FillEvents.ts:428-432), and
 * `ordinal` counts only rows that passed the market gate (`accept`, :476/:508). The poll path's gate is the one polled slab
 * (src/services/TradeIndexer.ts:774), the stream's and webhook's is the whole registry (src/services/EventStreamService.ts:149,
 * src/routes/webhook.ts:600). So 7f52gF's liquidation is (ix 16, inner 1000000) from poll and (ix 16, inner 1000001) from the
 * stream: two v22_events rows, both reason 3, -45,000,000. Real-path proof: the scratch-PG run "outsynthbase" in the report.
 *
 * Failed on e708fc5.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { decodeV22LogEvents } from "../../src/parsers/v22FillEvents.js";
import { rawInstructionsFromParsedTx } from "../../src/parsers/v22Events.js";

const W = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM";
const FX = JSON.parse(readFileSync(new URL("./fixtures/synth-sweep-2mkt.json", import.meta.url), "utf8"));
const A = "6TfAsNSi8dNdNCr4FQnXUmV9aUHD8ciz25gLR1MVVpUa";
const B = "7f52gFQcdhGBorp1DmuThCQ1wiPVMozEW7mTVq37ULbj";

const decode = (known: Set<string>) =>
  decodeV22LogEvents(
    { err: null, logMessages: FX.tx.meta.logMessages },
    rawInstructionsFromParsedTx(FX.tx, [W]),
    new Set([W]),
    { isKnownMarket: (s) => known.has(s), versionOf: () => null, generationOf: () => null },
    { signature: FX.signature, slot: FX.tx.slot, blockTimeSec: FX.tx.blockTime },
  ).rows.filter((r) => r.slab_address === B);

describe("LIQ-2: a log event's dedup key must not depend on which other markets the path knows", () => {
  it("market B's liquidation gets the same (ix_index, inner_index) from the poll gate {B} and the registry gate {A, B}", () => {
    const poll = decode(new Set([B]));
    const stream = decode(new Set([A, B]));
    expect(poll).toHaveLength(1);
    expect(stream).toHaveLength(1);
    expect(poll[0]).toMatchObject({ kind: "reduce_event", actor: "AdNZqnMDAVkXXYGfD6wQTDbuDA6fho2kELGavEQcAN21", amount: "45000000" });
    // FAILS on e708fc5: poll -> [16, 1000000], stream -> [16, 1000001] => two rows under the upsert key
    expect([stream[0].ix_index, stream[0].inner_index]).toEqual([poll[0].ix_index, poll[0].inner_index]);
  });
});
