import { bench, describe } from "vitest";
import * as next from "../../src/parsers/percolatorTxParser.js";
import * as ref from "../parsers/percolatorTxParser.reference.js";
import { makeTxs } from "../parsers/percolatorTxParser.fixtures.js";

const N = 20_000;
const { txs, programIds } = makeTxs(1234, N);

function pass(m: typeof next): number {
  let n = 0;
  for (const { tx, signature } of txs) {
    n += m.parsePercolatorFills(tx, signature, programIds).length;
    n += m.parsePercolatorLiquidations(tx, signature, programIds).length;
  }
  return n;
}

describe("tx-parser 20k", () => {
  // same process, alternating, so machine load hits both equally
  const opts = { iterations: 5, warmupIterations: 1, time: 0 };
  bench("reference a6e05f8", () => { pass(ref as never); }, opts);
  bench("new", () => { pass(next); }, opts);
});
