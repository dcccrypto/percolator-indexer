import { bench, describe } from "vitest";
import { decodeLpVaultEvents } from "../../src/lpVault/decoder.js";
import { buildCorpus, WRAPPERS } from "./lpVaultCorpus.js";

// vitest bench --run tests/bench/lp-vault-decoder.bench.ts
const corpus = buildCorpus(20_000);

describe("lpVault decoder, 20k txs", () => {
  bench("decodeLpVaultEvents", () => {
    let n = 0;
    for (const tx of corpus) n += decodeLpVaultEvents(tx, WRAPPERS).length;
    if (n === 0) throw new Error("no events");
  }, { iterations: 20, warmupIterations: 5 });
});
