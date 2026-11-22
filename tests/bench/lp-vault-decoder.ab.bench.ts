import { bench, describe } from "vitest";
import { decodeLpVaultEvents } from "../../src/lpVault/decoder.js";
import { decodeLpVaultEvents as legacy } from "./decoderLegacy.js";
import { buildCorpus, WRAPPERS } from "./lpVaultCorpus.js";

// Same-process A/B: vitest bench --run tests/bench/lp-vault-decoder.ab.bench.ts
const corpus = buildCorpus(20_000);
const pass = (f: typeof decodeLpVaultEvents) => () => {
  let n = 0;
  for (const tx of corpus) n += f(tx, WRAPPERS).length;
  if (n === 0) throw new Error("no events");
};
describe("A/B 20k txs", () => {
  bench("legacy", pass(legacy), { iterations: 40, warmupIterations: 5 });
  bench("current", pass(decodeLpVaultEvents), { iterations: 40, warmupIterations: 5 });
});
