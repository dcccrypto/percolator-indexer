/**
 * v22Events.ts and matcherFill.ts used to carry private base58 decoders; both now use ./fastBase58.ts.
 * Over every instruction `data` string in the recorded devnet txs (TXDIR, default
 * ~/agent-work/scratch-idx-replay/txs, 950 txs) the decoded bytes must equal the `@percolatorct/shared`
 * decodeBase58 that the private copies were pinned to.
 * Skipped when the recording is not on this machine.
 */
import { describe, it, expect } from "vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { decodeBase58 } from "@percolatorct/shared";
import { decodeBase58Fast } from "../../src/parsers/fastBase58.js";
import { decodeBase58Fast as matcherDecode } from "../../src/parsers/matcherFill.js";

const DIR = process.env.TXDIR ?? `${homedir()}/agent-work/scratch-idx-replay/txs`;

function collectData(o: unknown, out: Set<string>): void {
  if (Array.isArray(o)) o.forEach((x) => collectData(x, out));
  else if (o && typeof o === "object") {
    for (const [k, v] of Object.entries(o)) {
      if (k === "data" && typeof v === "string") out.add(v);
      else collectData(v, out);
    }
  }
}

describe.skipIf(!existsSync(DIR))("base58 over the recorded txs", () => {
  const files = existsSync(DIR) ? readdirSync(DIR).filter((f) => f.endsWith(".json") && !f.startsWith("_")).sort() : [];
  const txs = files.map((f) => ({ sig: f.slice(0, -5), tx: JSON.parse(readFileSync(`${DIR}/${f}`, "utf8")) })).filter((x) => x.tx);

  it("covers 950 txs", () => expect(txs.length).toBe(950));

  it("fastBase58 and the matcherFill export are byte-identical to the shared decoder on every data string", { timeout: 180_000 }, () => {
    const all = new Set<string>();
    for (const { tx } of txs) collectData(tx, all);
    expect(all.size).toBeGreaterThan(100);
    for (const s of all) {
      const want = Uint8Array.from(decodeBase58(s) as ArrayLike<number>);
      expect(decodeBase58Fast(s), s).toEqual(want);
      expect(matcherDecode(s), s).toEqual(want);
    }
  });
});
