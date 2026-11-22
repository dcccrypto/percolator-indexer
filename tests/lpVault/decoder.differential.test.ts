import { describe, it, expect } from "vitest";
import { decodeLpVaultEvents } from "../../src/lpVault/decoder.js";
import { decodeLpVaultEvents as legacyDecode } from "../bench/decoderLegacy.js";
import { loadFixtureTxs, synthTxs, rng, WRAPPERS } from "../bench/lpVaultCorpus.js";
import type { NormTx, NormIx } from "../../src/lpVault/decoder.js";

/** Old (frozen) decoder vs current one: same events, or the same error. */
function run(f: typeof decodeLpVaultEvents, tx: NormTx): { ok: unknown } | { err: string } {
  try { return { ok: f(tx, WRAPPERS) }; } catch (e) { return { err: String((e as Error).message) }; }
}
function same(tx: NormTx): void {
  expect(run(decodeLpVaultEvents, tx)).toStrictEqual(run(legacyDecode, tx));
}

describe("decoder differential vs frozen legacy copy", { timeout: 120_000 }, () => {
  it("all recorded fixtures", () => {
    const fx = loadFixtureTxs();
    expect(fx.length).toBeGreaterThan(0);
    for (const tx of fx) same(tx);
  });

  it("synthetic mixed corpus (incl. mismatched amounts, null heights, nested CPIs, failed txs)", () => {
    let events = 0;
    for (const tx of synthTxs(5000, 7)) {
      same(tx);
      const r = run(decodeLpVaultEvents, tx);
      if ("ok" in r) events += (r.ok as unknown[]).length;
    }
    expect(events).toBeGreaterThan(1000);
  });

  it("every truncation of every instruction's data and accounts", () => {
    const txs = [...loadFixtureTxs(), ...synthTxs(300, 3)];
    for (const tx of txs) {
      const cut = (ix: NormIx, mk: (d: Uint8Array, a: string[]) => void): void => {
        for (let n = 0; n <= ix.data.length; n++) mk(ix.data.slice(0, n), ix.accounts);
        for (let n = 0; n <= ix.accounts.length; n++) mk(ix.data, ix.accounts.slice(0, n));
      };
      tx.instructions.forEach((top, i) => {
        const variants: Array<[Uint8Array, string[]]> = [];
        cut(top, (d, a) => variants.push([d, a]));
        for (const [d, a] of variants) {
          const t2: NormTx = { ...tx, instructions: tx.instructions.map((x, k) => (k === i ? { ...x, data: d, accounts: a } : x)) };
          same(t2);
        }
        top.inner.forEach((ix, j) => {
          const vs: Array<[Uint8Array, string[]]> = [];
          cut(ix, (d, a) => vs.push([d, a]));
          for (const [d, a] of vs) {
            const t2: NormTx = {
              ...tx,
              instructions: tx.instructions.map((x, k) => (k === i ? { ...x, inner: x.inner.map((y, m) => (m === j ? { ...y, data: d, accounts: a } : y)) } : x)),
            };
            same(t2);
          }
        });
      });
    }
  });

  it("random buffers: random bytes with wrapper tags, random lengths, random account lists", () => {
    const r = rng(99);
    const keys = ["a", "b", "c", "", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"];
    const tags = [74, 75, 76, 77, 78, 81, 3, 7, 8, 12, 14, 15, 0, 255];
    const bytes = (n: number): Uint8Array => Uint8Array.from({ length: n }, () => (r() < 0.3 ? 0xff : r() < 0.3 ? 0 : Math.floor(r() * 256)));
    const mk = (prog: string): NormIx => {
      const d = bytes(Math.floor(r() * 40));
      if (d.length && r() < 0.8) d[0] = tags[Math.floor(r() * tags.length)]!;
      return {
        programId: prog,
        accounts: Array.from({ length: Math.floor(r() * 15) }, () => keys[Math.floor(r() * keys.length)]!),
        data: d,
        stackHeight: r() < 0.2 ? null : 1 + Math.floor(r() * 3),
      };
    };
    const progs = ["W", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "X"];
    const W = new Set(["W"]);
    for (let t = 0; t < 10000; t++) {
      const tx: NormTx = {
        signature: "s", slot: t, blockTime: null, failed: false,
        instructions: Array.from({ length: 1 + Math.floor(r() * 3) }, () => ({
          ...mk(progs[Math.floor(r() * 3)]!),
          inner: Array.from({ length: Math.floor(r() * 6) }, () => mk(progs[Math.floor(r() * 3)]!)),
        })),
      };
      const a = (() => { try { return { ok: decodeLpVaultEvents(tx, W) }; } catch (e) { return { err: String(e) }; } })();
      const b = (() => { try { return { ok: legacyDecode(tx, W) }; } catch (e) { return { err: String(e) }; } })();
      expect(a).toStrictEqual(b);
    }
  }, 120_000);
});
