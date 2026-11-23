import { describe, it, expect } from "vitest";
import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";
import { IX_TAG } from "@percolatorct/sdk";
import * as next from "../../src/parsers/percolatorTxParser.js";
import * as ref from "./percolatorTxParser.reference.js";
import { decodeBase58Fast } from "../../src/parsers/fastBase58.js";
import { makeTxs, encodeBase58, rng } from "./percolatorTxParser.fixtures.js";

/**
 * Cheap structural equality with toStrictEqual's strictness for the shapes the parser returns
 * (same prototype, same own keys incl. undefined-valued ones, Object.is on primitives/bigints).
 * Returns a path to the first difference, or null. Used per row so only a mismatch pays for vitest's diff.
 */
function diffPath(a: unknown, b: unknown, path = "$"): string | null {
  if (Object.is(a, b)) return null;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return path;
  if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) return path + "(proto)";
  if (ArrayBuffer.isView(a)) {
    const x = a as unknown as ArrayLike<number>;
    const y = b as unknown as ArrayLike<number>;
    if (x.length !== y.length) return path + ".length";
    for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return `${path}[${i}]`;
    return null;
  }
  if (Array.isArray(a)) {
    const y = b as unknown[];
    if (a.length !== y.length) return path + ".length";
    for (let i = 0; i < a.length; i++) {
      const d = diffPath(a[i], y[i], `${path}[${i}]`);
      if (d) return d;
    }
    return null;
  }
  const ka = Object.keys(a);
  if (ka.length !== Object.keys(b).length) return path + "(keys)";
  for (const k of ka) {
    if (!Object.prototype.hasOwnProperty.call(b, k)) return `${path}.${k}(missing)`;
    const d = diffPath((a as any)[k], (b as any)[k], `${path}.${k}`);
    if (d) return d;
  }
  return null;
}

/** Collects the first 10 mismatches, then one expect at the end. */
function collector() {
  const bad: string[] = [];
  return {
    check(label: string, got: unknown, want: unknown) {
      if (bad.length >= 10) return;
      const d = diffPath(got, want);
      if (d) bad.push(`${label} ${d}`);
    },
    done: () => expect(bad).toEqual([]),
  };
}

// Differential test: the sped-up parser must return exactly what the frozen pre-change copy returns.

describe("parser differential (reference = a6e05f8)", { timeout: 180_000 }, () => {
  const { txs, programIds } = makeTxs(20261116, 2000);

  it("parsePercolatorFills + parsePercolatorLiquidations: 2000 seeded txs, identical output", () => {
    let fills = 0;
    let cpi = 0;
    const c = collector();
    for (const { tx, signature } of txs) {
      const a = ref.parsePercolatorFills(tx, signature, programIds);
      c.check(`fills ${signature}`, next.parsePercolatorFills(tx, signature, programIds), a);
      fills += a.length;
      for (const f of a) if (f.cpi) cpi++;
      c.check(
        `liq ${signature}`,
        next.parsePercolatorLiquidations(tx, signature, programIds),
        ref.parsePercolatorLiquidations(tx, signature, programIds),
      );
    }
    c.done();
    console.log(`corpus seed=20261116 txs=${txs.length} fills=${fills} cpi=${cpi}`);
    // the corpus must actually exercise the parser, not compare 2000 empty arrays
    expect(fills).toBeGreaterThan(1000);
    expect(cpi).toBeGreaterThan(100);
  });

  it("a second seed, to catch seed-specific luck", () => {
    const other = makeTxs(7, 500);
    const c = collector();
    for (const { tx, signature } of other.txs) {
      c.check(signature, next.parsePercolatorFills(tx, signature, other.programIds), ref.parsePercolatorFills(tx, signature, other.programIds));
    }
    c.done();
    console.log(`corpus seed=7 txs=${other.txs.length}`);
  });

  it("raw decoders: random buffers incl. i128 extremes, truncated and zero-size", () => {
    const r = rng(99);
    const c = collector();
    const tags = [IX_TAG.TradeNoCpi, IX_TAG.TradeCpi, IX_TAG.BatchTradeNoCpi, IX_TAG.BatchTradeCpi, 44, 119, 0, 200];
    const sizeFill = [0x00, 0xff, 0x80, 0x7f];
    for (let i = 0; i < 2000; i++) {
      const len = Math.floor(r() * 260);
      const buf = Uint8Array.from({ length: len }, () => {
        const m = r();
        return m < 0.25 ? sizeFill[Math.floor(r() * 4)] : Math.floor(r() * 256);
      });
      if (len) buf[0] = tags[Math.floor(r() * tags.length)];
      if (len > 1 && r() < 0.5) buf[1] = Math.floor(r() * 6);
      // misaligned view over a larger buffer: byteOffset != 0
      const host = new Uint8Array(len + 5);
      host.set(buf, 3);
      const view = host.subarray(3, 3 + len);
      for (const tag of [IX_TAG.TradeNoCpi, IX_TAG.TradeCpi]) {
        c.check(`single#${i}/${tag}`, next.decodeV18SingleFill(tag, view), ref.decodeV18SingleFill(tag, buf));
      }
      for (const tag of [IX_TAG.BatchTradeNoCpi, IX_TAG.BatchTradeCpi]) {
        c.check(`batch#${i}/${tag}`, next.decodeV18BatchLegs(tag, view), ref.decodeV18BatchLegs(tag, buf));
      }
      c.check(`rebalance#${i}`, next.decodeRebalanceReduce(view), ref.decodeRebalanceReduce(buf));
    }
    c.done();
  });
});

describe("decodeBase58Fast == shared decodeBase58", { timeout: 60_000 }, () => {
  const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

  it("round-trips and matches on random bytes of every length 0..300 (leading zeros included)", () => {
    const r = rng(5);
    const c = collector();
    for (let len = 0; len <= 300; len++) {
      const bytes = Uint8Array.from({ length: len }, () => (r() < 0.15 ? 0 : Math.floor(r() * 256)));
      const s = encodeBase58(bytes);
      const fast = decodeBase58Fast(s);
      c.check(`shared len=${len}`, fast, sharedDecodeBase58(s));
      c.check(`roundtrip len=${len}`, fast, bytes);
    }
    c.done();
  });

  it("matches on random strings, invalid characters and non-ASCII, and on non-string input", () => {
    const r = rng(6);
    const c = collector();
    const pool = ALPHABET + "0OIl+/ é☃";
    for (let i = 0; i < 3000; i++) {
      const n = Math.floor(r() * 140);
      let s = "";
      for (let k = 0; k < n; k++) s += r() < 0.97 ? ALPHABET[Math.floor(r() * 58)] : pool[Math.floor(r() * pool.length)];
      if (r() < 0.2) s = "1".repeat(Math.floor(r() * 5)) + s;
      c.check(`str#${i}`, decodeBase58Fast(s), sharedDecodeBase58(s));
    }
    for (const v of ["", "1", "11", "111", "z", "zzzz", "1111z", "0", undefined, null, 5, [], {}, ["a"]]) {
      c.check(`input ${String(v)}`, decodeBase58Fast(v as never), sharedDecodeBase58(v as never));
    }
    c.done();
  });
});
