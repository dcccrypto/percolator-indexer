import { describe, it, expect } from "vitest";
import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";
import { IX_TAG } from "@percolatorct/sdk";
import * as next from "../../src/parsers/percolatorTxParser.js";
import * as ref from "./percolatorTxParser.reference.js";
import { decodeBase58Fast } from "../../src/parsers/fastBase58.js";
import { makeTxs, encodeBase58, rng } from "./percolatorTxParser.fixtures.js";

// Differential test: the sped-up parser must return exactly what the frozen pre-change copy returns.

describe("parser differential (reference = a6e05f8)", { timeout: 180_000 }, () => {
  const { txs, programIds } = makeTxs(20261116, 2000);
  // `ixIndex` (the trade instruction's top-level index, for the fill_event lookup) is the one field added since the
  // reference; every trade fill must carry it, and everything else must be unchanged.
  const withoutIxIndex = (fills: ReturnType<typeof next.parsePercolatorFills>) =>
    fills.map(({ ixIndex, ...rest }) => { expect(rest.rebalanceReduce || typeof ixIndex === "number").toBeTruthy(); return rest; });

  it("parsePercolatorFills + parsePercolatorLiquidations: 2000 seeded txs, identical output", () => {
    let fills = 0;
    let cpi = 0;
    for (const { tx, signature } of txs) {
      const a = ref.parsePercolatorFills(tx, signature, programIds);
      const b = next.parsePercolatorFills(tx, signature, programIds);
      expect(withoutIxIndex(b)).toStrictEqual(a);
      fills += a.length;
      cpi += a.filter((f) => f.cpi).length;
      expect(next.parsePercolatorLiquidations(tx, signature, programIds)).toStrictEqual(
        ref.parsePercolatorLiquidations(tx, signature, programIds),
      );
    }
    // the corpus must actually exercise the parser, not compare 2000 empty arrays
    expect(fills).toBeGreaterThan(1000);
    expect(cpi).toBeGreaterThan(100);
  });

  it("a second seed, to catch seed-specific luck", () => {
    const other = makeTxs(7, 500);
    for (const { tx, signature } of other.txs) {
      expect(withoutIxIndex(next.parsePercolatorFills(tx, signature, other.programIds))).toStrictEqual(
        ref.parsePercolatorFills(tx, signature, other.programIds),
      );
    }
  });

  it("raw decoders: random buffers incl. i128 extremes, truncated and zero-size", () => {
    const r = rng(99);
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
        expect(next.decodeV18SingleFill(tag, view)).toStrictEqual(ref.decodeV18SingleFill(tag, buf));
      }
      for (const tag of [IX_TAG.BatchTradeNoCpi, IX_TAG.BatchTradeCpi]) {
        expect(next.decodeV18BatchLegs(tag, view)).toStrictEqual(ref.decodeV18BatchLegs(tag, buf));
      }
      expect(next.decodeRebalanceReduce(view)).toStrictEqual(ref.decodeRebalanceReduce(buf));
    }
  });
});

describe("decodeBase58Fast == shared decodeBase58", { timeout: 60_000 }, () => {
  const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

  it("round-trips and matches on random bytes of every length 0..300 (leading zeros included)", () => {
    const r = rng(5);
    for (let len = 0; len <= 300; len++) {
      const bytes = Uint8Array.from({ length: len }, () => (r() < 0.15 ? 0 : Math.floor(r() * 256)));
      const s = encodeBase58(bytes);
      expect(decodeBase58Fast(s)).toStrictEqual(sharedDecodeBase58(s));
      expect(decodeBase58Fast(s)).toStrictEqual(bytes);
    }
  });

  it("matches on random strings, invalid characters and non-ASCII, and on non-string input", () => {
    const r = rng(6);
    const pool = ALPHABET + "0OIl+/ é☃";
    for (let i = 0; i < 3000; i++) {
      const n = Math.floor(r() * 140);
      let s = "";
      for (let k = 0; k < n; k++) s += r() < 0.97 ? ALPHABET[Math.floor(r() * 58)] : pool[Math.floor(r() * pool.length)];
      if (r() < 0.2) s = "1".repeat(Math.floor(r() * 5)) + s;
      expect(decodeBase58Fast(s)).toStrictEqual(sharedDecodeBase58(s));
    }
    for (const v of ["", "1", "11", "111", "z", "zzzz", "1111z", "0", undefined, null, 5, [], {}, ["a"]]) {
      expect(decodeBase58Fast(v as never)).toStrictEqual(sharedDecodeBase58(v as never));
    }
  });
});
