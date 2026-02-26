import { describe, it, expect } from "vitest";
import { sanitizeBigIntForDb, sanitizeBigIntToString } from "../../src/utils/bigint-sanitize.js";

const U64_MAX = 18_446_744_073_709_551_615n;
const PG_BIGINT_MAX = 9_223_372_036_854_775_807n;

describe("bigint-sanitize (PERC-206)", () => {
  describe("sanitizeBigIntForDb", () => {
    it("passes through normal values unchanged", () => {
      expect(sanitizeBigIntForDb(0n)).toBe(0);
      expect(sanitizeBigIntForDb(1n)).toBe(1);
      expect(sanitizeBigIntForDb(1_000_000n)).toBe(1_000_000);
      expect(sanitizeBigIntForDb(-500n)).toBe(-500);
      expect(sanitizeBigIntForDb(350_000_000n)).toBe(350_000_000); // typical slot
    });

    it("replaces u64::MAX sentinel with fallback", () => {
      expect(sanitizeBigIntForDb(U64_MAX)).toBe(0);
    });

    it("replaces near-sentinel values (≥90% of u64::MAX)", () => {
      const threshold = (U64_MAX * 9n) / 10n;
      expect(sanitizeBigIntForDb(threshold)).toBe(0);
      expect(sanitizeBigIntForDb(threshold + 1n)).toBe(0);
    });

    it("replaces values exceeding PG bigint max", () => {
      expect(sanitizeBigIntForDb(PG_BIGINT_MAX + 1n)).toBe(0);
      // The exact value from the PERC-206 bug report
      expect(sanitizeBigIntForDb(13_292_928_068_290_159_000n)).toBe(0);
    });

    it("accepts value at PG bigint max boundary", () => {
      expect(sanitizeBigIntForDb(PG_BIGINT_MAX)).toBe(Number(PG_BIGINT_MAX));
    });

    it("handles negative overflow", () => {
      expect(sanitizeBigIntForDb(-PG_BIGINT_MAX - 1n)).toBe(0);
      expect(sanitizeBigIntForDb(-U64_MAX)).toBe(0);
    });

    it("supports custom fallback", () => {
      expect(sanitizeBigIntForDb(U64_MAX, -1)).toBe(-1);
      expect(sanitizeBigIntForDb(U64_MAX, 100)).toBe(100);
    });
  });

  describe("sanitizeBigIntToString", () => {
    it("converts normal values to string with full precision", () => {
      expect(sanitizeBigIntToString(0n)).toBe("0");
      expect(sanitizeBigIntToString(12345n)).toBe("12345");
      expect(sanitizeBigIntToString(-999n)).toBe("-999");
      // Large but valid — preserves precision that Number() would lose
      expect(sanitizeBigIntToString(9_000_000_000_000_000_000n)).toBe("9000000000000000000");
    });

    it("replaces u64::MAX sentinel with '0'", () => {
      expect(sanitizeBigIntToString(U64_MAX)).toBe("0");
    });

    it("supports custom fallback string", () => {
      expect(sanitizeBigIntToString(U64_MAX, "null")).toBe("null");
    });
  });
});
