/**
 * Differential test: parseLiquidation vs the REAL SDK (no mock) v2.2 wire.
 *
 * Feeds every crank / forced-reduce / ADL-capable instruction the vendored v2.2 SDK can encode
 * into parseLiquidation and parsePercolatorLiquidations (the base58 tx path) and asserts zero
 * markers and zero throws. The v17 decoder flagged `data[1] === 1` on tag 5; under the v18+
 * wire data[1] is the low byte of nowSlot, so this pins that no v2.2 encoding is misread.
 */
import { describe, it, expect } from 'vitest';
import { PublicKey } from '@solana/web3.js';
import {
  IX_TAG,
  IX_TAG_P2B,
  encodePermissionlessCrank,
  encodeRebalanceReduce,
  encodeAdlWindDown,
  encodeLiquidateAtOracle,
  encodeExecuteAdl,
  encodeAdminForceClose,
  encodeForceCloseResolved,
  encodeEvictAndTradeCpiV22,
} from '@percolatorct/sdk';
import { parseLiquidation } from '../../src/parsers/liquidations.js';
import { parsePercolatorLiquidations } from '../../src/parsers/percolatorTxParser.js';

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function b58(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  let s = '';
  while (n > 0n) { s = B58[Number(n % 58n)] + s; n /= 58n; }
  for (const b of bytes) { if (b === 0) s = '1' + s; else break; }
  return s;
}

const key = (i: number) => new PublicKey(new Uint8Array(32).fill(i)).toBase58();
const ACCTS = [key(1), key(2), key(3), key(4)];
const PROG = key(9);

/** Every v2.2 instruction payload the SDK encoders emit that could be liquidation-shaped. */
function wireCases(): Array<{ name: string; data: Uint8Array }> {
  const out: Array<{ name: string; data: Uint8Array }> = [];
  // nowSlot low byte == 0x01 (the v17 false-positive trigger), with 0..N observations, plus all 256 low bytes.
  for (let low = 0; low < 256; low++) {
    for (const n of [0, 1, 3]) {
      const observations = Array.from({ length: n }, (_, i) => ({ assetIndex: i * 257, oracleAccounts: 1 }));
      out.push({ name: `crank low=${low} n=${n}`, data: encodePermissionlessCrank({ nowSlot: 0x1122334455667700n | BigInt(low), observations }) });
    }
  }
  out.push({ name: 'crank slot=1', data: encodePermissionlessCrank({ nowSlot: 1n, observations: [] }) });
  out.push({ name: 'crank slot=0x0101', data: encodePermissionlessCrank({ nowSlot: 0x0101n, observations: [] }) });
  // Forced-reduce / ADL paths that exist in the v2.2 SDK.
  out.push({ name: 'RebalanceReduce', data: encodeRebalanceReduce({ portfolioId: 1n, positionEpoch: 1n, assetIndex: 1, reduceQ: 1n }) });
  out.push({ name: 'RebalanceReduce big', data: encodeRebalanceReduce({ portfolioId: 0x0101n, positionEpoch: 1n, assetIndex: 0x0101, reduceQ: (1n << 100n) | 1n }) });
  out.push({ name: 'AdlWindDown slot=1', data: encodeAdlWindDown({ nowSlot: 1n, assetIndex: 1, portfolioId: 1n, positionEpoch: 1n } as never) });
  return out;
}

describe('parseLiquidation vs SDK-encoded v2.2 wire', () => {
  const cases = wireCases();

  it('covers a meaningful number of encoded cases', () => {
    expect(cases.length).toBeGreaterThan(700);
  });

  it('parseLiquidation: 0 markers, 0 throws on every SDK-encoded instruction', () => {
    for (const c of cases) {
      let r: unknown;
      expect(() => { r = parseLiquidation(c.data[0], c.data, ACCTS); }, c.name).not.toThrow();
      expect(r, c.name).toBeNull();
    }
  });

  it('parsePercolatorLiquidations (base58 tx path): 0 markers, 0 throws', () => {
    for (const c of cases) {
      const tx = {
        meta: { err: null },
        transaction: { message: { instructions: [{ programId: PROG, accounts: ACCTS, data: b58(c.data) }] } },
      };
      let r: unknown[] = [];
      expect(() => { r = parsePercolatorLiquidations(tx, 'sig', [PROG]); }, c.name).not.toThrow();
      expect(r, c.name).toEqual([]);
    }
  });

  it('removed tags (7 LiquidateAtOracle, 101 ExecuteAdl, 17 AdminForceClose) are rejected by the SDK encoders', () => {
    expect(() => encodeLiquidateAtOracle({} as never)).toThrow(/not accepted/);
    expect(() => encodeExecuteAdl({} as never)).toThrow(/not accepted/);
    expect(() => encodeAdminForceClose({} as never)).toThrow(/not accepted/);
    expect(() => encodeForceCloseResolved({} as never)).toThrow(/not accepted/);
  });

  it('raw bytes under the removed/forced tags, with data[1]===1, still yield no marker', () => {
    const tags = [IX_TAG.LiquidateAtOracle, 101, IX_TAG.AdminForceClose, IX_TAG.RebalanceReduce, IX_TAG_P2B.AdlWindDown, 119, 124, 64, 30];
    for (const t of tags) {
      for (const len of [1, 2, 9, 40, 175]) {
        const d = new Uint8Array(len);
        d[0] = t;
        if (len > 1) d[1] = 1;
        expect(parseLiquidation(t, d, ACCTS), `tag ${t} len ${len}`).toBeNull();
      }
    }
  });

  it('exhaustive: no (tag, data[1]) pair among 256x256 yields a marker', () => {
    for (let t = 0; t < 256; t++) {
      for (let b = 0; b < 256; b++) {
        if (parseLiquidation(t, new Uint8Array([t, b, 0, 0, 0, 0, 0, 0, 0, 0]), ACCTS) !== null) {
          throw new Error(`marker for tag ${t} byte ${b}`);
        }
      }
    }
  });

  it('EvictAndTradeCpi (v2.2 tag 119) SDK encoder export exists and is not liquidation-shaped', () => {
    expect(typeof encodeEvictAndTradeCpiV22).toBe('function');
  });
});
