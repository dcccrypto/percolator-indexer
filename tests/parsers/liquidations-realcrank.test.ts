/**
 * parseLiquidation on real-shaped devnet instructions (LIQMUT-1).
 *
 * v2.2 has no instruction-level liquidation, so parseLiquidation must return null for every instruction.
 * liquidations.test.ts only feeds cranks with a non-zero nowSlot, oracleAccounts=1 and 1 or 3 accounts, under an
 * SDK mock. Real devnet cranks look different: 4,262 real tag-5 cranks in non-liquidation txs use exactly the 3 byte
 * strings below, all with nowSlot=0, oracleAccounts=0 and 3 accounts. A decoder that keyed on any zero byte here
 * would mark every real crank as a liquidation while liquidations.test.ts still passed.
 *
 * No SDK mock on purpose, so a revived IX_TAG.LiquidateAtOracle (tag 7) check reads the real IX_TAG.
 */
import { describe, it, expect } from 'vitest';
import { parseLiquidation } from '../../src/parsers/liquidations.js';

// Verbatim v2.2 tag-5 data from real devnet txs (counts = occurrences in the recorded corpus).
const REAL_CRANKS: Array<[string, string]> = [
  ['nObs=0 (1,391 real)', '05000000000000000000'],
  ['nObs=1 asset 0 (2,349 real)', '05000000000000000001000000'],
  ['nObs=5 assets 0..4 (522 real)', '05000000000000000005000000010000020000030000040000'],
];
// Real crank account shape: [cranker, market-group slab, portfolio/oracle], always 3 on devnet.
const ACCTS = [
  'FbTbDeGWQpjrEqJdqoBHX3sTWHoAmU2xywD7wyxH6WC7',
  'AXyQMrAENwUvaHRJkXHekF6T6VsyfYP2A3BpNjA6uvsJ',
  'E9F2ha9eXXWXeoZAEtddc2sK2uHkSEwYb8Kn9gr3qo8E',
];
type Case = [tag: number, data: Uint8Array, accounts: (string | undefined)[]];
const hex = (h: string) => Uint8Array.from(Buffer.from(h, 'hex'));
// Returns the cases that were flagged, so a failure names the exact input.
const flagged = (cases: Case[]) =>
  cases
    .filter(([t, d, a]) => parseLiquidation(t, d, a) !== null)
    .map(([t, d, a]) => `${t}:${Buffer.from(d).toString('hex')}:${a.length}`);

describe('parseLiquidation never flags a real-shaped devnet instruction', () => {
  it.each(REAL_CRANKS)('real devnet crank %s is not a liquidation', (_name, h) => {
    expect(parseLiquidation(5, hex(h), ACCTS)).toBeNull();
  });

  it('every tag 0..255 on each real crank body returns null (incl. removed 7 LiquidateAtOracle, 101 ExecuteAdl)', () => {
    const cases: Case[] = [];
    for (const [, h] of REAL_CRANKS) {
      for (let t = 0; t < 256; t++) {
        const d = hex(h);
        d[0] = t;
        cases.push([t, d, ACCTS]);
      }
    }
    expect(flagged(cases)).toEqual([]);
  });

  it('every value 0..255 at every byte offset of each real crank returns null (no byte is an action byte)', () => {
    const cases: Case[] = [];
    for (const [, h] of REAL_CRANKS) {
      const base = hex(h);
      for (let off = 1; off < base.length; off++) {
        for (let v = 0; v < 256; v++) {
          const d = base.slice();
          d[off] = v;
          cases.push([5, d, ACCTS]);
        }
      }
    }
    expect(flagged(cases)).toEqual([]);
  });

  it('any account-list length 0..16 (empty, short, oracle tail, undefined keys) returns null', () => {
    const cases: Case[] = [];
    for (const [, h] of REAL_CRANKS) {
      for (let n = 0; n <= 16; n++) {
        cases.push([5, hex(h), Array.from({ length: n }, (_, i) => ACCTS[i % 3])]);
        cases.push([5, hex(h), Array.from({ length: n }, () => undefined)]);
      }
    }
    expect(flagged(cases)).toEqual([]);
  });
});
