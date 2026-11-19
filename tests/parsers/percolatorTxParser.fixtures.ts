import { PublicKey } from "@solana/web3.js";
import {
  IX_TAG,
  encodeTradeNoCpi,
  encodeTradeCpi,
  encodeBatchTradeNoCpi,
  encodeBatchTradeCpi,
} from "@percolatorct/sdk";

/** Shared fixture builders for the tx-parser differential test (and the local bench). Seeded: same seed, same txs. */

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

// Limbs in base 58^5 (656_356_768 < 2^30): 5x fewer inner iterations than one base-58 digit per limb, same output.
const LIMB = 58 ** 5;

export function encodeBase58(bytes: Uint8Array): string {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const limbs: number[] = []; // little-endian base 58^5
  for (let i = zeros; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < limbs.length; j++) {
      carry += limbs[j] * 256;
      limbs[j] = carry % LIMB;
      carry = Math.floor(carry / LIMB);
    }
    while (carry > 0) {
      limbs.push(carry % LIMB);
      carry = Math.floor(carry / LIMB);
    }
  }
  let out = "";
  for (let i = limbs.length - 1; i >= 0; i--) {
    let l = limbs[i];
    let chunk = "";
    for (let k = 0; k < 5; k++) {
      chunk = ALPHABET[l % 58] + chunk;
      l = Math.floor(l / 58);
    }
    out += chunk;
  }
  // drop the zero-padding of the most significant limb
  let lead = 0;
  while (lead < out.length && out[lead] === "1") lead++;
  return "1".repeat(zeros) + out.slice(lead);
}

/** mulberry32 */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
export const OTHER_PROGRAM = "11111111111111111111111111111111";

function pk(r: () => number, pool: PublicKey[]): PublicKey {
  return pool[Math.floor(r() * pool.length)];
}

export function makePool(r: () => number, n = 64): PublicKey[] {
  const out: PublicKey[] = [];
  for (let i = 0; i < n; i++) out.push(new PublicKey(Uint8Array.from({ length: 32 }, () => Math.floor(r() * 256))));
  return out;
}

function signedSize(r: () => number): bigint {
  const mag = BigInt(Math.floor(r() * 1e9)) * (r() < 0.05 ? 0n : 1n);
  return r() < 0.5 ? -mag : mag;
}

function instrData(r: () => number): Uint8Array {
  const kind = Math.floor(r() * 7);
  const common = {
    accountAPortfolioId: 1n,
    accountAPositionEpoch: 0n,
    accountBPortfolioId: 2n,
    accountBPositionEpoch: 0n,
  };
  const asset = Math.floor(r() * 8);
  switch (kind) {
    case 0:
      return encodeTradeNoCpi({ ...common, assetIndex: asset, marketId: 9n, sizeQ: signedSize(r), execPrice: BigInt(Math.floor(r() * 1e11)), feeBps: BigInt(Math.floor(r() * 100)), backingFeeCapBps: 0 });
    case 1:
      return encodeTradeCpi({ ...common, accountBMatcherSequence: 7n, assetIndex: asset, marketId: 9n, sizeQ: signedSize(r), feeBps: BigInt(Math.floor(r() * 100)), limitPrice: 51_000_000_000n, backingFeeCapBps: 0 });
    case 2: {
      const legs = Array.from({ length: 1 + Math.floor(r() * 4) }, () => ({ assetIndex: Math.floor(r() * 8), marketId: 9n, sizeQ: signedSize(r), execPrice: 50_000_000_000n, feeBps: 30n }));
      return encodeBatchTradeNoCpi({ legs, ...common });
    }
    case 3: {
      const legs = Array.from({ length: 1 + Math.floor(r() * 4) }, () => ({ assetIndex: Math.floor(r() * 8), marketId: 9n, sizeQ: signedSize(r), feeBps: 30n, limitPrice: 51_000_000_000n }));
      return encodeBatchTradeCpi({ legs, maxSlippageAtoms: 0n, maxFeeAtoms: 0n, ...common, accountBMatcherSequence: 7n });
    }
    case 4: {
      // RebalanceReduce (44), 35 bytes
      const b = new Uint8Array(35);
      b[0] = IX_TAG.RebalanceReduce;
      new DataView(b.buffer).setUint16(17, Math.floor(r() * 8), true);
      new DataView(b.buffer).setBigUint64(19, BigInt(Math.floor(r() * 1e9)), true);
      return b;
    }
    case 5: {
      // EvictAndTradeCpi (119) wrapping a TradeCpi body
      const inner = encodeTradeCpi({ ...common, accountBMatcherSequence: 7n, assetIndex: asset, marketId: 9n, sizeQ: signedSize(r), feeBps: 30n, limitPrice: 51_000_000_000n, backingFeeCapBps: 0 });
      const b = Uint8Array.from(inner);
      b[0] = 119;
      return b;
    }
    default: {
      // junk: random length/tag (incl. truncated trades, leading zero bytes)
      const len = Math.floor(r() * 100);
      const b = Uint8Array.from({ length: len }, () => (r() < 0.2 ? 0 : Math.floor(r() * 256)));
      if (len > 0 && r() < 0.5) b[0] = [6, 10, 66, 67, 44, 119][Math.floor(r() * 6)];
      return b;
    }
  }
}

export interface FixtureTx {
  tx: any;
  signature: string;
}

/** One transaction. `objects` selects PublicKey objects (getParsedTransaction shape) vs plain strings (webhook shape). */
const b58Cache = new WeakMap<PublicKey, string>();
const toB58 = (k: PublicKey): string => {
  let v = b58Cache.get(k);
  if (v === undefined) b58Cache.set(k, (v = k.toBase58()));
  return v;
};
const keyCache = new Map<string, PublicKey>();
const keyOf = (s: string): PublicKey => {
  let v = keyCache.get(s);
  if (!v) keyCache.set(s, (v = new PublicKey(s)));
  return v;
};

export function makeTx(r: () => number, pool: PublicKey[], programIds: string[], i: number): FixtureTx {
  const objects = r() < 0.5;
  const wrap = (k: PublicKey): unknown => {
    const m = r();
    if (!objects) return toB58(k);
    return m < 0.8 ? k : { pubkey: k, isSigner: false, isWritable: true };
  };
  const perc = keyOf(programIds[0]);
  const nIx = 1 + Math.floor(r() * 4);
  const instructions: any[] = [];
  const innerGroups: any[] = [];
  for (let n = 0; n < nIx; n++) {
    const roll = r();
    if (roll < 0.1) {
      instructions.push({ program: "system", parsed: { type: "transfer" }, programId: wrap(pool[0]) });
      continue;
    }
    const isPerc = roll > 0.2;
    const data = instrData(r);
    const nAcc = r() < 0.05 ? Math.floor(r() * 3) : 6 + Math.floor(r() * 4);
    const accounts = Array.from({ length: nAcc }, () => wrap(pk(r, pool)));
    const ixDataStr = r() < 0.02 ? "0OIl-bad" : encodeBase58(data);
    instructions.push({ programId: isPerc ? (objects ? perc : PERC) : wrap(pk(r, pool)), accounts, data: ixDataStr });
    if (r() < 0.5) {
      innerGroups.push({
        index: n,
        instructions: [
          { programId: accounts[4] ?? wrap(pool[1]), data: encodeBase58(Uint8Array.from({ length: 43 + Math.floor(r() * 30) }, () => Math.floor(r() * 256))), accounts: [] },
          { programId: wrap(pool[2]), data: encodeBase58(Uint8Array.from({ length: 12 }, () => Math.floor(r() * 256))), accounts: [] },
        ],
      });
    }
  }
  const meta: any = {
    err: r() < 0.03 ? { InstructionError: [0, "Custom"] } : null,
    logMessages: [],
    innerInstructions: r() < 0.1 ? null : innerGroups,
  };
  if (r() < 0.15) meta.returnData = { programId: wrap(pool[1]), data: [Buffer.from(Uint8Array.from({ length: 64 }, () => Math.floor(r() * 256))).toString("base64"), "base64"] };
  return { tx: { transaction: { message: { instructions } }, meta: r() < 0.01 ? null : meta }, signature: `sig${i}` };
}

export function makeTxs(seed: number, count: number): { txs: FixtureTx[]; programIds: string[] } {
  const r = rng(seed);
  const pool = makePool(r);
  pool[1] = new PublicKey(OTHER_PROGRAM);
  const programIds = [PERC];
  const txs: FixtureTx[] = [];
  for (let i = 0; i < count; i++) txs.push(makeTx(r, pool, programIds, i));
  return { txs, programIds };
}
