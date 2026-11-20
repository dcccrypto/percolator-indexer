import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";

/**
 * Drop-in for `@percolatorct/shared`'s `decodeBase58`, same results for every input (differential-tested in
 * tests/parsers/percolatorTxParser.diff.test.ts), minus its costs: `ALPHABET.indexOf` per char, a JS array with
 * `unshift`, and one multiply per char. Here: a 128-entry lookup table, a reused Uint8Array scratch, and four chars
 * (58^4 < 2^24) folded into each pass over the limbs, so the arithmetic stays inside uint32.
 *
 * Non-string input is delegated to the shared decoder so its (odd) behaviour for those is kept as is.
 */
const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const DIGIT = new Int8Array(128).fill(-1);
for (let i = 0; i < ALPHABET.length; i++) DIGIT[ALPHABET.charCodeAt(i)] = i;
const POW58 = [1, 58, 3364, 195112, 11316496];

let scratch = new Uint8Array(512);

export function decodeBase58Fast(str: string): Uint8Array | null {
  if (typeof str !== "string") return sharedDecodeBase58(str);
  const n = str.length;
  let zeros = 0;
  while (zeros < n && str.charCodeAt(zeros) === 49) zeros++; // "1"
  const rest = n - zeros;
  if (scratch.length < rest) scratch = new Uint8Array(rest);
  const limbs = scratch; // little-endian
  let len = 0;
  let i = zeros;
  // First chunk takes the remainder so every later chunk is exactly 4 chars.
  let chunk = rest % 4 || 4;
  while (i < n) {
    let val = 0;
    for (let k = 0; k < chunk; k++) {
      const c = str.charCodeAt(i + k);
      const d = c < 128 ? DIGIT[c] : -1;
      if (d < 0) return null;
      val = val * 58 + d;
    }
    i += chunk;
    const mul = POW58[chunk];
    let carry = val;
    for (let j = 0; j < len; j++) {
      const x = limbs[j] * mul + carry; // < 2^32
      limbs[j] = x & 0xff;
      carry = x >>> 8;
    }
    while (carry > 0) {
      limbs[len++] = carry & 0xff;
      carry >>>= 8;
    }
    chunk = 4;
  }
  const out = new Uint8Array(zeros + len);
  for (let j = 0; j < len; j++) out[zeros + j] = limbs[len - 1 - j];
  return out;
}
