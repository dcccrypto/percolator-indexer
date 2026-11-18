import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";

/**
 * Drop-in for `@percolatorct/shared`'s `decodeBase58`, same results for every input (differential-tested in
 * tests/parsers/percolatorTxParser.diff.test.ts), minus its costs: `ALPHABET.indexOf` per char, a JS array with
 * `unshift`, and one multiply per char. Here: a 128-entry lookup table, a reused Int32Array scratch of 24-bit limbs (3 bytes
 * each, a third of the passes of byte limbs), and four chars (58^4 < 2^24) folded into each pass, so every product stays
 * below 2^48 and is exact in a double.
 *
 * Non-string input is delegated to the shared decoder so its (odd) behaviour for those is kept as is.
 */
const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const DIGIT = new Int8Array(128).fill(-1);
for (let i = 0; i < ALPHABET.length; i++) DIGIT[ALPHABET.charCodeAt(i)] = i;
const POW58 = [1, 58, 3364, 195112, 11316496];
const LIMB = 16777216; // 2^24

let scratch = new Int32Array(256);

export function decodeBase58Fast(str: string): Uint8Array | null {
  if (typeof str !== "string") return sharedDecodeBase58(str);
  const n = str.length;
  let zeros = 0;
  while (zeros < n && str.charCodeAt(zeros) === 49) zeros++; // "1"
  const rest = n - zeros;
  const need = Math.ceil((rest * 0.7322) / 3) + 2; // 24-bit limbs: log2(58)/24 < 0.2442 per char
  if (scratch.length < need) scratch = new Int32Array(need);
  const limbs = scratch; // little-endian, 24 bits each
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
      // limb < 2^24 and mul, carry <= 58^4 < 2^24: x < 2^48, exact in a double.
      const x = limbs[j] * mul + carry;
      carry = Math.floor(x / LIMB);
      limbs[j] = x - carry * LIMB;
    }
    while (carry > 0) {
      const hi = Math.floor(carry / LIMB);
      limbs[len++] = carry - hi * LIMB;
      carry = hi;
    }
    chunk = 4;
  }
  let bytes = 0;
  if (len > 0) {
    const top = limbs[len - 1];
    bytes = (len - 1) * 3 + (top >= 65536 ? 3 : top >= 256 ? 2 : 1);
  }
  const out = new Uint8Array(zeros + bytes);
  let o = out.length;
  for (let j = 0; j < len && o > zeros; j++) {
    let v = limbs[j];
    for (let b = 0; b < 3 && o > zeros; b++) {
      out[--o] = v & 0xff;
      v >>= 8;
    }
  }
  return out;
}
