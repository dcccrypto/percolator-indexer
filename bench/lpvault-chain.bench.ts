/**
 * LP-vault chain bench: RpcLpVaultChain.getClaim (fake Connection, 200 users x 5 registries) and parseRedemptionShares.
 * Prints us/call (median of BENCH_RUNS) and a hash of all outputs so before/after can prove identical results.
 *
 * Run:  npx tsx bench/lpvault-chain.bench.ts
 */
import { createHash } from "node:crypto";
import { Keypair, PublicKey, type Connection } from "@solana/web3.js";
import { HEADER_LEN, KIND_LP_REDEMPTION, LP_REDEMPTION_LEN, RpcLpVaultChain, parseRedemptionShares } from "../src/lpVault/chain.js";

const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const CLAIMS = 5000;
const PARSES = 50000;

// Deterministic keys.
const key = (i: number) => new PublicKey(Uint8Array.from({ length: 32 }, (_, j) => (i * 31 + j * 7 + 1) & 0xff)).toBase58();
const users = Array.from({ length: 200 }, (_, i) => key(1000 + i));
const registries = Array.from({ length: 5 }, (_, i) => key(i + 1));
const PROGRAM = Keypair.fromSeed(new Uint8Array(32).fill(9)).publicKey.toBase58();
const MINT = key(77);

function redemption(registry: string, user: string, shares: bigint): Uint8Array {
  const d = new Uint8Array(LP_REDEMPTION_LEN);
  d.fill(0x42, 0, 8);
  d[10] = KIND_LP_REDEMPTION;
  d.set(new PublicKey(registry).toBytes(), HEADER_LEN);
  d.set(new PublicKey(user).toBytes(), HEADER_LEN + 32);
  let v = shares;
  for (let i = 0; i < 16; i++) { d[HEADER_LEN + 64 + i] = Number(v & 0xffn); v >>= 8n; }
  return d;
}

const conn = {
  getParsedTokenAccountsByOwner: async () => ({ context: { slot: 5 }, value: [{ account: { data: { parsed: { info: { tokenAmount: { amount: "1000" } } } } } }] }),
  getAccountInfoAndContext: async () => ({ context: { slot: 6 }, value: null }),
} as unknown as Connection;

const median = (a: number[]) => a.slice().sort((x, y) => x - y)[Math.floor(a.length / 2)]!;

async function main() {
  const chain = new RpcLpVaultChain(conn);
  const h = createHash("sha256");
  const claimUs: number[] = [];
  for (let r = 0; r < RUNS; r++) {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < CLAIMS; i++) {
      const c = await chain.getClaim(users[i % 200]!, MINT, registries[i % 5]!, PROGRAM);
      if (r === 0) h.update(`${c.heldShares}|${c.pendingShares}|${c.contextSlot};`);
    }
    claimUs.push(Number(process.hrtime.bigint() - t0) / 1000 / CLAIMS);
  }

  // PDA outputs hashed too (what the fake conn ignores).
  const { deriveRedemptionPda } = await import("../src/lpVault/chain.js");
  for (let i = 0; i < 1000; i++) h.update(deriveRedemptionPda(PROGRAM, registries[i % 5]!, users[i % 200]!).toBase58());

  // 50k parses: 1/2 matching, 1/4 wrong user, 1/4 zeroed magic.
  const bufs: { d: Uint8Array; reg: string; user: string }[] = [];
  for (let i = 0; i < 1000; i++) {
    const reg = registries[i % 5]!, user = users[i % 200]!;
    const d = redemption(reg, user, BigInt(i) * 1_000_003n);
    if (i % 4 === 2) bufs.push({ d, reg, user: users[(i + 1) % 200]! });
    else if (i % 4 === 3) { d.fill(0, 0, 8); bufs.push({ d, reg, user }); }
    else bufs.push({ d, reg, user });
  }
  const parseUs: number[] = [];
  for (let r = 0; r < RUNS; r++) {
    const t0 = process.hrtime.bigint();
    let acc = 0n;
    for (let i = 0; i < PARSES; i++) { const b = bufs[i % 1000]!; acc += parseRedemptionShares(b.d, b.reg, b.user); }
    parseUs.push(Number(process.hrtime.bigint() - t0) / 1000 / PARSES);
    if (r === 0) h.update(acc.toString());
  }
  console.log(`getClaim            ${median(claimUs).toFixed(2)} us/call  (${CLAIMS} calls, 200 users x 5 registries)`);
  console.log(`parseRedemptionShares ${median(parseUs).toFixed(2)} us/call  (${PARSES} calls)`);
  console.log(`output hash ${h.digest("hex").slice(0, 16)}`);
}
main().then(() => process.exit(0));
