// Read-only: records getSignaturesForAddress(limit) for the given slabs from a public devnet RPC.
import { Connection, PublicKey } from "@solana/web3.js";
import { writeFileSync } from "node:fs";
const [out, limit, ...slabs] = process.argv.slice(2);
const c = new Connection(process.env.BENCH_RPC ?? "https://api.devnet.solana.com", "confirmed");
const res = {};
for (const s of slabs) {
  res[s] = await c.getSignaturesForAddress(new PublicKey(s), { limit: Number(limit) });
  await new Promise((r) => setTimeout(r, 400));
}
writeFileSync(out, JSON.stringify(res));
const all = Object.values(res).flat().map((x) => x.signature);
console.log("sigs", all.length, "unique", new Set(all).size, "err", Object.values(res).flat().filter((x) => x.err).length);
for (const [s, v] of Object.entries(res)) console.log(s.slice(0, 6), v.length, v.length ? new Date(v.at(-1).blockTime * 1000).toISOString() + " .. " + new Date(v[0].blockTime * 1000).toISOString() : "");
