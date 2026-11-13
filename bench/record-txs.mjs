// Read-only: fetches parsed transactions (jsonParsed, maxSupportedTransactionVersion 1) for every unique signature in sigs.json,
// one call at a time (public devnet refuses batched getTransaction), ~2 req/s.
import { Connection } from "@solana/web3.js";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
const [sigsFile, out] = process.argv.slice(2);
const sigs = [...new Set(Object.values(JSON.parse(readFileSync(sigsFile, "utf8"))).flat().map((s) => s.signature))];
const c = new Connection(process.env.BENCH_RPC ?? "https://api.devnet.solana.com", { commitment: "confirmed", disableRetryOnRateLimit: true });
const res = existsSync(out) ? JSON.parse(readFileSync(out, "utf8")) : {};
for (const s of sigs) {
  if (res[s]) continue;
  for (let attempt = 0; ; attempt++) {
    try { res[s] = await c.getParsedTransaction(s, { maxSupportedTransactionVersion: 1, commitment: "confirmed" }); break; }
    catch (e) { if (attempt > 6) throw e; await new Promise((r) => setTimeout(r, 2000 * (attempt + 1))); }
  }
  await new Promise((r) => setTimeout(r, 500));
}
writeFileSync(out, JSON.stringify(res));
console.log("fetched", Object.keys(res).length, "null", Object.values(res).filter((x) => !x).length);
