// BATCH-ORDER-1: the public devnet answers a getTransaction batch with every item's id right but in a shuffled position
// (recorded: perm 0,4,3,1,2 / 9,2,3,0,5,7,4,1,6,8). web3.js maps response[i] -> sigs[i] by position, so before the fix
// fetchParsedTxsTolerant returned another transaction at sigs[i] (failed:false) and the poll wrote it under sigs[i].
// These go through a real web3 Connection with a fake fetch, so the reordering happens where it does on devnet.
import { describe, it, expect } from "vitest";
import { Connection, PublicKey } from "@solana/web3.js";
import { fetchParsedTxsTolerant, TxPairingError } from "../../src/lib/tolerantTxFetch";

const tx = (sig: string, slot: number) => ({
  slot, blockTime: 1, version: "legacy",
  meta: null,
  transaction: {
    signatures: [sig],
    message: { accountKeys: [{ pubkey: new PublicKey(new Uint8Array(32).fill(slot)).toBase58(), signer: true, writable: true, source: "transaction" }], instructions: [], recentBlockhash: "11111111111111111111111111111111" },
  },
});

type Req = { id: string; params: [string] };
/** A Connection whose batch answer is built by `answer` from the request items (in request order). */
function conn(answer: (req: Req[]) => unknown[]) {
  const fetch = (async (_u: unknown, init: { body: string }) => {
    const body = JSON.parse(init.body) as Req | Req[];
    const out = Array.isArray(body) ? answer(body) : answer([body])[0];
    return new Response(JSON.stringify(out), { status: 200, headers: { "content-type": "application/json" } });
  }) as unknown as typeof globalThis.fetch;
  return new Connection("http://replay.invalid", { commitment: "confirmed", fetch });
}
const ok = (r: Req, result: unknown) => ({ jsonrpc: "2.0", id: r.id, result });
const sigsOf = (n: number) => Array.from({ length: n }, (_, i) => String.fromCharCode(97 + i).repeat(64));
const retry = <R>(fn: () => Promise<R>) => fn();

describe("batch responses out of request order (real public-devnet behaviour)", () => {
  it("fetchParsedTxsTolerant returns txs[i] for sigs[i] even when the RPC reorders the batch", async () => {
    const sigs = sigsOf(5);
    const perm = [0, 4, 3, 1, 2]; // recorded devnet order, call 14
    const r = await fetchParsedTxsTolerant(conn((req) => perm.map((k) => ok(req[k]!, tx(req[k]!.params[0], k + 1)))), sigs, retry);
    expect(r.failed).toBe(false);
    expect(r.txs.map((t) => t?.transaction.signatures[0])).toEqual(sigs);
  });

  it("the recorded 10-item devnet order (call 31), with a not-found item: each tx at its own signature, the null at the unknown one", async () => {
    const sigs = sigsOf(10);
    const perm = [9, 2, 3, 0, 5, 7, 4, 1, 6, 8];
    const r = await fetchParsedTxsTolerant(conn((req) => perm.map((k) => ok(req[k]!, k === 4 ? null : tx(req[k]!.params[0], k + 1)))), sigs, retry);
    expect(r.failed).toBe(false);
    expect(r.txs.map((t) => t?.transaction.signatures[0] ?? null)).toEqual(sigs.map((s, i) => (i === 4 ? null : s)));
    expect(r.txs.map((t) => t?.slot ?? null)).toEqual(sigs.map((_, i) => (i === 4 ? null : i + 1)));
  });

  it("a transaction of a signature that was not requested is never paired: held (failed), retried", async () => {
    const sigs = sigsOf(3);
    let calls = 0;
    const c = conn((req) => req.map((r, k) => ok(r, tx(k === 1 ? "z".repeat(64) : r.params[0], k + 1))));
    const r = await fetchParsedTxsTolerant(c, sigs, async (fn) => { calls++; try { return await fn(); } catch { calls++; return fn(); } });
    expect(calls).toBe(2); // the pairing failure is thrown inside the retry wrapper, so withRetry retries it
    expect(r).toMatchObject({ failed: true, skipped: [], txs: [null, null, null] });
    expect(r.error).toBeInstanceOf(TxPairingError);
  });

  it("the same signature returned twice (one requested signature left unanswered) is held", async () => {
    const sigs = sigsOf(3);
    const r = await fetchParsedTxsTolerant(conn((req) => req.map((x, k) => ok(x, tx(req[k === 2 ? 0 : k]!.params[0], k + 1)))), sigs, retry);
    expect(r).toMatchObject({ failed: true, txs: [null, null, null] });
  });

  it("a transaction without a signature cannot be paired: held", async () => {
    const sigs = sigsOf(2);
    const r = await fetchParsedTxsTolerant(conn((req) => req.map((x, k) => {
      const t = tx(x.params[0], k + 1) as { transaction: { signatures?: string[] } };
      if (k === 0) t.transaction.signatures = [];
      return ok(x, t);
    })), sigs, retry);
    expect(r).toMatchObject({ failed: true, txs: [null, null] });
  });

  it("per-signature fallback: a single answer carrying another signature is held, never paired", async () => {
    const sigs = sigsOf(2);
    const V2 = { code: -32015, message: "Transaction version (2) is not supported by the requesting client" };
    const c = conn((req) => req.length > 1
      ? req.map((x) => ({ jsonrpc: "2.0", id: x.id, error: V2 }))
      : [ok(req[0]!, tx("z".repeat(64), 1))]);
    const r = await fetchParsedTxsTolerant(c, sigs, retry);
    expect(r).toMatchObject({ failed: true, skipped: [] });
    expect(r.error).toBeInstanceOf(TxPairingError);
  });
});
