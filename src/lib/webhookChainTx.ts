/**
 * The webhook's chain lookup (D1, report idx-webhook-abuse-replay-20261120).
 *
 * A Helius delivery is only a hint that a signature landed. Its rows are built from the transaction the RPC
 * returns for that signature, never from the payload: a payload with a made-up signature, or a real signature
 * with a swapped trader / slab / size, would otherwise be stored for good (trades upsert with ignoreDuplicates,
 * so the first writer of a key owns it, on every path).
 *
 * One `getParsedTransactions` call per CHAIN_TX_BATCH signatures (the poll path's fetch, fetchParsedTxsTolerant),
 * and a bounded memo so a redelivery of the same signatures (Helius retries on 5xx) costs no second read.
 */
import type { ParsedTransactionWithMeta } from "@solana/web3.js";
import { withRetry } from "@percolatorct/shared";
import { fetchParsedTxsTolerant, type TxFetcher } from "./tolerantTxFetch.js";
import { BoundedTtlMap } from "./boundedTtlMap.js";

/** Signatures per getParsedTransactions call. A 250-fill delivery costs 3 calls. */
export const CHAIN_TX_BATCH = 100;

/** The Helius-enhanced fields the webhook extractor reads, rebuilt from the chain transaction. */
export interface ChainEnhancedInstruction {
  programId: string;
  data?: string;
  accounts: string[];
  innerInstructions?: ChainEnhancedInstruction[];
}
export interface ChainEnhancedTx {
  signature: string;
  slot: number;
  timestamp: number | null;
  transactionError: unknown;
  instructions: ChainEnhancedInstruction[];
  innerInstructions: [];
  /** Account post-state cannot be checked against getTransaction, so none is taken from the payload. */
  accountData: [];
  logMessages?: string[];
}

/** signature -> chain transaction in enhanced shape. Only found transactions are kept (a miss may still land). */
const newMemo = () => new BoundedTtlMap<string, ChainEnhancedTx>(5_000, 30 * 60 * 1000);
let memo = newMemo();

/** Test hook: forget memoised transactions. */
export function resetChainTxMemo(): void {
  memo = newMemo();
}

function keyOf(k: unknown): string | null {
  if (typeof k === "string") return k;
  if (k && typeof (k as { toBase58?: unknown }).toBase58 === "function") return (k as { toBase58(): string }).toBase58();
  return null;
}

function convertIx(ix: unknown): ChainEnhancedInstruction | null {
  if (!ix || typeof ix !== "object") return null;
  const o = ix as { programId?: unknown; data?: unknown; accounts?: unknown[]; parsed?: unknown };
  const programId = keyOf(o.programId);
  if (!programId) return null;
  // A jsonParsed system / token instruction carries `parsed`, not raw data: never a wrapper instruction.
  if ("parsed" in o) return { programId, accounts: [] };
  return {
    programId,
    data: typeof o.data === "string" ? o.data : undefined,
    accounts: (o.accounts ?? []).map((a) => keyOf(a) ?? ""),
  };
}

/**
 * The chain transaction in the enhanced shape the extractor reads: top-level instructions with their CPIs
 * nested (as Helius nests them), slot, block time and error from the chain. Null when the transaction has no
 * meta (its status is unknown, so nothing can be written from it).
 */
export function enhancedFromParsedTx(signature: string, tx: ParsedTransactionWithMeta): ChainEnhancedTx | null {
  if (!tx || !tx.meta) return null;
  const inner = new Map<number, unknown[]>();
  for (const g of tx.meta.innerInstructions ?? []) inner.set(g.index, g.instructions ?? []);
  const instructions: ChainEnhancedInstruction[] = [];
  (tx.transaction?.message?.instructions ?? []).forEach((ix, i) => {
    const top = convertIx(ix) ?? { programId: "", accounts: [] }; // keeps the instruction index aligned
    top.innerInstructions = (inner.get(i) ?? []).map(convertIx).filter((x): x is ChainEnhancedInstruction => x !== null);
    instructions.push(top);
  });
  return {
    signature,
    slot: tx.slot,
    timestamp: typeof tx.blockTime === "number" ? tx.blockTime : null,
    transactionError: tx.meta.err ?? null,
    instructions,
    innerInstructions: [],
    accountData: [],
    logMessages: Array.isArray(tx.meta.logMessages) ? tx.meta.logMessages : [],
  };
}

/** The RPC could not answer (transport / rate limit): the delivery must be retried, nothing may be written. */
export class ChainTxReadError extends Error {
  constructor(readonly cause: unknown) {
    super(`chain transaction read failed: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
}

/**
 * The chain transaction of every signature, in enhanced shape. A signature the RPC does not know (or cannot
 * decode for this client) maps to null. Throws ChainTxReadError when a batch read fails, so the caller can
 * answer 5xx before writing anything.
 */
export async function loadChainTxs(
  connection: TxFetcher<ParsedTransactionWithMeta>,
  signatures: string[],
): Promise<Map<string, ChainEnhancedTx | null>> {
  const out = new Map<string, ChainEnhancedTx | null>();
  const misses: string[] = [];
  for (const s of new Set(signatures)) {
    const hit = memo.get(s);
    if (hit) out.set(s, hit);
    else misses.push(s);
  }
  const chunks: string[][] = [];
  for (let i = 0; i < misses.length; i += CHAIN_TX_BATCH) chunks.push(misses.slice(i, i + CHAIN_TX_BATCH));
  const retry = <R>(fn: () => Promise<R>, label: string) => withRetry(fn, { maxRetries: 1, baseDelayMs: 200, label });
  const results = await Promise.all(chunks.map((c) => fetchParsedTxsTolerant<ParsedTransactionWithMeta>(connection, c, retry)));
  results.forEach((r, k) => {
    if (r.failed) throw new ChainTxReadError(r.error);
    chunks[k].forEach((sig, j) => {
      const tx = r.txs[j];
      const enhanced = tx ? enhancedFromParsedTx(sig, tx) : null;
      if (enhanced) memo.set(sig, enhanced);
      out.set(sig, enhanced);
    });
  });
  return out;
}

/** Solana signatures are base58-encoded 64-byte values (87-88 chars); the same shape check the extractor applies. */
const BASE58_SIGNATURE = /^[1-9A-HJ-NP-Za-km-z]{64,88}$/;

/** programId|data|accounts of every top-level instruction of the given programs, in order. */
function wrapperIxKeys(ixs: ReadonlyArray<{ programId?: unknown; data?: unknown; accounts?: unknown }> | undefined, programIds: ReadonlySet<string>): string {
  return (ixs ?? [])
    .filter((ix) => ix && typeof ix.programId === "string" && programIds.has(ix.programId))
    .map((ix) => `${ix.programId}|${typeof ix.data === "string" ? ix.data : ""}|${Array.isArray(ix.accounts) ? ix.accounts.join(",") : ""}`)
    .join(";");
}

export interface ChainCheckedDelivery {
  /** What the extractor processes: the chain transaction of each payload entry the chain confirms, in payload order. */
  txs: ChainEnhancedTx[];
  /** Payload entries the chain does not know (or whose signature is malformed): nothing is written for them. */
  unknown: string[];
  /** Payload entries whose wrapper instructions differ from the chain's: the chain's are used. */
  mismatched: string[];
}

/**
 * Replace each payload transaction by the chain's transaction for the same signature (see the file comment).
 * Payload fields are never used for rows: instructions, CPIs, slot, block time, error and logs all come from
 * the chain; payload `accountData` is dropped. Logs are carried only when the payload carried them, so the
 * delivery's v2.2 log-event path runs exactly when it did before, now on the chain's logs.
 */
export async function txsFromChain(
  payload: ReadonlyArray<{ signature?: string; instructions?: ReadonlyArray<{ programId?: unknown; data?: unknown; accounts?: unknown }>; logMessages?: unknown }>,
  connection: TxFetcher<ParsedTransactionWithMeta>,
  programIds: ReadonlySet<string>,
): Promise<ChainCheckedDelivery> {
  const isSig = (s: unknown): s is string => typeof s === "string" && BASE58_SIGNATURE.test(s);
  const sigs = payload.map((p) => p.signature).filter(isSig);
  const chain = sigs.length > 0 ? await loadChainTxs(connection, sigs) : new Map<string, ChainEnhancedTx | null>();
  const out: ChainCheckedDelivery = { txs: [], unknown: [], mismatched: [] };
  for (const p of payload) {
    const c = isSig(p.signature) ? chain.get(p.signature) : null;
    if (!c) {
      out.unknown.push(typeof p.signature === "string" ? p.signature : "");
      continue;
    }
    if (wrapperIxKeys(p.instructions, programIds) !== wrapperIxKeys(c.instructions, programIds)) out.mismatched.push(c.signature);
    out.txs.push(Array.isArray(p.logMessages) ? c : { ...c, logMessages: undefined });
  }
  return out;
}
