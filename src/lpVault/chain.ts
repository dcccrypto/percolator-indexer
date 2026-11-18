/**
 * GH#207 — the chain reads the LP-vault indexer needs, behind an interface so
 * the ingest/reconcile logic is testable against recorded devnet transactions.
 */
import { PublicKey, type Connection } from "@solana/web3.js";
import { normalizeRpcTransaction, type NormTx } from "./decoder.js";

/** Wrapper account header: magic u64 | version u16 | kind u8 | pad -> 16 bytes. */
export const HEADER_LEN = 16;
export const KIND_LP_VAULT_REGISTRY = 5;
export const KIND_LP_REDEMPTION = 6;
/** HEADER + LpVaultRegistryV16 (160 B, `const _: () = assert!(size_of == 160)`). */
export const LP_VAULT_REGISTRY_LEN = HEADER_LEN + 160;
/** HEADER + LpRedemptionV16 (96 B). */
export const LP_REDEMPTION_LEN = HEADER_LEN + 96;

export interface RegistryInfo {
  registry: string;
  programId: string;
  market: string;
  lpMint: string;
}

export interface SigInfo {
  signature: string;
  slot: number;
  failed: boolean;
}

export interface OnchainClaim {
  /** Sum over every token account the user owns for the LP mint. */
  heldShares: bigint;
  /** Shares escrowed by a live (unconsumed) redemption request. */
  pendingShares: bigint;
  contextSlot: number;
}

export interface LpVaultChain {
  listRegistries(programIds: readonly string[]): Promise<RegistryInfo[]>;
  /** Newest first, like getSignaturesForAddress. */
  getSignatures(address: string, opts: { until?: string; before?: string; limit: number }): Promise<SigInfo[]>;
  getTransaction(signature: string): Promise<NormTx | null>;
  getClaim(user: string, lpMint: string, registry: string, programId: string): Promise<OnchainClaim>;
}

export function parseRegistry(registry: string, programId: string, data: Uint8Array): RegistryInfo | null {
  if (data.length < LP_VAULT_REGISTRY_LEN || data[10] !== KIND_LP_VAULT_REGISTRY) return null;
  const market = new PublicKey(data.subarray(HEADER_LEN, HEADER_LEN + 32)).toBase58();
  const lpMint = new PublicKey(data.subarray(HEADER_LEN + 32, HEADER_LEN + 64)).toBase58();
  return { registry, programId, market, lpMint };
}

const KEY_CACHE_MAX = 4096;

/** Insertion-ordered cache, oldest evicted when full (never clear()). */
function boundedSet<K, V>(m: Map<K, V>, k: K, v: V): void {
  m.set(k, v);
  if (m.size > KEY_CACHE_MAX) {
    const oldest = m.keys().next();
    if (!oldest.done) m.delete(oldest.value);
  }
}

/** base58 -> 32 raw bytes, or null when it is not a valid 32-byte key (such a string can never equal a toBase58()). */
const keyBytesCache = new Map<string, Uint8Array | null>();
function keyBytes(b58: string): Uint8Array | null {
  const hit = keyBytesCache.get(b58);
  if (hit !== undefined) return hit;
  let out: Uint8Array | null;
  try {
    out = new PublicKey(b58).toBytes();
  } catch {
    out = null;
  }
  boundedSet(keyBytesCache, b58, out);
  return out;
}

function bytesEqual(data: Uint8Array, off: number, key: Uint8Array): boolean {
  for (let i = 0; i < 32; i++) if (data[off + i] !== key[i]) return false;
  return true;
}

/**
 * Shares of a live redemption request, or 0. ExecuteRedemption zeroes the magic
 * and drains the lamports; CancelRedemption closes it — either way it is gone.
 */
export function parseRedemptionShares(data: Uint8Array | null, registry: string, user: string): bigint {
  if (!data || data.length < LP_REDEMPTION_LEN || data[10] !== KIND_LP_REDEMPTION) return 0n;
  if (data.subarray(0, 8).every((b) => b === 0)) return 0n;
  // Base58 is a bijection on 32-byte keys, so comparing raw bytes equals comparing toBase58() strings.
  const reg = keyBytes(registry);
  const who = keyBytes(user);
  if (!reg || !who) return 0n;
  if (!bytesEqual(data, HEADER_LEN, reg) || !bytesEqual(data, HEADER_LEN + 32, who)) return 0n;
  let v = 0n;
  for (let i = 15; i >= 0; i--) v = (v << 8n) | BigInt(data[HEADER_LEN + 64 + i]!);
  return v;
}

const pdaCache = new Map<string, PublicKey>();
const REDEMPTION_SEED = Buffer.from("lp_redemption");

export function deriveRedemptionPda(programId: string, registry: string, user: string): PublicKey {
  const ck = `${programId}|${registry}|${user}`;
  const hit = pdaCache.get(ck);
  if (hit) return hit;
  const pda = PublicKey.findProgramAddressSync(
    [REDEMPTION_SEED, new PublicKey(registry).toBuffer(), new PublicKey(user).toBuffer()],
    new PublicKey(programId),
  )[0];
  boundedSet(pdaCache, ck, pda);
  return pda;
}

export class RpcLpVaultChain implements LpVaultChain {
  constructor(private readonly conn: Connection) {}

  async listRegistries(programIds: readonly string[]): Promise<RegistryInfo[]> {
    const out: RegistryInfo[] = [];
    for (const programId of programIds) {
      const accounts = await this.conn.getProgramAccounts(new PublicKey(programId), {
        commitment: "confirmed",
        filters: [
          { dataSize: LP_VAULT_REGISTRY_LEN },
          // kind byte at offset 10; base58 of the single byte 0x05 is "6".
          { memcmp: { offset: 10, bytes: "6" } },
        ],
      });
      for (const a of accounts) {
        const info = parseRegistry(a.pubkey.toBase58(), programId, a.account.data);
        if (info) out.push(info);
      }
    }
    return out;
  }

  async getSignatures(address: string, opts: { until?: string; before?: string; limit: number }): Promise<SigInfo[]> {
    const sigs = await this.conn.getSignaturesForAddress(new PublicKey(address), opts, "confirmed");
    return sigs.map((s) => ({ signature: s.signature, slot: s.slot, failed: s.err !== null }));
  }

  async getTransaction(signature: string): Promise<NormTx | null> {
    const resp = await this.conn.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 1,
    });
    return resp ? normalizeRpcTransaction(signature, resp) : null;
  }

  async getClaim(user: string, lpMint: string, registry: string, programId: string): Promise<OnchainClaim> {
    const owner = new PublicKey(user);
    const [held, redemption] = await Promise.all([
      this.conn.getParsedTokenAccountsByOwner(owner, { mint: new PublicKey(lpMint) }, "confirmed"),
      this.conn.getAccountInfoAndContext(deriveRedemptionPda(programId, registry, user), "confirmed"),
    ]);
    let heldShares = 0n;
    for (const { account } of held.value) {
      const parsed = account.data as { parsed?: { info?: { tokenAmount?: { amount?: string } } } };
      const amt = parsed.parsed?.info?.tokenAmount?.amount;
      if (typeof amt === "string") heldShares += BigInt(amt);
    }
    return {
      heldShares,
      pendingShares: parseRedemptionShares(redemption.value?.data ?? null, registry, user),
      // The later of the two reads: the guard in the indexer compares against it.
      contextSlot: Math.max(held.context.slot, redemption.context.slot),
    };
  }
}
