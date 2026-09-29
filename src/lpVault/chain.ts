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

/**
 * Shares of a live redemption request, or 0. ExecuteRedemption zeroes the magic
 * and drains the lamports; CancelRedemption closes it — either way it is gone.
 */
export function parseRedemptionShares(data: Uint8Array | null, registry: string, user: string): bigint {
  if (!data || data.length < LP_REDEMPTION_LEN || data[10] !== KIND_LP_REDEMPTION) return 0n;
  if (data.subarray(0, 8).every((b) => b === 0)) return 0n;
  const reg = new PublicKey(data.subarray(HEADER_LEN, HEADER_LEN + 32)).toBase58();
  const who = new PublicKey(data.subarray(HEADER_LEN + 32, HEADER_LEN + 64)).toBase58();
  if (reg !== registry || who !== user) return 0n;
  let v = 0n;
  for (let i = 15; i >= 0; i--) v = (v << 8n) | BigInt(data[HEADER_LEN + 64 + i]!);
  return v;
}

export function deriveRedemptionPda(programId: string, registry: string, user: string): PublicKey {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("lp_redemption"), new PublicKey(registry).toBuffer(), new PublicKey(user).toBuffer()],
    new PublicKey(programId),
  )[0];
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
      maxSupportedTransactionVersion: 0,
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
