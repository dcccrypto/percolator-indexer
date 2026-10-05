/**
 * GH#207 — decode the wrapper's Earn LP-vault instructions into per-user events.
 *
 * The Earn card is the WRAPPER LP vault (tags 74–81 on the v18 wrapper), not the
 * stake program. Layouts are read from the deployed wrapper source
 * (percolator-prog @ 6377376a, `src/v16_program.rs`):
 *
 *   75 DepositToLpVault      data: tag u8 | amount u128 | domain u16           (19 B)
 *        accounts: 0 depositor(signer) 1 market 2 registry 3 lp_mint
 *                  4 depositor_lp_ata 5 source_token 6 vault_token 7 ledger
 *                  8 token_program 9 system_program 10 sibling_ledger
 *        CPIs:     spl Transfer(5 -> 6, amount)   spl MintTo(3 -> 4, minted)
 *   76 RequestRedeemLpShares data: tag u8 | shares u128                        (17 B)
 *        accounts: 0 redeemer(signer) 1 registry 2 lp_mint 3 redeemer_lp_ata
 *                  4 escrow 5 redemption 6 token_program 7 system_program
 *        CPIs:     spl Transfer(3 -> 4, shares)   (shares ESCROWED, not burned)
 *   77 ExecuteRedemption     data: tag u8 | domain u16                          (3 B)
 *        accounts: 0 cranker(signer, ANYONE) 1 market 2 registry 3 redemption
 *                  4 lp_mint 5 escrow 6 vault_token 7 vault_authority 8 ledger
 *                  9 redeemer_dest 10 token_program 11 sibling_ledger
 *                  12 redeemer_rent_dest (pinned on-chain to redemption.redeemer)
 *        CPIs:     spl Transfer(6 -> 9, payout)   spl Burn(5, mint 4, shares)
 *   81 CancelRedemption      data: tag u8                                       (1 B)
 *        accounts: 0 redeemer(signer) 1 registry 2 redemption 3 lp_mint
 *                  4 redeemer_lp_ata 5 escrow 6 token_program
 *        CPIs:     spl Transfer(5 -> 4, shares)
 *
 * Why amounts come from the CPIs and not only from instruction data:
 *  - Deposit: the collateral IN is `amount` (ix data), but the shares MINTED are
 *    computed on-chain from NAV (and the vault's genesis deposit withholds
 *    LP_VAULT_MINIMUM_LIQUIDITY = 1000 dead shares), so they are only knowable from
 *    the MintTo CPI.
 *  - Execute: the instruction carries no amount at all. Collateral OUT is the vault
 *    -> redeemer Transfer CPI and the shares redeemed are the Burn CPI.
 * The program emits no summary log, so the CPIs are the only exact source.
 *
 * Execute is permissionless: the signer is a cranker, NOT the LP. The LP is
 * account 12, which the handler pins to `redemption.redeemer` (GH#412) — the
 * only account in that instruction guaranteed to be the redeemer's wallet.
 */
import type { VersionedTransactionResponse } from "@solana/web3.js";
import { decodeBase58 } from "@percolatorct/shared";

export const TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

export const LP_VAULT_TAG = {
  CreateLpVault: 74,
  DepositToLpVault: 75,
  RequestRedeemLpShares: 76,
  ExecuteRedemption: 77,
  LpVaultCrankFees: 78,
  CancelRedemption: 81,
} as const;

export type LpVaultEventKind = "deposit" | "request_redeem" | "cancel_redeem" | "execute_redeem";

export interface LpVaultEvent {
  signature: string;
  /**
   * Position of the wrapper instruction in the transaction. Top-level instruction
   * `i` is `i`; an instruction reached by CPI at position `j` of top-level `i`'s
   * inner list is `1000 * (i + 1) + j`. Unique per (signature) and stable across
   * re-fetches, so (signature, ix_index) is an idempotent key.
   */
  ixIndex: number;
  slot: number;
  blockTime: number | null;
  kind: LpVaultEventKind;
  programId: string;
  /** Market slab. Present in deposit/execute; null for request/cancel (not an account there). */
  market: string | null;
  registry: string;
  user: string;
  lpMint: string | null;
  /** Collateral atoms IN (deposit) or OUT (execute); 0 for request/cancel. */
  collateralAtoms: bigint;
  /**
   * LP shares minted (deposit), escrowed (request), returned (cancel) or burned
   * (execute). `null` when the amount could not be read from the CPIs — the event
   * is still recorded, and the position is then marked basis-unknown.
   */
  lpAmount: bigint | null;
  domain: number | null;
}

// ── normalized transaction shape ────────────────────────────────────────────

export interface NormIx {
  programId: string;
  accounts: string[];
  data: Uint8Array;
  /** 1 = top level, 2 = CPI from a top-level ix, … ; null if the source did not say. */
  stackHeight: number | null;
}

export interface NormTopIx extends NormIx {
  /** Every instruction executed beneath this one, in execution order. */
  inner: NormIx[];
}

export interface NormTx {
  signature: string;
  slot: number;
  blockTime: number | null;
  failed: boolean;
  instructions: NormTopIx[];
}

/**
 * Normalize a web3.js `getTransaction(sig, { maxSupportedTransactionVersion: 1 })`
 * response (legacy, v0 with lookup tables, or v1: no lookup tables, `meta.loadedAddresses` empty).
 * Needs @solana/web3.js >= 1.99.0: older versions cannot parse a v1 response at all.
 */
export function normalizeRpcTransaction(signature: string, resp: VersionedTransactionResponse): NormTx {
  const msg = resp.transaction.message;
  const keys = msg
    .getAccountKeys({ accountKeysFromLookups: resp.meta?.loadedAddresses ?? undefined })
    .keySegments()
    .flat()
    .map((k) => k.toBase58());
  const key = (i: number): string => {
    const k = keys[i];
    if (k === undefined) throw new Error(`account index ${i} out of range (${keys.length} keys) in ${signature}`);
    return k;
  };

  const innerByIndex = new Map<number, NormIx[]>();
  for (const group of resp.meta?.innerInstructions ?? []) {
    innerByIndex.set(
      group.index,
      group.instructions.map((ix) => ({
        programId: key(ix.programIdIndex),
        accounts: ix.accounts.map(key),
        data: decodeBase58(ix.data) ?? new Uint8Array(),
        // Present on RPC responses since 1.14 but absent from web3.js's type.
        stackHeight: stackHeightOf(ix),
      })),
    );
  }

  return {
    signature,
    slot: resp.slot,
    blockTime: resp.blockTime ?? null,
    failed: resp.meta?.err != null,
    instructions: msg.compiledInstructions.map((ix, i) => ({
      programId: key(ix.programIdIndex),
      accounts: ix.accountKeyIndexes.map(key),
      data: ix.data,
      stackHeight: 1,
      inner: innerByIndex.get(i) ?? [],
    })),
  };
}

function stackHeightOf(ix: object): number | null {
  const h: unknown = (ix as { stackHeight?: unknown }).stackHeight;
  return typeof h === "number" ? h : null;
}

// ── little-endian readers ───────────────────────────────────────────────────

function readU64(d: Uint8Array, off: number): bigint | null {
  if (d.length < off + 8) return null;
  let v = 0n;
  for (let i = 7; i >= 0; i--) v = (v << 8n) | BigInt(d[off + i]!);
  return v;
}
function readU128(d: Uint8Array, off: number): bigint | null {
  const lo = readU64(d, off);
  const hi = readU64(d, off + 8);
  return lo === null || hi === null ? null : (hi << 64n) | lo;
}
function readU16(d: Uint8Array, off: number): number | null {
  return d.length < off + 2 ? null : d[off]! | (d[off + 1]! << 8);
}

// ── SPL Token CPI matching ─────────────────────────────────────────────────

type SplKind = "transfer" | "mintTo" | "burn";

/**
 * Amount of the first SPL Token instruction among `children` that matches `kind`
 * and the given account constraints. Handles both the plain and `*Checked`
 * variants (the deployed wrapper uses the plain ones; the checked forms are
 * accepted so a future wrapper switching to them is not silently mis-read).
 */
function splAmount(
  children: NormIx[],
  kind: SplKind,
  match: { source?: string; dest?: string; mint?: string; account?: string },
): bigint | null {
  for (const ix of children) {
    if (ix.programId !== TOKEN_PROGRAM_ID || ix.data.length < 9) continue;
    const t = ix.data[0]!;
    const a = ix.accounts;
    let ok = false;
    if (kind === "transfer") {
      if (t === 3) ok = (!match.source || a[0] === match.source) && (!match.dest || a[1] === match.dest);
      else if (t === 12) ok = (!match.source || a[0] === match.source) && (!match.dest || a[2] === match.dest);
    } else if (kind === "mintTo") {
      if (t === 7 || t === 14) ok = (!match.mint || a[0] === match.mint) && (!match.dest || a[1] === match.dest);
    } else if (t === 8 || t === 15) {
      ok = (!match.account || a[0] === match.account) && (!match.mint || a[1] === match.mint);
    }
    if (ok) return readU64(ix.data, 1);
  }
  return null;
}

/**
 * The instructions executed directly by the wrapper instruction at `pos` of
 * `inner` (stack height h): the following entries at height h+1, stopping at the
 * first entry at height <= h. Returns null when heights are missing, because
 * attribution would then be a guess.
 */
function childrenOfInner(inner: NormIx[], pos: number): NormIx[] | null {
  const h = inner[pos]!.stackHeight;
  if (h === null) return null;
  const out: NormIx[] = [];
  for (let k = pos + 1; k < inner.length; k++) {
    const sh = inner[k]!.stackHeight;
    if (sh === null) return null;
    if (sh <= h) break;
    if (sh === h + 1) out.push(inner[k]!);
  }
  return out;
}

function childrenOfTop(top: NormTopIx): NormIx[] {
  // Direct CPIs only. Without heights every inner ix is treated as a child; the
  // token-account constraints in splAmount keep that unambiguous for these ixs.
  return top.inner.filter((ix) => ix.stackHeight === null || ix.stackHeight === 2);
}

function decodeOne(
  ix: NormIx,
  children: NormIx[] | null,
  base: { signature: string; ixIndex: number; slot: number; blockTime: number | null },
): LpVaultEvent | null {
  const d = ix.data;
  if (d.length === 0) return null;
  const tag = d[0]!;
  const a = ix.accounts;
  const kids = children ?? [];
  const common = { ...base, programId: ix.programId };

  switch (tag) {
    case LP_VAULT_TAG.DepositToLpVault: {
      if (d.length < 19 || a.length < 11) return null;
      const amount = readU128(d, 1)!;
      const [user, market, registry, lpMint, lpAta, source, vaultToken] = a as [string, string, string, string, string, string, string];
      const moved = splAmount(kids, "transfer", { source, dest: vaultToken });
      // The handler transfers exactly `amount`. A disagreement means these CPIs
      // are not the ones we think they are, so the shares are not trusted either:
      // the event is kept with lpAmount = null, which marks the basis unknown
      // rather than recording a wrong one or wedging ingestion on a throw.
      const consistent = moved === null || moved === amount;
      return {
        ...common, kind: "deposit", market, registry, user, lpMint,
        collateralAtoms: amount,
        lpAmount: consistent ? splAmount(kids, "mintTo", { mint: lpMint, dest: lpAta }) : null,
        domain: readU16(d, 17),
      };
    }
    case LP_VAULT_TAG.RequestRedeemLpShares: {
      if (d.length < 17 || a.length < 8) return null;
      const [user, registry, lpMint] = a as [string, string, string];
      return {
        ...common, kind: "request_redeem", market: null, registry, user, lpMint,
        collateralAtoms: 0n,
        lpAmount: readU128(d, 1),
        domain: null,
      };
    }
    case LP_VAULT_TAG.ExecuteRedemption: {
      if (d.length < 3 || a.length < 13) return null;
      const market = a[1]!;
      const registry = a[2]!;
      const lpMint = a[4]!;
      const escrow = a[5]!;
      const vaultToken = a[6]!;
      const dest = a[9]!;
      const user = a[12]!;
      const out = splAmount(kids, "transfer", { source: vaultToken, dest });
      return {
        ...common, kind: "execute_redeem", market, registry, user, lpMint,
        // The handler skips the Transfer CPI only for a 0-atom payout, which it
        // rejects earlier; a missing Transfer with a present Burn is therefore 0.
        collateralAtoms: out ?? 0n,
        lpAmount: splAmount(kids, "burn", { account: escrow, mint: lpMint }),
        domain: readU16(d, 1),
      };
    }
    case LP_VAULT_TAG.CancelRedemption: {
      if (a.length < 7) return null;
      const [user, registry, , lpMint, lpAta, escrow] = a as [string, string, string, string, string, string];
      return {
        ...common, kind: "cancel_redeem", market: null, registry, user, lpMint,
        collateralAtoms: 0n,
        lpAmount: splAmount(kids, "transfer", { source: escrow, dest: lpAta }),
        domain: null,
      };
    }
    default:
      return null;
  }
}

/**
 * Every LP-vault deposit / redemption event in `tx`, top level and inner CPI.
 * Failed transactions yield nothing (nothing moved).
 */
export function decodeLpVaultEvents(tx: NormTx, wrapperProgramIds: ReadonlySet<string>): LpVaultEvent[] {
  if (tx.failed) return [];
  const events: LpVaultEvent[] = [];
  const base = { signature: tx.signature, slot: tx.slot, blockTime: tx.blockTime };

  tx.instructions.forEach((top, i) => {
    if (wrapperProgramIds.has(top.programId)) {
      const ev = decodeOne(top, childrenOfTop(top), { ...base, ixIndex: i });
      if (ev) events.push(ev);
    }
    top.inner.forEach((ix, j) => {
      if (!wrapperProgramIds.has(ix.programId)) return;
      const ev = decodeOne(ix, childrenOfInner(top.inner, j), { ...base, ixIndex: 1000 * (i + 1) + j });
      if (ev) events.push(ev);
    });
  });
  return events;
}
