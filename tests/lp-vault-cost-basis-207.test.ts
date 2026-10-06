import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { Keypair, Message, PublicKey, type VersionedTransactionResponse } from "@solana/web3.js";
import {
  encodeDepositToLpVault,
  encodeExecuteRedemption,
  encodeRequestRedeemLpShares,
} from "@percolatorct/sdk";
import {
  decodeLpVaultEvents,
  normalizeRpcTransaction,
  TOKEN_PROGRAM_ID,
  type NormIx,
  type NormTx,
} from "../src/lpVault/decoder.js";
import { basisKnown, foldPosition, type FoldEvent } from "../src/lpVault/position.js";
import { MemoryLpVaultStore, recomputePosition } from "../src/lpVault/store.js";
import {
  parseRedemptionShares,
  type LpVaultChain,
  type OnchainClaim,
  type RegistryInfo,
  type SigInfo,
} from "../src/lpVault/chain.js";
import { LpVaultIndexer } from "../src/services/LpVaultIndexer.js";

/**
 * GH#207 — Earn LP-vault cost basis.
 *
 * The fixtures are REAL devnet transactions against the live v18 wrapper
 * (GnwdeQr…), fetched with getTransaction(json) — every successful LP-vault
 * transaction that existed on devnet at the time of writing (2 vaults with
 * deposits: registry FXUCNn… on market 7mgX3b…, registry CrscF8… on market
 * 5bVTTM…). No ExecuteRedemption / CancelRedemption has ever landed on devnet,
 * so those two are covered with SDK-encoded instruction bytes and CPI shapes
 * copied from the handler (percolator-prog @ 6377376a).
 */

const WRAPPER = "GnwdeQrAh4qzChJeVLrM21CXXWC1akjLH3DiijwzEEYZ";
const WRAPPERS = new Set([WRAPPER]);
const FIXTURES = join(__dirname, "fixtures", "lp-vault");

const SIG = {
  createFxuc: "3fvdkrtrfa3kq1KEjbGqxHTgMzFTEWELDMtj25WvpxEj6nSHDp9x9dcAqZW7odL3oBsj6uMDGUfYQEG5GsEiypzC",
  depositFxuc: "626qJJquVopCrgdwNWpfmcn5NLmWfbJrg6tYZZYTSrK3ihHHGZ7r616BQ72uEVGKH1go5EMbKZm166GkPcGtAnP2",
  requestFxuc: "4pdjLxJ5eaDgnnzPQY7ENuCreFqFXkSTADVRc3Z3mGgiNkfqevH8e56bVCAKNxN4EPhrekpixveX719VQMTBwmG",
  crankFxuc: "5xq4yAXHrYsqEqyF4CDLMAgtgtUKm3STxrCex3pUuAtwvfidrg54xw8jkmLMGdi4CCpoKoBkgtTuQVZYf33WMHKA",
  createCrsc: "48FRzB8uqx7cRe9XoRaseXz4HLZHncuUKssZyYGzXv9ANFHd1n423z9rVm7K9ZMFUvDn7U3oi91E6oNiRNUSiwng",
  depositCrsc: "4gx2tSdc3eVzBB9sUZ1LwhLm9zniTwt9eB1Cv7dtrK7794ZgSwxqeMdEM9ohTFxLmnyFyANr6GXAgfXu5uz7G794",
  requestCrsc: "2uXjwNsmUhNW6pjZfaYtwsG7zD9giuiXYGsEhfbgqavWKtpaYnETtF3jtnHKb9os6fUXMXf3hPdcVLdyVhQmPdVF",
} as const;

const FXUC: RegistryInfo = {
  registry: "FXUCNnxbkZBed2cax5waBWCaGiHg7BRzxEjrcZie7iDC",
  programId: WRAPPER,
  market: "7mgX3bkzEivRrCffCJ7XzqfAp3gpjm63RNinwDhr7b41",
  lpMint: "Fgn5f3Sg7xPbGZeXVMZ4GBwQbQBChFovoGU5eHGpnPoU",
};
const LP_USER = "9sM73A4MvS2ye2Fuvpr1tmkj68iA61eebuRKz1rnGUWa";

interface RawTx {
  slot: number;
  blockTime: number | null;
  transaction: { signatures: string[]; message: ConstructorParameters<typeof Message>[0] };
  meta: {
    err: unknown;
    innerInstructions: VersionedTransactionResponse["meta"] extends infer M
      ? M extends { innerInstructions?: infer I } ? I : never : never;
    loadedAddresses?: { writable: string[]; readonly: string[] };
  };
}

/** Rebuild the web3.js response the poller receives from a recorded RPC json payload. */
function loadRpc(sig: string): VersionedTransactionResponse {
  const raw = JSON.parse(readFileSync(join(FIXTURES, `${sig}.json`), "utf8")) as RawTx;
  return {
    slot: raw.slot,
    blockTime: raw.blockTime,
    transaction: { signatures: raw.transaction.signatures, message: new Message(raw.transaction.message) },
    meta: {
      err: raw.meta.err,
      innerInstructions: raw.meta.innerInstructions,
      loadedAddresses: {
        writable: (raw.meta.loadedAddresses?.writable ?? []).map((k) => new PublicKey(k)),
        readonly: (raw.meta.loadedAddresses?.readonly ?? []).map((k) => new PublicKey(k)),
      },
    },
  } as unknown as VersionedTransactionResponse;
}
const loadTx = (sig: string): NormTx => normalizeRpcTransaction(sig, loadRpc(sig));

// ── synthetic builders ──────────────────────────────────────────────────────

const pk = (): string => Keypair.generate().publicKey.toBase58();
const u64le = (v: bigint): Uint8Array => {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, v, true);
  return b;
};
const spl = (tag: number, accounts: string[], amount: bigint, stackHeight: number | null): NormIx => ({
  programId: TOKEN_PROGRAM_ID,
  accounts,
  data: Uint8Array.from([tag, ...u64le(amount)]),
  stackHeight,
});

function executeAccounts(o: { cranker: string; user: string; escrow: string; vaultToken: string; dest: string }): string[] {
  // 0 cranker 1 market 2 registry 3 redemption 4 lp_mint 5 escrow 6 vault_token
  // 7 vault_authority 8 ledger 9 redeemer_dest 10 token_program 11 sibling 12 redeemer_rent_dest
  return [o.cranker, FXUC.market, FXUC.registry, pk(), FXUC.lpMint, o.escrow, o.vaultToken, pk(), pk(),
    o.dest, TOKEN_PROGRAM_ID, pk(), o.user];
}

function syntheticExecute(o: { user: string; burned: bigint; out: bigint; slot: number; sig?: string }): NormTx {
  const escrow = pk(), vaultToken = pk(), dest = pk(), cranker = pk();
  return {
    signature: o.sig ?? `exec-${o.slot}`,
    slot: o.slot,
    blockTime: null,
    failed: false,
    instructions: [{
      programId: WRAPPER,
      accounts: executeAccounts({ cranker, user: o.user, escrow, vaultToken, dest }),
      data: encodeExecuteRedemption({ domain: 0 }),
      stackHeight: 1,
      inner: [
        spl(3, [vaultToken, dest, pk()], o.out, 2),
        spl(8, [escrow, FXUC.lpMint, FXUC.registry], o.burned, 2),
      ],
    }],
  };
}

function syntheticCancel(o: { user: string; shares: bigint; slot: number }): NormTx {
  const ata = pk(), escrow = pk();
  return {
    signature: `cancel-${o.slot}`,
    slot: o.slot,
    blockTime: null,
    failed: false,
    instructions: [{
      programId: WRAPPER,
      // 0 redeemer 1 registry 2 redemption 3 lp_mint 4 redeemer_lp_ata 5 escrow 6 token_program
      accounts: [o.user, FXUC.registry, pk(), FXUC.lpMint, ata, escrow, TOKEN_PROGRAM_ID],
      data: Uint8Array.from([81]),
      stackHeight: 1,
      inner: [spl(3, [escrow, ata, FXUC.registry], o.shares, 2)],
    }],
  };
}

const ev = (kind: FoldEvent["kind"], slot: number, collateral: bigint, lp: bigint | null): FoldEvent => ({
  kind, slot, signature: `s${slot}`, ixIndex: 0, collateralAtoms: collateral, lpAmount: lp,
});

// ── decoder: real devnet transactions ───────────────────────────────────────

describe("GH#207 decoder — real devnet LP-vault transactions", () => {
  it("genesis deposit: collateral from ix data, shares from the MintTo CPI (1000 dead shares withheld)", () => {
    const events = decodeLpVaultEvents(loadTx(SIG.depositFxuc), WRAPPERS);
    expect(events).toHaveLength(1);
    const e = events[0]!;
    expect(e).toMatchObject({
      kind: "deposit",
      signature: SIG.depositFxuc,
      ixIndex: 4,
      slot: 505300414,
      user: LP_USER,
      market: FXUC.market,
      registry: FXUC.registry,
      lpMint: FXUC.lpMint,
      programId: WRAPPER,
      domain: 0,
    });
    expect(e.collateralAtoms).toBe(5_000_000_000n);
    // Registry total_lp_shares_outstanding is 5_000_000_000 on-chain; the depositor
    // received 1000 fewer (LP_VAULT_MINIMUM_LIQUIDITY). The recorded
    // postTokenBalance of the depositor's LP ATA is exactly this.
    expect(e.lpAmount).toBe(4_999_999_000n);
  });

  it("second vault's deposit decodes the same way", () => {
    const [e] = decodeLpVaultEvents(loadTx(SIG.depositCrsc), WRAPPERS);
    expect(e?.kind).toBe("deposit");
    expect(e?.collateralAtoms).toBe(250_000_000n);
    expect(e?.lpAmount).toBe(249_999_000n);
    expect(e?.registry).toBe("CrscF8tv9PYoa4nKrWSZqVTNajVw17ZJiq6ZgFvY3zi");
    expect(e?.market).toBe("5bVTTMRceF9qEERjPWvqxtrDighE846QkVXSJm4uC8Tk");
  });

  it("RequestRedeemLpShares records escrowed shares, no collateral, no market account", () => {
    for (const sig of [SIG.requestFxuc, SIG.requestCrsc]) {
      const events = decodeLpVaultEvents(loadTx(sig), WRAPPERS);
      expect(events).toHaveLength(1);
      const e = events[0]!;
      expect(e.kind).toBe("request_redeem");
      expect(e.user).toBe(LP_USER);
      expect(e.market).toBeNull();
      expect(e.collateralAtoms).toBe(0n);
      expect(e.lpAmount).toBeGreaterThan(0n);
    }
  });

  it("CreateLpVault (74) and LpVaultCrankFees (78) produce no position events", () => {
    for (const sig of [SIG.createFxuc, SIG.createCrsc, SIG.crankFxuc]) {
      expect(decodeLpVaultEvents(loadTx(sig), WRAPPERS)).toEqual([]);
    }
  });

  it("the SDK encoder reproduces the recorded deposit bytes (wire layout pinned both ways)", () => {
    const top = loadTx(SIG.depositFxuc).instructions[4]!;
    expect(Buffer.from(top.data)).toEqual(Buffer.from(encodeDepositToLpVault({ amount: 5_000_000_000n, domain: 0 })));
  });

  it("an unknown program id is ignored", () => {
    expect(decodeLpVaultEvents(loadTx(SIG.depositFxuc), new Set([pk()]))).toEqual([]);
  });

  it("a failed transaction yields nothing", () => {
    expect(decodeLpVaultEvents({ ...loadTx(SIG.depositFxuc), failed: true }, WRAPPERS)).toEqual([]);
  });
});

// ── decoder: synthetic execute / cancel / CPI ───────────────────────────────

describe("GH#207 decoder — ExecuteRedemption, CancelRedemption, CPI", () => {
  it("execute: LP is account 12 (the pinned redeemer), NOT the permissionless cranker", () => {
    const [e] = decodeLpVaultEvents(syntheticExecute({ user: LP_USER, burned: 700n, out: 812n, slot: 1 }), WRAPPERS);
    expect(e?.kind).toBe("execute_redeem");
    expect(e?.user).toBe(LP_USER);
    expect(e?.collateralAtoms).toBe(812n);
    expect(e?.lpAmount).toBe(700n);
    expect(e?.market).toBe(FXUC.market);
    expect(e?.domain).toBe(0);
  });

  it("execute: a transfer that is not vault -> redeemer_dest is not taken as the payout", () => {
    const tx = syntheticExecute({ user: LP_USER, burned: 700n, out: 812n, slot: 1 });
    const top = tx.instructions[0]!;
    top.inner.unshift(spl(3, [pk(), pk(), pk()], 999_999n, 2));
    expect(decodeLpVaultEvents(tx, WRAPPERS)[0]?.collateralAtoms).toBe(812n);
  });

  it("execute with too few accounts (pre-GH#412 caller) is not decoded", () => {
    const tx = syntheticExecute({ user: LP_USER, burned: 1n, out: 1n, slot: 1 });
    tx.instructions[0]!.accounts = tx.instructions[0]!.accounts.slice(0, 12);
    expect(decodeLpVaultEvents(tx, WRAPPERS)).toEqual([]);
  });

  it("cancel returns escrowed shares", () => {
    const [e] = decodeLpVaultEvents(syntheticCancel({ user: LP_USER, shares: 55n, slot: 2 }), WRAPPERS);
    expect(e).toMatchObject({ kind: "cancel_redeem", user: LP_USER, registry: FXUC.registry });
    expect(e?.lpAmount).toBe(55n);
  });

  it("request bytes from the SDK encoder decode to the same share count", () => {
    const real = loadTx(SIG.requestFxuc);
    const top = real.instructions[3]!;
    const [before] = decodeLpVaultEvents(real, WRAPPERS);
    top.data = encodeRequestRedeemLpShares({ shares: 1_234n });
    const [after] = decodeLpVaultEvents(real, WRAPPERS);
    expect(before?.lpAmount).not.toBe(1_234n);
    expect(after?.lpAmount).toBe(1_234n);
  });

  it("a deposit reached by CPI decodes with its OWN children (stack heights), indexed 1000*(i+1)+j", () => {
    const real = loadTx(SIG.depositFxuc).instructions[4]!;
    // Wrap the real deposit as a CPI from some router program at top-level index 2.
    const router: NormTx = {
      signature: "router", slot: 9, blockTime: null, failed: false,
      instructions: [
        { programId: pk(), accounts: [], data: new Uint8Array([1]), stackHeight: 1, inner: [] },
        { programId: pk(), accounts: [], data: new Uint8Array([1]), stackHeight: 1, inner: [] },
        {
          programId: pk(), accounts: [], data: new Uint8Array([9]), stackHeight: 1,
          inner: [
            // A sibling MintTo on the same mint+ATA made by the ROUTER (height 2),
            // before the wrapper runs: must not be attributed to the deposit.
            spl(7, [real.accounts[3]!, real.accounts[4]!, pk()], 42n, 2),
            { ...real, stackHeight: 2 },
            ...real.inner.map((ix) => ({ ...ix, stackHeight: (ix.stackHeight ?? 2) + 1 })),
          ],
        },
      ],
    };
    const events = decodeLpVaultEvents(router, WRAPPERS);
    expect(events).toHaveLength(1);
    expect(events[0]?.ixIndex).toBe(3001);
    expect(events[0]?.lpAmount).toBe(4_999_999_000n);
  });

  it("a deposit whose Transfer CPI disagrees with the ix amount keeps the event but no share count", () => {
    const tx = loadTx(SIG.depositFxuc);
    const top = tx.instructions[4]!;
    const t = top.inner.find((ix) => ix.programId === TOKEN_PROGRAM_ID && ix.data[0] === 3)!;
    t.data = Uint8Array.from([3, ...u64le(1n)]);
    const [e] = decodeLpVaultEvents(tx, WRAPPERS);
    expect(e?.collateralAtoms).toBe(5_000_000_000n);
    expect(e?.lpAmount).toBeNull();
  });
});

// ── average-cost fold ───────────────────────────────────────────────────────

describe("GH#207 average-cost fold", () => {
  it("late depositor pays NAV: earned is measured from THEIR basis, not from par", () => {
    // The #2675 counter-example: after fees, NAV is 1.1 and a newcomer deposits
    // 1100 for 1000 shares. Their earned must be 0, not +100.
    const p = foldPosition([ev("deposit", 1, 1100n, 1000n)]);
    expect(p.costBasisAtoms).toBe(1100n);
    expect(p.lpShares).toBe(1000n);
    const valueAtNav = (p.lpShares * 11n) / 10n; // 1100 at NAV 1.1
    expect(valueAtNav - p.costBasisAtoms).toBe(0n);
  });

  it("two deposits at different NAVs average; partial redemption realizes pro rata", () => {
    const p = foldPosition([
      ev("deposit", 1, 1000n, 1000n),        // NAV 1.0
      ev("deposit", 2, 1100n, 1000n),        // NAV 1.1
      ev("request_redeem", 3, 0n, 1000n),
      ev("execute_redeem", 4, 1150n, 1000n), // NAV 1.15
    ]);
    // basis 2100 over 2000 shares; 1000 redeemed removes 1050; realized 1150 - 1050.
    expect(p.realizedPnlAtoms).toBe(100n);
    expect(p.costBasisAtoms).toBe(1050n);
    expect(p.lpShares).toBe(1000n);
    expect(p.pendingRedeemShares).toBe(0n);
    expect(p.eventsConsistent).toBe(true);
  });

  it("request keeps shares and basis (escrow, not burn); cancel clears pending", () => {
    const p = foldPosition([ev("deposit", 1, 500n, 500n), ev("request_redeem", 2, 0n, 200n)]);
    expect(p.lpShares).toBe(500n);
    expect(p.pendingRedeemShares).toBe(200n);
    expect(p.costBasisAtoms).toBe(500n);
    const q = foldPosition([ev("deposit", 1, 500n, 500n), ev("request_redeem", 2, 0n, 200n), ev("cancel_redeem", 3, 0n, 200n)]);
    expect(q.pendingRedeemShares).toBe(0n);
  });

  it("full redemption removes ALL basis (no rounding residue)", () => {
    const p = foldPosition([ev("deposit", 1, 1000n, 3n), ev("deposit", 2, 1001n, 3n), ev("execute_redeem", 3, 1990n, 6n)]);
    expect(p.costBasisAtoms).toBe(0n);
    expect(p.lpShares).toBe(0n);
    expect(p.realizedPnlAtoms).toBe(1990n - 2001n);
  });

  it("order-independent input: the fold sorts by (slot, signature, ix_index)", () => {
    const a = [ev("deposit", 1, 1000n, 1000n), ev("deposit", 2, 1100n, 1000n), ev("execute_redeem", 4, 1150n, 1000n)];
    expect(foldPosition([...a].reverse())).toEqual(foldPosition(a));
  });

  it("burning more shares than were indexed (transfer-in) is inconsistent", () => {
    const p = foldPosition([ev("deposit", 1, 100n, 100n), ev("execute_redeem", 2, 300n, 250n)]);
    expect(p.eventsConsistent).toBe(false);
    expect(basisKnown(p, null)).toBe(false);
  });

  it("an unreadable share amount makes the basis unknown", () => {
    expect(foldPosition([ev("deposit", 1, 100n, null)]).eventsConsistent).toBe(false);
  });

  it("a fresh start (deposit into an empty position) clears earlier inconsistency and older transfer detection", () => {
    const hist = [ev("deposit", 1, 100n, 100n), ev("execute_redeem", 2, 300n, 250n), ev("deposit", 10, 50n, 50n)];
    const p = foldPosition(hist);
    expect(p.eventsConsistent).toBe(true);
    expect(p.freshStartSlot).toBe(10);
    expect(basisKnown(p, 5)).toBe(true);   // detected before the fresh start
    expect(basisKnown(p, 11)).toBe(false); // detected after it: sticky
  });
});

// ── store + indexer with a fake chain over the real fixtures ────────────────

class FakeChain implements LpVaultChain {
  sigs: SigInfo[] = [];            // newest first
  txs = new Map<string, NormTx>();
  claim: OnchainClaim = { heldShares: 0n, pendingShares: 0n, contextSlot: 0 };
  /** Signature to insert as newest AFTER the next getClaim — simulates a racing vault tx. */
  raceSig: SigInfo | null = null;

  add(tx: NormTx): void {
    this.txs.set(tx.signature, tx);
    this.sigs.unshift({ signature: tx.signature, slot: tx.slot, failed: tx.failed });
  }
  async listRegistries(): Promise<RegistryInfo[]> { return [FXUC]; }
  async getSignatures(_a: string, o: { until?: string; before?: string; limit: number }): Promise<SigInfo[]> {
    let list = this.sigs;
    if (o.before) list = list.slice(list.findIndex((s) => s.signature === o.before) + 1);
    if (o.until) {
      const i = list.findIndex((s) => s.signature === o.until);
      if (i >= 0) list = list.slice(0, i);
    }
    return list.slice(0, o.limit);
  }
  async getTransaction(sig: string): Promise<NormTx | null> { return this.txs.get(sig) ?? null; }
  async getClaim(): Promise<OnchainClaim> {
    if (this.raceSig) { this.sigs.unshift(this.raceSig); this.raceSig = null; }
    return this.claim;
  }
}

function fxucChain(): FakeChain {
  const c = new FakeChain();
  for (const s of [SIG.createFxuc, SIG.depositFxuc, SIG.requestFxuc, SIG.crankFxuc]) c.add(loadTx(s));
  return c;
}
const requestedShares = (): bigint => decodeLpVaultEvents(loadTx(SIG.requestFxuc), WRAPPERS)[0]!.lpAmount!;

describe("GH#207 indexer over real FXUC vault history", () => {
  it("backfills the vault and produces an exact, reconciled position", async () => {
    const chain = fxucChain();
    // On-chain: held = minted - escrowed, pending = escrowed -> claim = minted.
    chain.claim = { heldShares: 4_999_999_000n - requestedShares(), pendingShares: requestedShares(), contextSlot: 505_500_000 };
    const store = new MemoryLpVaultStore();
    const ix = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    const r = await ix.syncRegistry(FXUC);
    expect(r.events).toBe(2);
    expect(r.users).toEqual([LP_USER]);
    expect(r.reconciled).toBe(1);

    const pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos).toMatchObject({
      market_slab: FXUC.market,
      lp_mint: FXUC.lpMint,
      lp_shares: "4999999000",
      pending_redeem_shares: requestedShares().toString(),
      cost_basis_atoms: "5000000000",
      realized_pnl_atoms: "0",
      basis_known: true,
      transfer_detected_slot: null,
      onchain_lp_shares: "4999999000",
      event_count: 2,
    });
  });

  it("re-syncing is idempotent (no double-counted deposit)", async () => {
    const chain = fxucChain();
    chain.claim = { heldShares: 4_999_999_000n - requestedShares(), pendingShares: requestedShares(), contextSlot: 1 };
    const store = new MemoryLpVaultStore();
    const a = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    await a.syncRegistry(FXUC);
    // A restarted process has no cursor and re-reads all history.
    const b = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    await b.syncRegistry(FXUC);
    const pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos?.cost_basis_atoms).toBe("5000000000");
    expect(pos?.event_count).toBe(2);
  });

  it("an on-chain claim different from the indexed shares (LP transferred) marks basis unknown, stickily", async () => {
    const chain = fxucChain();
    chain.claim = { heldShares: 1_000n, pendingShares: requestedShares(), contextSlot: 505_500_000 };
    const store = new MemoryLpVaultStore();
    const ix = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    await ix.syncRegistry(FXUC);
    let pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos?.basis_known).toBe(false);
    expect(pos?.transfer_detected_slot).toBe(505_500_000);

    // The shares come back; the basis of what moved is still unknowable.
    chain.claim = { heldShares: 4_999_999_000n - requestedShares(), pendingShares: requestedShares(), contextSlot: 505_600_000 };
    await ix.reconcileOpenPositions(FXUC);
    pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos?.basis_known).toBe(false);
  });

  it("a vault tx racing the balance read skips the comparison instead of flagging a false transfer", async () => {
    const chain = fxucChain();
    // The balance read already includes a redemption the sync has not seen.
    chain.claim = { heldShares: 0n, pendingShares: 0n, contextSlot: 505_700_000 };
    chain.raceSig = { signature: "racing-execute", slot: 505_699_999, failed: false };
    const store = new MemoryLpVaultStore();
    const ix = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    const r = await ix.syncRegistry(FXUC);
    expect(r.reconcileSkipped).toBe(1);
    const pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos?.basis_known).toBe(true);
    expect(pos?.transfer_detected_slot).toBeNull();
  });

  it("execute after the real deposit/request realizes PnL against the real basis", async () => {
    const chain = fxucChain();
    chain.claim = { heldShares: 4_999_999_000n - requestedShares(), pendingShares: 0n, contextSlot: 505_900_000 };
    chain.add(syntheticExecute({ user: LP_USER, burned: requestedShares(), out: requestedShares() + 7n, slot: 505_800_000, sig: "exec-1" }));
    const store = new MemoryLpVaultStore();
    const ix = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [WRAPPER] });
    await ix.syncRegistry(FXUC);
    const pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    const removed = (5_000_000_000n * requestedShares()) / 4_999_999_000n;
    expect(pos?.lp_shares).toBe((4_999_999_000n - requestedShares()).toString());
    expect(pos?.pending_redeem_shares).toBe("0");
    expect(pos?.realized_pnl_atoms).toBe((requestedShares() + 7n - removed).toString());
    expect(pos?.cost_basis_atoms).toBe((5_000_000_000n - removed).toString());
    expect(pos?.basis_known).toBe(true);
  });

  it("recomputePosition without events for a user writes an empty, known position", async () => {
    const store = new MemoryLpVaultStore();
    const row = await recomputePosition(store, "devnet", FXUC.registry, pk());
    expect(row.lp_shares).toBe("0");
    expect(row.basis_known).toBe(true);
  });
});

describe("GH#207 redemption account parsing", () => {
  const build = (magicZero: boolean, registry: string, user: string, shares: bigint): Uint8Array => {
    const d = new Uint8Array(112);
    if (!magicZero) d.set([1, 2, 3, 4, 5, 6, 7, 8], 0);
    d[10] = 6;
    d.set(new PublicKey(registry).toBytes(), 16);
    d.set(new PublicKey(user).toBytes(), 48);
    d.set(u64le(shares), 80);
    return d;
  };
  it("reads live request shares; consumed (zero magic) and foreign requests count 0", () => {
    expect(parseRedemptionShares(build(false, FXUC.registry, LP_USER, 77n), FXUC.registry, LP_USER)).toBe(77n);
    expect(parseRedemptionShares(build(true, FXUC.registry, LP_USER, 77n), FXUC.registry, LP_USER)).toBe(0n);
    expect(parseRedemptionShares(build(false, FXUC.registry, pk(), 77n), FXUC.registry, LP_USER)).toBe(0n);
    expect(parseRedemptionShares(null, FXUC.registry, LP_USER)).toBe(0n);
  });
});

// ── migration ───────────────────────────────────────────────────────────────

describe("GH#207 migration", () => {
  const dir = join(__dirname, "..", "supabase", "migrations");
  const file = readdirSync(dir).find((f) => f.includes("lp_vault_cost_basis"));
  const ddl = (file ? readFileSync(join(dir, file), "utf8") : "")
    .split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n");

  it("exists", () => expect(file).toBeDefined());
  it("keys events by (signature, ix_index) and positions by (network, registry, user_wallet)", () => {
    expect(ddl).toMatch(/CREATE TABLE IF NOT EXISTS lp_vault_events[\s\S]*?PRIMARY KEY \(signature, ix_index\)/);
    expect(ddl).toMatch(/CREATE TABLE IF NOT EXISTS lp_vault_positions[\s\S]*?PRIMARY KEY \(network, registry, user_wallet\)/);
  });
  it("stores u128 amounts as numeric, not bigint", () => {
    for (const col of ["collateral_atoms", "lp_amount", "lp_shares", "cost_basis_atoms", "realized_pnl_atoms"]) {
      expect(ddl).toMatch(new RegExp(`\\b${col}\\s+numeric\\(40,0\\)`));
    }
  });
  it("is service-role only (RLS on, anon/authenticated revoked, no public grant)", () => {
    expect(ddl).toMatch(/ALTER TABLE lp_vault_events\s+ENABLE ROW LEVEL SECURITY/);
    expect(ddl).toMatch(/ALTER TABLE lp_vault_positions ENABLE ROW LEVEL SECURITY/);
    expect(ddl).toMatch(/REVOKE ALL ON lp_vault_positions FROM anon, authenticated/);
    expect(ddl).not.toMatch(/GRANT SELECT[^;]*lp_vault_[^;]*TO[^;]*anon/);
  });
});

// X-1 (d): a transaction the reader cannot return must never be skipped silently by the LP-vault cursor.
import { existsSync, readFileSync as readFile, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { getSkippedSignatureCount, resetSkippedSignatureCount, resetSkippedSignatureDedupe } from "../src/lib/skippedSignatures.js";

describe("X-1 LP vault: unreadable transactions", () => {
  const FILE = join(tmpdir(), `skipped-${process.pid}.jsonl`);
  const V2 = Object.assign(new Error("failed to get transaction: Transaction version (2) is not supported by the requesting client"), { code: -32015 });
  const withExtras = (n: number, bad: string[]): FakeChain => {
    const c = fxucChain();
    for (let i = 0; i < n; i++) c.add({ signature: `extra${i}`, slot: 505_400_000 + i, blockTime: null, failed: false, instructions: [] });
    const real = c.getTransaction.bind(c);
    c.getTransaction = async (sig: string) => { if (bad.includes(sig)) throw V2; return real(sig); };
    return c;
  };
  const claim = () => ({ heldShares: 4_999_999_000n - requestedShares(), pendingShares: requestedShares(), contextSlot: 505_500_000 });
  const mkIndexer = (chain: FakeChain) => { chain.claim = claim(); return new LpVaultIndexer({ chain, store: new MemoryLpVaultStore(), network: "devnet", programIds: [WRAPPER] }); };
  beforeEach(() => { process.env.SKIPPED_SIGNATURES_FILE = FILE; rmSync(FILE, { force: true }); resetSkippedSignatureCount(); resetSkippedSignatureDedupe(); });
  afterEach(() => { delete process.env.SKIPPED_SIGNATURES_FILE; rmSync(FILE, { force: true }); });

  it("one unreadable tx in a large enough window: recorded durably (FULL signature + registry), counter bumped, positions reconciled", async () => {
    const ix = mkIndexer(withExtras(8, ["extra3"]));
    const r = await ix.syncRegistry(FXUC);
    expect(r.events).toBe(2); // the real history still folds
    expect(getSkippedSignatureCount()).toBe(1);
    const rec = readFile(FILE, "utf8").trim().split("\n").map((l) => JSON.parse(l));
    expect(rec).toHaveLength(1);
    expect(rec[0]).toMatchObject({ signature: "extra3", slab: FXUC.registry, source: "lp-vault" });
    expect(r.reconciled).toBeGreaterThanOrEqual(2); // touched-user reconcile + the explicit reconcile of open positions
  });
  it("negative control: with nothing unreadable nothing is recorded and no extra reconcile happens", async () => {
    const r = await mkIndexer(withExtras(8, [])).syncRegistry(FXUC);
    expect(getSkippedSignatureCount()).toBe(0);
    expect(existsSync(FILE)).toBe(false);
    expect(r.reconciled).toBe(1);
  });
  it("2 poison + good siblings: both skipped and recorded, the cursor advances (successful-sibling rule)", async () => {
    const r = await mkIndexer(withExtras(8, ["extra1", "extra2"])).syncRegistry(FXUC);
    expect(r.events).toBe(2);
    expect(getSkippedSignatureCount()).toBe(2);
  });
  it("breaker: an ALL-unreadable window holds the vault cursor and records nothing; once the reader works the whole window is re-read", async () => {
    const chain = fxucChain();
    const real = chain.getTransaction.bind(chain);
    chain.getTransaction = async () => { throw V2; };
    const ix = mkIndexer(chain);
    await expect(ix.syncRegistry(FXUC)).rejects.toThrow(/mass skip refused/);
    expect(getSkippedSignatureCount()).toBe(0);
    chain.getTransaction = real;
    expect((await ix.syncRegistry(FXUC)).signatures).toBe(4);
  });
  it("breaker: more than 5 unreadable holds even with good siblings", async () => {
    const bad = ["extra0", "extra1", "extra2", "extra3", "extra4", "extra5"];
    await expect(mkIndexer(withExtras(8, bad)).syncRegistry(FXUC)).rejects.toThrow(/mass skip refused/);
    expect(getSkippedSignatureCount()).toBe(0);
  });
  it("C: the cursor moves only AFTER the skipped signatures are recorded and the reconcile ran (a failing reconcile leaves it unset)", async () => {
    const chain = withExtras(8, ["extra3"]);
    const ix = mkIndexer(chain);
    const orig = chain.getClaim.bind(chain);
    chain.getClaim = async () => { throw new Error("rpc down during reconcile"); };
    await expect(ix.syncRegistry(FXUC)).rejects.toThrow(/rpc down/);
    expect((ix as unknown as { cursors: Map<string, string> }).cursors.has(FXUC.registry)).toBe(false);
    chain.getClaim = orig;
    await ix.syncRegistry(FXUC); // retried from the same cursor, now completes
    expect((ix as unknown as { cursors: Map<string, string> }).cursors.has(FXUC.registry)).toBe(true);
  });
  it("E: a transient-looking error (-32015 with 429 text) is NOT skipped: the sync aborts", async () => {
    const chain = withExtras(8, []);
    const real = chain.getTransaction.bind(chain);
    chain.getTransaction = async (sig: string) => { if (sig === "extra3") throw Object.assign(new Error("429 Too Many Requests: Transaction version (2) is not supported"), { code: -32015 }); return real(sig); };
    await expect(mkIndexer(chain).syncRegistry(FXUC)).rejects.toThrow(/429/);
    expect(getSkippedSignatureCount()).toBe(0);
  });
});
const fxucSigOf = (i: number): string => [SIG.createFxuc, SIG.depositFxuc, SIG.requestFxuc, SIG.crankFxuc][i]!;
