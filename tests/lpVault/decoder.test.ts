import { describe, it, expect } from "vitest";
import { Keypair, Message, PublicKey, type VersionedTransactionResponse } from "@solana/web3.js";
import {
  decodeLpVaultEvents,
  normalizeRpcTransaction,
  LP_VAULT_TAG,
  TOKEN_PROGRAM_ID,
  type NormIx,
  type NormTopIx,
  type NormTx,
} from "../../src/lpVault/decoder.js";

/**
 * Guard and edge branches of the LP-vault decoder: short buffers, unknown tags,
 * zero / max integer fields, the *Checked SPL variants, and CPI attribution by
 * stack height. The happy paths over real devnet transactions live in
 * tests/lp-vault-cost-basis-207.test.ts.
 */

const WRAPPER = "GnwdeQrAh4qzChJeVLrM21CXXWC1akjLH3DiijwzEEYZ";
const WRAPPERS = new Set([WRAPPER]);
const U64_MAX = (1n << 64n) - 1n;
const U128_MAX = (1n << 128n) - 1n;

const pk = (): string => Keypair.generate().publicKey.toBase58();

function le(v: bigint, bytes: number): number[] {
  const out: number[] = [];
  for (let i = 0; i < bytes; i++) out.push(Number((v >> BigInt(8 * i)) & 0xffn));
  return out;
}

const splIx = (tag: number, accounts: string[], amount: bigint, stackHeight: number | null, extra: number[] = []): NormIx => ({
  programId: TOKEN_PROGRAM_ID,
  accounts,
  data: Uint8Array.from([tag, ...le(amount, 8), ...extra]),
  stackHeight,
});

const wrapperIx = (data: number[], accounts: string[], stackHeight: number | null): NormIx => ({
  programId: WRAPPER,
  accounts,
  data: Uint8Array.from(data),
  stackHeight,
});

function tx(instructions: NormTopIx[]): NormTx {
  return { signature: "sig", slot: 10, blockTime: 1_700_000_000, failed: false, instructions };
}
const top = (ix: NormIx, inner: NormIx[] = []): NormTopIx => ({ ...ix, stackHeight: 1, inner });
const decode = (t: NormTx) => decodeLpVaultEvents(t, WRAPPERS);

// ── deposit fixture ─────────────────────────────────────────────────────────

interface DepositAccts { user: string; market: string; registry: string; lpMint: string; lpAta: string; source: string; vaultToken: string }
function depositAccts(): DepositAccts {
  return { user: pk(), market: pk(), registry: pk(), lpMint: pk(), lpAta: pk(), source: pk(), vaultToken: pk() };
}
const depositAccounts = (a: DepositAccts): string[] =>
  [a.user, a.market, a.registry, a.lpMint, a.lpAta, a.source, a.vaultToken, pk(), TOKEN_PROGRAM_ID, pk(), pk()];
const depositData = (amount: bigint, domain: number): number[] =>
  [LP_VAULT_TAG.DepositToLpVault, ...le(amount, 16), ...le(BigInt(domain), 2)];

describe("decodeOne guards: length, account count, tag", () => {
  it("an empty-data wrapper instruction is ignored", () => {
    expect(decode(tx([top(wrapperIx([], [pk()], 1))]))).toEqual([]);
  });

  it.each([0, 1, 74, 78, 79, 80, 82, 255])("tag %i is not a position event", (tag) => {
    const data = [tag, ...le(5n, 16), 0, 0];
    expect(decode(tx([top(wrapperIx(data, depositAccounts(depositAccts()), 1))]))).toEqual([]);
  });

  it("deposit: 18-byte data (domain truncated) is not decoded; 19 bytes is", () => {
    const a = depositAccts();
    const full = depositData(7n, 3);
    expect(decode(tx([top(wrapperIx(full.slice(0, 18), depositAccounts(a), 1))]))).toEqual([]);
    expect(decode(tx([top(wrapperIx(full, depositAccounts(a), 1))]))).toHaveLength(1);
  });

  it("deposit: 10 accounts (no sibling_ledger) is not decoded", () => {
    const a = depositAccts();
    expect(decode(tx([top(wrapperIx(depositData(7n, 3), depositAccounts(a).slice(0, 10), 1))]))).toEqual([]);
  });

  it("request: 16-byte data is not decoded, and neither is a 7-account request", () => {
    const accts = Array.from({ length: 8 }, pk);
    const data = [LP_VAULT_TAG.RequestRedeemLpShares, ...le(9n, 16)];
    expect(decode(tx([top(wrapperIx(data.slice(0, 16), accts, 1))]))).toEqual([]);
    expect(decode(tx([top(wrapperIx(data, accts.slice(0, 7), 1))]))).toEqual([]);
    expect(decode(tx([top(wrapperIx(data, accts, 1))]))[0]!.lpAmount).toBe(9n);
  });

  it("execute: 2-byte data (domain truncated) is not decoded", () => {
    const accts = Array.from({ length: 13 }, pk);
    expect(decode(tx([top(wrapperIx([LP_VAULT_TAG.ExecuteRedemption, 1], accts, 1))]))).toEqual([]);
  });

  it("cancel: 6 accounts is not decoded; the bare 1-byte tag with 7 accounts is", () => {
    const accts = Array.from({ length: 7 }, pk);
    expect(decode(tx([top(wrapperIx([LP_VAULT_TAG.CancelRedemption], accts.slice(0, 6), 1))]))).toEqual([]);
    const [ev] = decode(tx([top(wrapperIx([LP_VAULT_TAG.CancelRedemption], accts, 1))]));
    expect(ev).toMatchObject({ kind: "cancel_redeem", user: accts[0], registry: accts[1], lpMint: accts[3], lpAmount: null });
  });
});

describe("integer fields: zero, max and byte order", () => {
  it("deposit amount u128::MAX and domain u16::MAX decode exactly", () => {
    const a = depositAccts();
    const mint = splIx(7, [a.lpMint, a.lpAta, pk()], U64_MAX, 2);
    const [ev] = decode(tx([top(wrapperIx(depositData(U128_MAX, 0xffff), depositAccounts(a), 1), [mint])]));
    expect(ev).toMatchObject({ collateralAtoms: U128_MAX, lpAmount: U64_MAX, domain: 0xffff });
  });

  it("deposit amount 0 and a 0-share MintTo give 0n, not null", () => {
    const a = depositAccts();
    const mint = splIx(7, [a.lpMint, a.lpAta, pk()], 0n, 2);
    const xfer = splIx(3, [a.source, a.vaultToken, a.user], 0n, 2);
    const [ev] = decode(tx([top(wrapperIx(depositData(0n, 0), depositAccounts(a), 1), [xfer, mint])]));
    expect(ev).toMatchObject({ collateralAtoms: 0n, lpAmount: 0n, domain: 0 });
  });

  it("a deposit above u64 can never match a u64 Transfer, so its shares are not trusted", () => {
    const a = depositAccts();
    const amount = U64_MAX + 1n;
    const xfer = splIx(3, [a.source, a.vaultToken, a.user], 0n, 2); // the low 64 bits of `amount`
    const mint = splIx(7, [a.lpMint, a.lpAta, pk()], 5n, 2);
    const [ev] = decode(tx([top(wrapperIx(depositData(amount, 1), depositAccounts(a), 1), [xfer, mint])]));
    expect(ev).toMatchObject({ collateralAtoms: amount, lpAmount: null });
  });

  it("u128 is little-endian across both limbs (distinct bytes pin the order)", () => {
    const v = 0x100f0e0d0c0b0a090807060504030201n;
    const accts = Array.from({ length: 8 }, pk);
    const data = [LP_VAULT_TAG.RequestRedeemLpShares, ...Array.from({ length: 16 }, (_, i) => i + 1)];
    expect(decode(tx([top(wrapperIx(data, accts, 1))]))[0]!.lpAmount).toBe(v);
  });

  it("request shares 0 and u128::MAX", () => {
    const accts = Array.from({ length: 8 }, pk);
    for (const shares of [0n, U128_MAX]) {
      const [ev] = decode(tx([top(wrapperIx([LP_VAULT_TAG.RequestRedeemLpShares, ...le(shares, 16)], accts, 1))]));
      expect(ev).toMatchObject({ kind: "request_redeem", lpAmount: shares, collateralAtoms: 0n, market: null, domain: null });
    }
  });

  it("execute: Burn u64::MAX with no payout Transfer reads collateral 0n and the domain", () => {
    const accts = Array.from({ length: 13 }, pk);
    const burn = splIx(8, [accts[5]!, accts[4]!, pk()], U64_MAX, 2);
    const [ev] = decode(tx([top(wrapperIx([LP_VAULT_TAG.ExecuteRedemption, 0xff, 0xff], accts, 1), [burn])]));
    expect(ev).toMatchObject({ kind: "execute_redeem", user: accts[12], collateralAtoms: 0n, lpAmount: U64_MAX, domain: 0xffff });
  });
});

describe("SPL CPI matching", () => {
  it("TransferChecked (source a[0], dest a[2]) and MintToChecked are read like the plain forms", () => {
    const a = depositAccts();
    const xfer = splIx(12, [a.source, pk(), a.vaultToken, a.user], 50n, 2, [6]);
    const mint = splIx(14, [a.lpMint, a.lpAta, pk()], 40n, 2, [6]);
    const [ev] = decode(tx([top(wrapperIx(depositData(50n, 2), depositAccounts(a), 1), [xfer, mint])]));
    expect(ev).toMatchObject({ collateralAtoms: 50n, lpAmount: 40n });
  });

  it("a TransferChecked that disagrees with the ix amount marks the shares unknown", () => {
    const a = depositAccts();
    const xfer = splIx(12, [a.source, pk(), a.vaultToken, a.user], 49n, 2, [6]);
    const mint = splIx(14, [a.lpMint, a.lpAta, pk()], 40n, 2, [6]);
    const [ev] = decode(tx([top(wrapperIx(depositData(50n, 2), depositAccounts(a), 1), [xfer, mint])]));
    expect(ev!.lpAmount).toBeNull();
  });

  it("a TransferChecked with the wrong source or wrong dest is not taken as the deposit transfer", () => {
    const a = depositAccts();
    const mint = splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 2);
    for (const accounts of [[pk(), pk(), a.vaultToken, a.user], [a.source, pk(), pk(), a.user]]) {
      // Amount disagrees with the ix amount: if it were matched, lpAmount would be null.
      const xfer = splIx(12, accounts, 1n, 2, [6]);
      const [ev] = decode(tx([top(wrapperIx(depositData(50n, 2), depositAccounts(a), 1), [xfer, mint])]));
      expect(ev!.lpAmount).toBe(40n);
    }
  });

  it("short SPL data, non-token programs and unrelated token tags are skipped", () => {
    const a = depositAccts();
    const short: NormIx = { programId: TOKEN_PROGRAM_ID, accounts: [a.lpMint, a.lpAta], data: Uint8Array.from([7, 1, 2, 3]), stackHeight: 2 };
    const foreign: NormIx = { ...splIx(7, [a.lpMint, a.lpAta, pk()], 99n, 2), programId: pk() };
    const approve = splIx(4, [a.lpMint, a.lpAta, pk()], 98n, 2); // Approve, same accounts
    const mint = splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 2);
    const [ev] = decode(tx([top(wrapperIx(depositData(50n, 2), depositAccounts(a), 1), [short, foreign, approve, mint])]));
    expect(ev!.lpAmount).toBe(40n);
  });

  it("execute: BurnChecked is read, and a Burn of another account is not", () => {
    const accts = Array.from({ length: 13 }, pk);
    const [escrow, lpMint, vaultToken, dest] = [accts[5]!, accts[4]!, accts[6]!, accts[9]!];
    const wrongBurn = splIx(8, [pk(), lpMint, pk()], 1n, 2);
    const burn = splIx(15, [escrow, lpMint, pk()], 30n, 2, [6]);
    const pay = splIx(3, [vaultToken, dest, pk()], 33n, 2);
    const [ev] = decode(tx([top(wrapperIx([LP_VAULT_TAG.ExecuteRedemption, 1, 0], accts, 1), [pay, wrongBurn, burn])]));
    expect(ev).toMatchObject({ collateralAtoms: 33n, lpAmount: 30n, domain: 1 });
  });
});

describe("CPI attribution for a wrapper instruction reached by CPI", () => {
  function nested(innerBuilder: (a: DepositAccts, dep: NormIx) => NormIx[]) {
    const a = depositAccts();
    const dep = wrapperIx(depositData(50n, 2), depositAccounts(a), 2);
    const router = top({ programId: pk(), accounts: [], data: new Uint8Array(), stackHeight: 1 }, innerBuilder(a, dep));
    return decode(tx([router]));
  }

  it("the wrapper ix has no stack height: the event is kept, its shares are unknown", () => {
    const [ev] = nested((a) => [
      wrapperIx(depositData(50n, 2), depositAccounts(a), null),
      splIx(7, [a.lpMint, a.lpAta, pk()], 40n, null),
    ]);
    expect(ev).toMatchObject({ kind: "deposit", ixIndex: 1000, collateralAtoms: 50n, lpAmount: null });
  });

  it("a following entry without a stack height makes attribution a guess, so shares are unknown", () => {
    const [ev] = nested((a, dep) => [dep, splIx(7, [a.lpMint, a.lpAta, pk()], 40n, null)]);
    expect(ev!.lpAmount).toBeNull();
  });

  it("a sibling at the same height ends the children; a MintTo after it is not this deposit's", () => {
    const [ev] = nested((a, dep) => [
      dep,
      { programId: pk(), accounts: [], data: Uint8Array.from([1]), stackHeight: 2 },
      splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 3),
    ]);
    expect(ev!.lpAmount).toBeNull();
  });

  it("a grandchild (height h+2) is not a direct child", () => {
    const [ev] = nested((a, dep) => [
      dep,
      { programId: pk(), accounts: [], data: Uint8Array.from([1]), stackHeight: 3 },
      splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 4),
    ]);
    expect(ev!.lpAmount).toBeNull();
  });

  it("negative control: a direct child at h+1 after a deeper entry is still read", () => {
    const [ev] = nested((a, dep) => [
      dep,
      { programId: pk(), accounts: [], data: Uint8Array.from([1]), stackHeight: 3 },
      { programId: pk(), accounts: [], data: Uint8Array.from([1]), stackHeight: 4 },
      splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 3),
    ]);
    expect(ev!.lpAmount).toBe(40n);
  });

  it("an inner wrapper ix with a non-position tag (crank fees) yields nothing, siblings still decode", () => {
    const a = depositAccts();
    const crank = wrapperIx([LP_VAULT_TAG.LpVaultCrankFees], Array.from({ length: 13 }, pk), 2);
    const dep = wrapperIx(depositData(50n, 2), depositAccounts(a), 2);
    const router = top({ programId: pk(), accounts: [], data: new Uint8Array(), stackHeight: 1 }, [crank, dep]);
    const evs = decode(tx([router]));
    expect(evs.map((e) => [e.kind, e.ixIndex])).toEqual([["deposit", 1001]]);
  });

  it("top level: an inner entry deeper than height 2 is not a direct child", () => {
    const a = depositAccts();
    const deep = splIx(7, [a.lpMint, a.lpAta, pk()], 40n, 3);
    const [ev] = decode(tx([top(wrapperIx(depositData(50n, 2), depositAccounts(a), 1), [deep])]));
    expect(ev!.lpAmount).toBeNull();
  });
});

describe("normalizeRpcTransaction on a legacy response with sparse fields", () => {
  const k = (n: number) => new PublicKey(new Uint8Array(32).fill(n));
  const [payer, prog, token] = [k(1), k(2), k(3)];
  const message = new Message({
    header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 2 },
    accountKeys: [payer.toBase58(), prog.toBase58(), token.toBase58()],
    recentBlockhash: k(9).toBase58(),
    instructions: [{ programIdIndex: 1, accounts: [0], data: "2" }],
  });
  const resp = (meta: unknown, blockTime: number | null | undefined): VersionedTransactionResponse =>
    ({ slot: 5, blockTime, transaction: { signatures: ["s"], message }, meta }) as unknown as VersionedTransactionResponse;

  it("missing blockTime, missing stackHeight and undecodable base58 data become null / empty", () => {
    const n = normalizeRpcTransaction("s", resp({
      err: null,
      innerInstructions: [{ index: 0, instructions: [{ programIdIndex: 2, accounts: [0], data: "0OIl" }] }],
    }, undefined));
    expect(n.blockTime).toBeNull();
    expect(n.failed).toBe(false);
    const inner = n.instructions[0]!.inner[0]!;
    expect(inner.stackHeight).toBeNull();
    expect(inner.data).toEqual(new Uint8Array());
    expect(inner.programId).toBe(token.toBase58());
  });

  it("a null meta is read as not failed with no inner instructions", () => {
    const n = normalizeRpcTransaction("s", resp(null, 42));
    expect(n).toMatchObject({ blockTime: 42, failed: false });
    expect(n.instructions[0]!.inner).toEqual([]);
    expect([...n.instructions[0]!.data]).toEqual([1]);
  });
});
