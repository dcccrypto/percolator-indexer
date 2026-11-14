/**
 * src/lpVault/chain.ts against a mocked web3.js Connection (no live RPC).
 * Covers the account parsers, the registry scan, and the claim read that the
 * indexer reconciles LP-vault money against.
 */
import { describe, expect, it, vi } from "vitest";
import { Keypair, MessageV0, PublicKey, type Connection } from "@solana/web3.js";
import {
  HEADER_LEN,
  KIND_LP_REDEMPTION,
  KIND_LP_VAULT_REGISTRY,
  LP_REDEMPTION_LEN,
  LP_VAULT_REGISTRY_LEN,
  RpcLpVaultChain,
  deriveRedemptionPda,
  parseRedemptionShares,
  parseRegistry,
} from "../src/lpVault/chain.js";

const key = () => Keypair.generate().publicKey;
const PROGRAM = key().toBase58();
const REGISTRY = key().toBase58();
const USER = key().toBase58();
const MINT = key().toBase58();

function registryData(market: PublicKey, mint: PublicKey, kind = KIND_LP_VAULT_REGISTRY, len = LP_VAULT_REGISTRY_LEN) {
  const d = new Uint8Array(len);
  d[10] = kind;
  d.set(market.toBytes(), HEADER_LEN);
  d.set(mint.toBytes(), HEADER_LEN + 32);
  return d;
}

function redemptionData(registry: string, user: string, shares: bigint, opts: { magic?: number; kind?: number; len?: number } = {}) {
  const d = new Uint8Array(opts.len ?? LP_REDEMPTION_LEN);
  d.fill(opts.magic ?? 0x42, 0, 8);
  d[10] = opts.kind ?? KIND_LP_REDEMPTION;
  d.set(new PublicKey(registry).toBytes(), HEADER_LEN);
  d.set(new PublicKey(user).toBytes(), HEADER_LEN + 32);
  let v = shares;
  for (let i = 0; i < 16; i++) {
    d[HEADER_LEN + 64 + i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return d;
}

describe("parseRegistry", () => {
  it("reads market and LP mint", () => {
    const market = key();
    const mint = key();
    expect(parseRegistry(REGISTRY, PROGRAM, registryData(market, mint))).toEqual({
      registry: REGISTRY,
      programId: PROGRAM,
      market: market.toBase58(),
      lpMint: mint.toBase58(),
    });
  });

  it("rejects short data and the wrong kind", () => {
    expect(parseRegistry(REGISTRY, PROGRAM, registryData(key(), key(), KIND_LP_VAULT_REGISTRY, LP_VAULT_REGISTRY_LEN - 1))).toBeNull();
    expect(parseRegistry(REGISTRY, PROGRAM, registryData(key(), key(), KIND_LP_REDEMPTION))).toBeNull();
  });
});

describe("parseRedemptionShares", () => {
  it("decodes a little-endian u128 above 2^64", () => {
    const shares = (1n << 100n) + 12345n;
    expect(parseRedemptionShares(redemptionData(REGISTRY, USER, shares), REGISTRY, USER)).toBe(shares);
  });

  it("reads shares little-endian from byte 80 (hard-coded bytes, not the test encoder)", () => {
    const d = redemptionData(REGISTRY, USER, 0n);
    d[80] = 0x39; // 0x3039 = 12345
    d[81] = 0x30;
    expect(parseRedemptionShares(d, REGISTRY, USER)).toBe(12345n);
  });

  it("is 0 when the account is missing, short, or the wrong kind", () => {
    expect(parseRedemptionShares(null, REGISTRY, USER)).toBe(0n);
    expect(parseRedemptionShares(redemptionData(REGISTRY, USER, 5n, { len: LP_REDEMPTION_LEN - 1 }), REGISTRY, USER)).toBe(0n);
    expect(parseRedemptionShares(redemptionData(REGISTRY, USER, 5n, { kind: KIND_LP_VAULT_REGISTRY }), REGISTRY, USER)).toBe(0n);
  });

  it("is 0 once ExecuteRedemption has zeroed the magic", () => {
    expect(parseRedemptionShares(redemptionData(REGISTRY, USER, 5n, { magic: 0 }), REGISTRY, USER)).toBe(0n);
  });

  it("is 0 when the request belongs to another registry or user", () => {
    const d = redemptionData(REGISTRY, USER, 5n);
    expect(parseRedemptionShares(d, key().toBase58(), USER)).toBe(0n);
    expect(parseRedemptionShares(d, REGISTRY, key().toBase58())).toBe(0n);
  });
});

describe("deriveRedemptionPda", () => {
  it("is deterministic and depends on registry and user", () => {
    const a = deriveRedemptionPda(PROGRAM, REGISTRY, USER);
    expect(a.equals(deriveRedemptionPda(PROGRAM, REGISTRY, USER))).toBe(true);
    expect(a.equals(deriveRedemptionPda(PROGRAM, REGISTRY, key().toBase58()))).toBe(false);
    expect(a.equals(deriveRedemptionPda(PROGRAM, key().toBase58(), USER))).toBe(false);
  });
});

const asConn = (c: object) => c as unknown as Connection;

describe("RpcLpVaultChain.listRegistries", () => {
  it("scans each program with the size + kind filters and skips undecodable accounts", async () => {
    const programB = key().toBase58();
    const market = key();
    const mint = key();
    const good = key();
    const bad = key();
    const getProgramAccounts = vi.fn(async (pid: PublicKey) =>
      pid.toBase58() === PROGRAM
        ? [
            { pubkey: good, account: { data: registryData(market, mint) } },
            // Passed the RPC filter but is not a registry: dropped.
            { pubkey: bad, account: { data: registryData(market, mint, KIND_LP_REDEMPTION) } },
          ]
        : [],
    );
    const chain = new RpcLpVaultChain(asConn({ getProgramAccounts }));

    const out = await chain.listRegistries([PROGRAM, programB]);

    expect(out).toEqual([{ registry: good.toBase58(), programId: PROGRAM, market: market.toBase58(), lpMint: mint.toBase58() }]);
    expect(getProgramAccounts).toHaveBeenCalledTimes(2);
    expect(getProgramAccounts.mock.calls[0]![1]).toEqual({
      commitment: "confirmed",
      filters: [{ dataSize: LP_VAULT_REGISTRY_LEN }, { memcmp: { offset: 10, bytes: "6" } }],
    });
  });

  it("returns nothing for no programs", async () => {
    const getProgramAccounts = vi.fn();
    expect(await new RpcLpVaultChain(asConn({ getProgramAccounts })).listRegistries([])).toEqual([]);
    expect(getProgramAccounts).not.toHaveBeenCalled();
  });
});

describe("RpcLpVaultChain.getSignatures", () => {
  it("maps err to failed and passes the paging options through", async () => {
    const getSignaturesForAddress = vi.fn(async () => [
      { signature: "s2", slot: 20, err: null },
      { signature: "s1", slot: 10, err: { InstructionError: [0, "Custom"] } },
    ]);
    const chain = new RpcLpVaultChain(asConn({ getSignaturesForAddress }));
    const opts = { until: "u", before: "b", limit: 50 };

    const out = await chain.getSignatures(REGISTRY, opts);

    expect(out).toEqual([
      { signature: "s2", slot: 20, failed: false },
      { signature: "s1", slot: 10, failed: true },
    ]);
    expect(getSignaturesForAddress).toHaveBeenCalledWith(expect.any(PublicKey), opts, "confirmed");
  });
});

describe("RpcLpVaultChain.getTransaction", () => {
  it("returns null when the RPC has no such transaction", async () => {
    const getTransaction = vi.fn(async () => null);
    expect(await new RpcLpVaultChain(asConn({ getTransaction })).getTransaction("sig")).toBeNull();
    expect(getTransaction).toHaveBeenCalledWith("sig", { commitment: "confirmed", maxSupportedTransactionVersion: 1 });
  });

  it("normalizes a found transaction", async () => {
    const payer = key();
    const program = key();
    const message = new MessageV0({
      header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 1 },
      staticAccountKeys: [payer, program],
      recentBlockhash: key().toBase58(),
      compiledInstructions: [{ programIdIndex: 1, accountKeyIndexes: [0], data: Uint8Array.from([7, 8]) }],
      addressTableLookups: [],
    });
    const getTransaction = vi.fn(async () => ({
      slot: 99,
      blockTime: 1_700_000_000,
      meta: { err: null, innerInstructions: [] },
      transaction: { message, signatures: [] },
    }));

    const tx = await new RpcLpVaultChain(asConn({ getTransaction })).getTransaction("sig");

    expect(tx).toMatchObject({ signature: "sig", slot: 99, blockTime: 1_700_000_000, failed: false });
    expect(tx!.instructions).toHaveLength(1);
    expect(tx!.instructions[0]).toMatchObject({ programId: program.toBase58(), accounts: [payer.toBase58()] });
  });
});

describe("RpcLpVaultChain.getClaim", () => {
  const tokenAcct = (amount: unknown) => ({ account: { data: { parsed: { info: { tokenAmount: { amount } } } } } });

  function conn(held: unknown, redemption: unknown, heldSlot = 100, redSlot = 100) {
    const getParsedTokenAccountsByOwner = vi.fn(async () => ({ context: { slot: heldSlot }, value: held }));
    const getAccountInfoAndContext = vi.fn(async () => ({ context: { slot: redSlot }, value: redemption }));
    return { getParsedTokenAccountsByOwner, getAccountInfoAndContext };
  }

  it("sums every token account, adds the live redemption, and uses the later slot", async () => {
    const c = conn(
      [tokenAcct("1000"), tokenAcct((1n << 70n).toString())],
      { data: redemptionData(REGISTRY, USER, 250n) },
      100,
      105,
    );

    const claim = await new RpcLpVaultChain(asConn(c)).getClaim(USER, MINT, REGISTRY, PROGRAM);

    expect(claim).toEqual({ heldShares: 1000n + (1n << 70n), pendingShares: 250n, contextSlot: 105 });
    expect(c.getParsedTokenAccountsByOwner).toHaveBeenCalledWith(new PublicKey(USER), { mint: new PublicKey(MINT) }, "confirmed");
    expect(c.getAccountInfoAndContext).toHaveBeenCalledWith(deriveRedemptionPda(PROGRAM, REGISTRY, USER), "confirmed");
  });

  it("takes the held-read slot when it is the later one", async () => {
    const claim = await new RpcLpVaultChain(asConn(conn([], null, 300, 200))).getClaim(USER, MINT, REGISTRY, PROGRAM);
    expect(claim).toEqual({ heldShares: 0n, pendingShares: 0n, contextSlot: 300 });
  });

  it("ignores token accounts without a string amount", async () => {
    const c = conn(
      [tokenAcct(undefined), tokenAcct(123), { account: { data: new Uint8Array(0) } }, { account: { data: { parsed: {} } } }, tokenAcct("7")],
      null,
    );
    const claim = await new RpcLpVaultChain(asConn(c)).getClaim(USER, MINT, REGISTRY, PROGRAM);
    expect(claim.heldShares).toBe(7n);
  });
});
