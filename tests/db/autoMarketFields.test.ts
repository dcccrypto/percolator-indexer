/**
 * Auto-inserted `markets` rows: deployer, oracle_authority, initial_price_e6 and
 * trading_fee_bps.
 *
 * Real case, 9EPm8nB8… on the 2026-10-01 relaunch wrapper ETDLAdiA… The indexer
 * row read deployer=DJ54k4wH… (the collateral mint), oracle_authority="",
 * initial_price_e6=0, trading_fee_bps=10, keeper_status='retired'. On chain:
 * created by 9sM73A4M… (signer and fee payer of the creation tx; the wrapper ix
 * accounts are [9sM73A4M, 9EPm8nB8, DJ54k4wH]), marketauth GE8NTjew… (a PDA),
 * tradeFeeBps 5, oracleTargetPriceE6 3630.
 */
import { describe, it, expect, vi } from "vitest";
import {
  Keypair,
  PublicKey,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import type { ConfirmedSignatureInfo, VersionedTransactionResponse } from "@solana/web3.js";
import { LAYOUT_V21, LAYOUT_V22 } from "@percolatorct/sdk";
import {
  CREATOR_LOOKUP_MAX_PAGES,
  creatorFromCreationTx,
  findSlabCreator,
  findSlabCreation,
  resolveAutoMarketFields,
  v17InitialMarginBps,
  V17_INITIAL_MARGIN_BPS_OFF,
  initialMarginBpsOffset,
  type CreatorLookupRpc,
} from "../../src/db/autoMarketFields.js";

const WRAPPER = new PublicKey("ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB");
const SLAB = new PublicKey("9EPm8nB8Fs7WcEZgE1WGFPTGc6rAzD6GhFJyMm4dEFHn");
const CREATOR = new PublicKey("9sM73A4MvS2ye2Fuvpr1tmkj68iA61eebuRKz1rnGUWa");
const MINT = new PublicKey("DJ54k4wH92NTtNP8RuHAwG8si1bevXEknzctDdqYN8eC");
const MARKETAUTH = new PublicKey("GE8NTjew1sjLVb53J1jUczDKFwn6CAqT1HhbLZ8363QV");
const SYSTEM = new PublicKey("11111111111111111111111111111111");

const v17Market = {
  header: {},
  config: {},
  params: {},
  configV17: { marketauth: MARKETAUTH, tradeFeeBps: 5n, oracleTargetPriceE6: 3630n, markEwmaE6: 0n },
};

describe("resolveAutoMarketFields (9EPm8nB8 real case)", () => {
  it("records the creator as deployer, never the collateral mint", () => {
    const f = resolveAutoMarketFields(v17Market, CREATOR.toBase58());
    expect(f.deployer).toBe(CREATOR.toBase58());
    expect(f.deployer).not.toBe(MINT.toBase58());
  });

  it("falls back to configV17.marketauth (not the mint) when the creator is unknown", () => {
    const f = resolveAutoMarketFields(v17Market, null);
    expect(f.deployer).toBe(MARKETAUTH.toBase58());
  });

  it("reads trading_fee_bps from configV17.tradeFeeBps (5), not the hard-coded 10", () => {
    expect(resolveAutoMarketFields(v17Market, null).trading_fee_bps).toBe(5);
  });

  it("writes NULL oracle_authority for v17 (no empty string)", () => {
    expect(resolveAutoMarketFields(v17Market, null).oracle_authority).toBeNull();
  });

  it("takes initial_price_e6 from oracleTargetPriceE6, and NULL rather than 0 when unset", () => {
    expect(resolveAutoMarketFields(v17Market, null).initial_price_e6).toBe(3630);
    const unpriced = { ...v17Market, configV17: { ...v17Market.configV17, oracleTargetPriceE6: 0n } };
    expect(resolveAutoMarketFields(unpriced, null).initial_price_e6).toBeNull();
  });

  it("v12: header.admin, config.oracleAuthority, params.tradingFeeBps", () => {
    const admin = Keypair.generate().publicKey;
    const oracle = Keypair.generate().publicKey;
    const f = resolveAutoMarketFields(
      {
        header: { admin },
        config: { oracleAuthority: oracle, authorityPriceE6: 1_500_000n },
        params: { tradingFeeBps: 7n },
      },
      null,
    );
    expect(f).toEqual({
      deployer: admin.toBase58(),
      oracle_authority: oracle.toBase58(),
      initial_price_e6: 1_500_000,
      trading_fee_bps: 7,
    });
  });

  it("NULL deployer when nothing on chain names anyone (caller skips the row)", () => {
    expect(resolveAutoMarketFields({ configV17: { marketauth: PublicKey.default } }, null).deployer).toBeNull();
  });

  it("an unreadable fee falls back to 10", () => {
    expect(resolveAutoMarketFields({ configV17: { marketauth: MARKETAUTH } }, null).trading_fee_bps).toBe(10);
  });
});

/** A confirmed-tx response shaped like getTransaction's, built from a real compiled v0 message. */
function txResponse(payer: PublicKey, ixs: TransactionInstruction[]): VersionedTransactionResponse {
  const message = new TransactionMessage({
    payerKey: payer,
    recentBlockhash: "11111111111111111111111111111111",
    instructions: ixs,
  }).compileToV0Message();
  return {
    slot: 1,
    blockTime: null,
    transaction: { message, signatures: [] },
    meta: null,
    version: 0,
  } as unknown as VersionedTransactionResponse;
}

function creationTx(): VersionedTransactionResponse {
  // Mirrors 9EPm8nB8's creation: wrapper ix with accounts [creator(s), slab(s), mint].
  return txResponse(CREATOR, [
    new TransactionInstruction({
      programId: WRAPPER,
      keys: [
        { pubkey: CREATOR, isSigner: true, isWritable: true },
        { pubkey: SLAB, isSigner: true, isWritable: true },
        { pubkey: MINT, isSigner: false, isWritable: false },
      ],
      data: Buffer.from([0]),
    }),
  ]);
}

describe("creatorFromCreationTx", () => {
  it("returns the signing first account of the wrapper ix that touches the slab", () => {
    expect(creatorFromCreationTx(creationTx(), SLAB, WRAPPER)).toBe(CREATOR.toBase58());
  });

  it("returns NULL for a transaction with no wrapper ix touching the slab", () => {
    const other = txResponse(CREATOR, [
      new TransactionInstruction({
        programId: SYSTEM,
        keys: [{ pubkey: SLAB, isSigner: false, isWritable: true }],
        data: Buffer.from([2]),
      }),
    ]);
    expect(creatorFromCreationTx(other, SLAB, WRAPPER)).toBeNull();
  });

  it("falls back to the fee payer when the ix's first account did not sign", () => {
    const payer = Keypair.generate().publicKey;
    const tx = txResponse(payer, [
      new TransactionInstruction({
        programId: WRAPPER,
        keys: [
          { pubkey: MARKETAUTH, isSigner: false, isWritable: false },
          { pubkey: SLAB, isSigner: false, isWritable: true },
        ],
        data: Buffer.from([1]),
      }),
    ]);
    expect(creatorFromCreationTx(tx, SLAB, WRAPPER)).toBe(payer.toBase58());
  });
});

function sig(n: number, err: unknown = null): ConfirmedSignatureInfo {
  return { signature: `sig${n}`, slot: n, err: err as ConfirmedSignatureInfo["err"], memo: null, blockTime: null };
}

describe("findSlabCreation (#223)", () => {
  it("returns the creation transaction's blockTime alongside the creator", async () => {
    const tx = { ...creationTx(), blockTime: 1_700_000_123 };
    const rpc: CreatorLookupRpc = {
      getSignaturesForAddress: vi.fn(async () => [{ ...sig(1), blockTime: 1_700_000_100 }]),
      getTransaction: vi.fn(async () => tx),
    };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toEqual({ creator: CREATOR.toBase58(), blockTime: 1_700_000_123 });
  });

  it("falls back to the oldest signature's time, and is null when history is out of reach", async () => {
    const rpc: CreatorLookupRpc = {
      getSignaturesForAddress: vi.fn(async () => [{ ...sig(1), blockTime: 1_700_000_100 }]),
      getTransaction: vi.fn(async () => null),
    };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toEqual({ creator: null, blockTime: 1_700_000_100 });
    const deep: CreatorLookupRpc = {
      getSignaturesForAddress: vi.fn(async () => Array.from({ length: 1000 }, (_, i) => sig(i))),
      getTransaction: vi.fn(async () => null),
    };
    await expect(findSlabCreation(deep, SLAB, WRAPPER)).resolves.toEqual({ creator: null, blockTime: null });
  });
});

describe("findSlabCreator", () => {
  it("walks to the oldest page and returns the creator from the oldest successful tx", async () => {
    // Newest first, like the RPC: page 1 = sig2002..sig1003, page 2 = sig1002..sig1000.
    const page1 = Array.from({ length: 1000 }, (_, i) => sig(2002 - i));
    const page2 = [sig(1002), sig(1001), sig(1000, { InstructionError: [0, "Custom"] })];
    const rpc: CreatorLookupRpc = {
      getSignaturesForAddress: vi.fn(async (_a, opts) => (opts.before ? page2 : page1)),
      getTransaction: vi.fn(async (s: string) => (s === "sig1001" ? creationTx() : null)),
    };
    await expect(findSlabCreator(rpc, SLAB, WRAPPER)).resolves.toBe(CREATOR.toBase58());
    // The failed sig1000 is skipped; sig1001 (oldest successful) is tried first.
    expect(vi.mocked(rpc.getTransaction).mock.calls[0][0]).toBe("sig1001");
  });

  it("X-1: ONE unreadable tx is skipped and recorded like the other sites (full signature + slab); an all-unreadable lookup is refused", async () => {
    const { existsSync, readFileSync, rmSync } = await import("node:fs");
    const { resetSkippedSignatureDedupe } = await import("../../src/lib/skippedSignatures.js");
    resetSkippedSignatureDedupe();
    const file = `/tmp/creator-skip-${process.pid}.jsonl`;
    process.env.SKIPPED_SIGNATURES_FILE = file;
    rmSync(file, { force: true });
    try {
      const V2 = Object.assign(new Error("Transaction version (2) is not supported by the requesting client"), { code: -32015 });
      const page = [sig(1002), sig(1001), sig(1000)];
      const mk = (bad: string[]): CreatorLookupRpc => ({
        getSignaturesForAddress: vi.fn(async () => page),
        getTransaction: vi.fn(async (s: string) => { if (bad.includes(s)) throw V2; return s === "sig1001" ? creationTx() : null; }),
      });
      // oldest-first scan: sig1000 is unreadable -> skipped and recorded, the lookup continues to the creation tx sig1001
      await expect(findSlabCreator(mk(["sig1000"]), SLAB, WRAPPER)).resolves.toBe(CREATOR.toBase58());
      const rec = readFileSync(file, "utf8").trim().split("\n").map((l) => JSON.parse(l));
      expect(rec).toHaveLength(1);
      expect(rec[0]).toMatchObject({ signature: "sig1000", slab: SLAB.toBase58(), source: "creator-lookup" });
      // EVERY candidate unreadable = broken reader: refused, nothing recorded
      rmSync(file, { force: true });
      resetSkippedSignatureDedupe();
      const many = Array.from({ length: 6 }, (_, i) => sig(1000 + i));
      const rpcMany: CreatorLookupRpc = {
        getSignaturesForAddress: vi.fn(async () => many),
        getTransaction: vi.fn(async () => { throw V2; }),
      };
      await expect(findSlabCreator(rpcMany, SLAB, WRAPPER)).rejects.toThrow(/mass skip refused/);
      expect(existsSync(file)).toBe(false);
    } finally {
      delete process.env.SKIPPED_SIGNATURES_FILE;
      rmSync(file, { force: true });
    }
  });

  it("returns NULL when the start of history is out of reach", async () => {
    const full = Array.from({ length: 1000 }, (_, i) => sig(i));
    const rpc: CreatorLookupRpc = {
      getSignaturesForAddress: vi.fn(async () => full),
      getTransaction: vi.fn(async () => creationTx()),
    };
    await expect(findSlabCreator(rpc, SLAB, WRAPPER)).resolves.toBeNull();
    expect(rpc.getSignaturesForAddress).toHaveBeenCalledTimes(CREATOR_LOOKUP_MAX_PAGES);
    expect(rpc.getTransaction).not.toHaveBeenCalled();
  });
});

describe("v17InitialMarginBps (max_leverage source), VERSION-keyed", () => {
  function slabWithIm(bps: bigint, len = 4096, version = 18): Uint8Array {
    const data = new Uint8Array(len);
    const dv = new DataView(data.buffer);
    dv.setBigUint64(0, 0x5045_5243_5631_3600n, true); // wrapper magic
    dv.setUint16(8, version, true);
    data[10] = 1; // KIND_MARKET
    dv.setBigUint64(V17_INITIAL_MARGIN_BPS_OFF, bps, true);
    return data;
  }

  it("sits at market-group + 32 + 62 = 686 on the deployed (v2.1) layout and on v2.2 (band/rent words are appended at the config END)", () => {
    expect(V17_INITIAL_MARGIN_BPS_OFF).toBe(686);
    expect(initialMarginBpsOffset(LAYOUT_V21)).toBe(686);
    expect(initialMarginBpsOffset(LAYOUT_V22)).toBe(686);
  });

  it("reads the engine's initial margin on v2.1 and v2.2 (9EPm8nB8: 1819 bps, so 5x not the 10x fallback)", () => {
    expect(v17InitialMarginBps(slabWithIm(1819n))).toBe(1819n);
    expect(v17InitialMarginBps(slabWithIm(1819n, 4096, 19))).toBe(1819n);
    expect(Math.floor(10_000 / Number(v17InitialMarginBps(slabWithIm(1819n))))).toBe(5);
  });

  it("NULL for a short account, an unset (zero) margin, an UNKNOWN VERSION (never read with another layout), or a non-wrapper buffer", () => {
    expect(v17InitialMarginBps(slabWithIm(1819n).subarray(0, V17_INITIAL_MARGIN_BPS_OFF + 7))).toBeNull();
    expect(v17InitialMarginBps(slabWithIm(0n))).toBeNull();
    expect(v17InitialMarginBps(slabWithIm(1819n, 4096, 20))).toBeNull();
    const raw = new Uint8Array(4096);
    new DataView(raw.buffer).setBigUint64(V17_INITIAL_MARGIN_BPS_OFF, 1819n, true);
    expect(v17InitialMarginBps(raw)).toBeNull();
  });
});

describe("resolveAutoMarketFields fallbacks", () => {
  const key = () => Keypair.generate().publicKey;

  it("a null/absent header, config, params and configV17 yields all-null fields and the default fee", () => {
    for (const m of [{}, { header: null, config: null, params: null, configV17: null }]) {
      expect(resolveAutoMarketFields(m, null)).toEqual({ deployer: null, oracle_authority: null, initial_price_e6: null, trading_fee_bps: 10 });
    }
  });

  it("an all-zero (default) header.admin or oracleAuthority counts as unset; creator wins over header.admin", () => {
    const real = key();
    expect(resolveAutoMarketFields({ header: { admin: PublicKey.default }, config: { oracleAuthority: PublicKey.default } }, null))
      .toMatchObject({ deployer: null, oracle_authority: null });
    expect(resolveAutoMarketFields({ header: { admin: real }, configV17: { marketauth: MARKETAUTH } }, null).deployer).toBe(real.toBase58());
    expect(resolveAutoMarketFields({ header: { admin: real } }, CREATOR.toBase58()).deployer).toBe(CREATOR.toBase58());
  });

  it("v12 price falls back authorityPriceE6 -> markEwmaE6 -> null (zero, negative, absent all unset)", () => {
    expect(resolveAutoMarketFields({ config: { authorityPriceE6: 0n, markEwmaE6: 42n } }, null).initial_price_e6).toBe(42);
    expect(resolveAutoMarketFields({ config: { authorityPriceE6: -1n, markEwmaE6: 0n } }, null).initial_price_e6).toBeNull();
    expect(resolveAutoMarketFields({ config: {} }, null).initial_price_e6).toBeNull();
  });

  it("v17 price falls back oracleTargetPriceE6 -> markEwmaE6; a value beyond double range is unset", () => {
    expect(resolveAutoMarketFields({ configV17: { oracleTargetPriceE6: 0n, markEwmaE6: 99n } }, null).initial_price_e6).toBe(99);
    expect(resolveAutoMarketFields({ configV17: { oracleTargetPriceE6: 10n ** 400n } }, null).initial_price_e6).toBeNull();
  });

  it("a u64 price above 2^53 is unset rather than stored rounded; 2^53-1 is exact", () => {
    const big = 2n ** 53n + 1n; // Number(big) === 2^53: silently rounded
    expect(resolveAutoMarketFields({ configV17: { oracleTargetPriceE6: big } }, null).initial_price_e6).toBeNull();
    expect(resolveAutoMarketFields({ config: { authorityPriceE6: 2n ** 64n - 1n } }, null).initial_price_e6).toBeNull();
    expect(resolveAutoMarketFields({ configV17: { oracleTargetPriceE6: 2n ** 53n - 1n } }, null).initial_price_e6).toBe(Number.MAX_SAFE_INTEGER);
    // an unsafe primary falls through to a safe fallback
    expect(resolveAutoMarketFields({ configV17: { oracleTargetPriceE6: big, markEwmaE6: 7n } }, null).initial_price_e6).toBe(7);
  });

  it("fee bounds: 0 and 10000 are kept; 10001 and negative fall back to 10; null falls back; v12 reads params", () => {
    const f = (tradeFeeBps: bigint | null | undefined) => resolveAutoMarketFields({ configV17: { tradeFeeBps: tradeFeeBps as bigint } }, null).trading_fee_bps;
    expect([f(0n), f(10_000n), f(10_001n), f(-1n), f(null), f(undefined)]).toEqual([0, 10_000, 10, 10, 10, 10]);
    expect(resolveAutoMarketFields({ params: { tradingFeeBps: 10_001n } }, null).trading_fee_bps).toBe(10);
    expect(resolveAutoMarketFields({ params: {} }, null).trading_fee_bps).toBe(10);
  });
});

describe("findSlabCreation history-walk and tx edge cases", () => {
  it("an account with no signatures at all: exhausted, nothing found", async () => {
    const rpc: CreatorLookupRpc = { getSignaturesForAddress: vi.fn(async () => []), getTransaction: vi.fn() };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toEqual({ creator: null, blockTime: null });
    expect(rpc.getTransaction).not.toHaveBeenCalled();
  });

  it("a full 1000-signature page followed by an empty page ends the walk on the earlier page", async () => {
    const full = Array.from({ length: 1000 }, (_, i) => sig(2000 - i));
    const getSignaturesForAddress = vi.fn()
      .mockResolvedValueOnce(full)
      .mockResolvedValueOnce([]);
    const rpc: CreatorLookupRpc = { getSignaturesForAddress, getTransaction: vi.fn(async (s: string) => (s === "sig1001" ? creationTx() : null)) };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toMatchObject({ creator: CREATOR.toBase58() });
    expect(getSignaturesForAddress).toHaveBeenCalledTimes(2);
    expect(getSignaturesForAddress.mock.calls[1][1]).toMatchObject({ before: "sig1001" });
  });

  it("failed (err) signatures are never fetched; with only failed ones the result is null/null", async () => {
    const rpc: CreatorLookupRpc = { getSignaturesForAddress: vi.fn(async () => [sig(1, { InstructionError: [0, "x"] })]), getTransaction: vi.fn() };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toEqual({ creator: null, blockTime: null });
    expect(rpc.getTransaction).not.toHaveBeenCalled();
  });

  it("a null getTransaction result is skipped; no creation found returns the oldest signature's time", async () => {
    const s = { ...sig(5), blockTime: 1_700_000_000 };
    const rpc: CreatorLookupRpc = { getSignaturesForAddress: vi.fn(async () => [s]), getTransaction: vi.fn(async () => null) };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).resolves.toEqual({ creator: null, blockTime: 1_700_000_000 });
  });

  it("blockTime comes from the creation tx when it has one, else from the oldest signature", async () => {
    const withTime = { ...creationTx(), blockTime: 123 } as VersionedTransactionResponse;
    const mk = (tx: VersionedTransactionResponse): CreatorLookupRpc => ({
      getSignaturesForAddress: vi.fn(async () => [{ ...sig(1), blockTime: 77 }]),
      getTransaction: vi.fn(async () => tx),
    });
    await expect(findSlabCreation(mk(withTime), SLAB, WRAPPER)).resolves.toEqual({ creator: CREATOR.toBase58(), blockTime: 123 });
    await expect(findSlabCreation(mk(creationTx()), SLAB, WRAPPER)).resolves.toEqual({ creator: CREATOR.toBase58(), blockTime: 77 });
  });

  it("only the 5 OLDEST successful signatures are tried", async () => {
    const page = Array.from({ length: 8 }, (_, i) => sig(100 - i)); // newest first
    const getTransaction = vi.fn(async () => null);
    await findSlabCreation({ getSignaturesForAddress: vi.fn(async () => page), getTransaction }, SLAB, WRAPPER);
    expect(getTransaction.mock.calls.map((c) => c[0])).toEqual(["sig93", "sig94", "sig95", "sig96", "sig97"]);
  });

  it("a non-poison fetch error (e.g. network) propagates instead of being skipped", async () => {
    const rpc: CreatorLookupRpc = { getSignaturesForAddress: vi.fn(async () => [sig(1)]), getTransaction: vi.fn(async () => { throw new Error("socket hang up"); }) };
    await expect(findSlabCreation(rpc, SLAB, WRAPPER)).rejects.toThrow("socket hang up");
  });

  it("empty v0 loadedAddresses are accepted; a different program id finds no creation", async () => {
    const tx = creationTx();
    (tx as unknown as { meta: unknown }).meta = { loadedAddresses: { writable: [], readonly: [] } };
    expect(creatorFromCreationTx(tx, SLAB, WRAPPER)).toBe(CREATOR.toBase58());
    // wrapper program present but a different program id asked for -> no creation
    expect(creatorFromCreationTx(tx, SLAB, SYSTEM)).toBeNull();
  });
});

describe("findSlabCreation skip bookkeeping edge cases", () => {
  it("a thrown non-Error poison value is skipped (stringified), and a non-creation tx is passed over to the next candidate", async () => {
    const { existsSync, readFileSync, rmSync } = await import("node:fs");
    const { resetSkippedSignatureDedupe } = await import("../../src/lib/skippedSignatures.js");
    resetSkippedSignatureDedupe();
    const file = `/tmp/creator-skip-str-${process.pid}.jsonl`;
    process.env.SKIPPED_SIGNATURES_FILE = file;
    rmSync(file, { force: true });
    try {
      const other = txResponse(CREATOR, [new TransactionInstruction({ programId: SYSTEM, keys: [{ pubkey: SLAB, isSigner: false, isWritable: true }], data: Buffer.from([2]) })]);
      const page = [sig(1002), sig(1001), sig(1000)]; // oldest first: 1000 (unreadable, plain object), 1001 (not the creation), 1002 (creation)
      const getTransaction = vi.fn(async (s: string) => {
        if (s === "sig1000") throw { code: -32015 };
        return s === "sig1001" ? other : creationTx();
      });
      await expect(findSlabCreation({ getSignaturesForAddress: vi.fn(async () => page), getTransaction }, SLAB, WRAPPER))
        .resolves.toMatchObject({ creator: CREATOR.toBase58() });
      expect(getTransaction).toHaveBeenCalledTimes(3);
      expect(existsSync(file)).toBe(true);
      const rec = readFileSync(file, "utf8").trim().split("\n").map((l) => JSON.parse(l));
      expect(rec).toHaveLength(1);
      expect(rec[0]).toMatchObject({ signature: "sig1000", error: "[object Object]" });
    } finally {
      delete process.env.SKIPPED_SIGNATURES_FILE;
      rmSync(file, { force: true });
    }
  });
});

describe("creatorFromCreationTx malformed / non-touching instructions", () => {
  const withIxs = (ixs: Array<{ programIdIndex: number; accountKeyIndexes: number[] }>) => {
    const tx = creationTx();
    (tx.transaction.message as unknown as { compiledInstructions: unknown }).compiledInstructions = ixs.map((i) => ({ ...i, data: new Uint8Array() }));
    return tx;
  };
  // creationTx account keys: 0 CREATOR, 1 SLAB, 2 MINT, 3 WRAPPER (static, in compile order)
  const wrapperIdx = () => {
    const keys = creationTx().transaction.message.getAccountKeys();
    return keys.staticAccountKeys.findIndex((k) => k.equals(WRAPPER));
  };

  it("a wrapper ix that does not reference the slab is passed over", () => {
    const w = wrapperIdx();
    expect(creatorFromCreationTx(withIxs([{ programIdIndex: w, accountKeyIndexes: [0, 2] }]), SLAB, WRAPPER)).toBeNull();
  });

  it("an ix whose program index is out of range is passed over, and an out-of-range account index does not count as the slab", () => {
    const w = wrapperIdx();
    expect(creatorFromCreationTx(withIxs([{ programIdIndex: 99, accountKeyIndexes: [0, 1] }]), SLAB, WRAPPER)).toBeNull();
    expect(creatorFromCreationTx(withIxs([{ programIdIndex: w, accountKeyIndexes: [98, 2] }]), SLAB, WRAPPER)).toBeNull();
    // a later well-formed ix is still found after the malformed one
    expect(creatorFromCreationTx(withIxs([{ programIdIndex: 99, accountKeyIndexes: [0, 1] }, { programIdIndex: w, accountKeyIndexes: [0, 1] }]), SLAB, WRAPPER)).toBe(CREATOR.toBase58());
  });
});
