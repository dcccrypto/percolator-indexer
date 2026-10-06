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
