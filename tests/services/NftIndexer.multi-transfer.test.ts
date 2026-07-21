import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";

const mocks = vi.hoisted(() => ({
  getAccountInfo: vi.fn(),
  upsert: vi.fn().mockResolvedValue({ error: null }),
}));

vi.mock("@percolatorct/sdk", () => ({
  IX_TAG: {
    TransferPortfolioOwnership: 72,
    MintPositionNft: 64,
    BurnPositionNft: 66,
  },
}));

vi.mock("@percolatorct/shared", () => ({
  config: {
    allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"],
  },
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  getConnection: vi.fn(() => ({
    getSignaturesForAddress: vi.fn().mockResolvedValue([]),
    getParsedTransactions: vi.fn().mockResolvedValue([]),
    getAccountInfo: mocks.getAccountInfo,
  })),
  getSupabase: vi.fn(() => ({
    from: vi.fn(() => ({
      upsert: mocks.upsert,
    })),
  })),
  getMarkets: vi.fn(async () => [
    { slab_address: "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD" },
  ]),
  getNetwork: vi.fn(() => "devnet"),
  withRetry: vi.fn(async (fn: any) => fn()),
  captureException: vi.fn(),
  decodeBase58: vi.fn((data: string) => {
    const buf = new Uint8Array(35);
    buf[0] = 72; // TransferPortfolioOwnership

    // Different owner bytes for each instruction so the two events are distinct.
    if (data === "transfer-a") {
      buf[1] = 1;
      buf[33] = 0;
      buf[34] = 0; // asset_index = 0
    } else {
      buf[1] = 2;
      buf[33] = 1;
      buf[34] = 0; // asset_index = 1
    }

    return buf;
  }),
}));

import { NftIndexerPolling } from "../../src/services/NftIndexer.js";

const SLAB = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const PROGRAM_ID = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const TRADER = "So11111111111111111111111111111111111111112";
const PORTFOLIO_A = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const PORTFOLIO_B = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const SIG = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";

function makePortfolioData(slabAddress: string): Uint8Array {
  const data = new Uint8Array(64);
  data.set(new PublicKey(slabAddress).toBytes(), 16);
  return data;
}

function makeTxWithTwoTransferInstructions() {
  return {
    meta: { err: null },
    transaction: {
      message: {
        instructions: [
          {
            programId: { toBase58: () => PROGRAM_ID },
            data: "transfer-a",
            accounts: [
              { toBase58: () => TRADER },
              { toBase58: () => PORTFOLIO_A },
              { toBase58: () => SLAB },
            ],
          },
          {
            programId: { toBase58: () => PROGRAM_ID },
            data: "transfer-b",
            accounts: [
              { toBase58: () => TRADER },
              { toBase58: () => PORTFOLIO_B },
              { toBase58: () => SLAB },
            ],
          },
        ],
      },
    },
  };
}

describe("NftIndexerPolling multi-transfer transaction handling", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.upsert.mockResolvedValue({ error: null });

    // Both portfolio accounts resolve to the same slab, so both events are valid
    // for the currently polled slab.
    mocks.getAccountInfo.mockResolvedValue({
      data: makePortfolioData(SLAB),
    });
  });

  it("indexes every TransferPortfolioOwnership instruction in the same transaction", async () => {
    const indexer = new NftIndexerPolling();

    const didIndex = await (indexer as any).processTransaction(
      makeTxWithTwoTransferInstructions(),
      SIG,
      SLAB,
      new Set([PROGRAM_ID]),
      123,
      1_700_000_000,
    );

    expect(didIndex).toBe(true);

    // A single transaction can contain multiple program instructions.
    // The indexer should collect all valid TransferPortfolioOwnership events
    // and write them in one bulk upsert.
    expect(mocks.upsert).toHaveBeenCalledTimes(1);

    const rows = mocks.upsert.mock.calls[0][0];
    expect(rows).toHaveLength(2);

    const indexedAssetIndexes = rows.map((row: any) => row.user_idx);
    expect(indexedAssetIndexes).toEqual([0, 1]);

    const indexedInstructionIndexes = rows.map((row: any) => row.instruction_index);
    expect(indexedInstructionIndexes).toEqual([0, 1]);

    expect(mocks.upsert.mock.calls[0][1]).toEqual(expect.objectContaining({
      onConflict: "signature,instruction_index",
    }));
  });
});
