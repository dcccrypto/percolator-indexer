import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";

const mocks = vi.hoisted(() => ({
  getAccountInfo: vi.fn(),
  getSignaturesForAddress: vi.fn().mockResolvedValue([]),
  getParsedTransactions: vi.fn().mockResolvedValue([]),
  upsert: vi.fn().mockResolvedValue({ error: null }),
  captureException: vi.fn(),
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
    getSignaturesForAddress: mocks.getSignaturesForAddress,
    getParsedTransactions: mocks.getParsedTransactions,
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
  captureException: mocks.captureException,
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
const OTHER_PROGRAM_ID = "11111111111111111111111111111111";
const TRADER = "So11111111111111111111111111111111111111112";
const PORTFOLIO_A = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const PORTFOLIO_B = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const SIG = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";

function makePortfolioData(slabAddress: string): Uint8Array {
  const data = new Uint8Array(64);
  data.set(new PublicKey(slabAddress).toBytes(), 16);
  return data;
}

function makeTxWithSeparatedTransferInstructions() {
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
            programId: { toBase58: () => OTHER_PROGRAM_ID },
            data: "non-matching",
            accounts: [],
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
    mocks.getSignaturesForAddress.mockResolvedValue([]);
    mocks.getParsedTransactions.mockResolvedValue([]);
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
      makeTxWithSeparatedTransferInstructions(),
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
    expect(indexedInstructionIndexes).toEqual([0, 2]);

    expect(mocks.upsert.mock.calls[0][1]).toEqual(expect.objectContaining({
      onConflict: "signature,instruction_index",
    }));
  });

  it("keeps re-indexing idempotent for the same transaction", async () => {
    const storedRows = new Map<string, any>();

    mocks.upsert.mockImplementation(
      async (rows: any[], options: { onConflict: string }) => {
        expect(options).toEqual(expect.objectContaining({
          onConflict: "signature,instruction_index",
        }));

        for (const row of rows) {
          storedRows.set(
            `${row.signature}:${row.instruction_index}`,
            row,
          );
        }

        return { error: null };
      },
    );

    const indexer = new NftIndexerPolling();
    const tx = makeTxWithSeparatedTransferInstructions();

    const firstPass = await (indexer as any).processTransaction(
      tx,
      SIG,
      SLAB,
      new Set([PROGRAM_ID]),
      123,
      1_700_000_000,
    );

    const secondPass = await (indexer as any).processTransaction(
      tx,
      SIG,
      SLAB,
      new Set([PROGRAM_ID]),
      123,
      1_700_000_000,
    );

    expect(firstPass).toBe(true);
    expect(secondPass).toBe(true);
    expect(mocks.upsert).toHaveBeenCalledTimes(2);

    const firstRows = mocks.upsert.mock.calls[0][0];
    const secondRows = mocks.upsert.mock.calls[1][0];

    expect(secondRows).toEqual(firstRows);
    expect(storedRows.size).toBe(2);

    const storedInstructionIndexes = [...storedRows.values()]
      .map((row: any) => row.instruction_index);

    expect(storedInstructionIndexes).toEqual([0, 2]);
  });
  it("captures transaction failures and preserves the existing cursor for retry", async () => {
    mocks.getSignaturesForAddress.mockResolvedValue([
      {
        signature: SIG,
        err: null,
        slot: 123,
        blockTime: 1_700_000_000,
      },
    ]);

    mocks.getParsedTransactions.mockResolvedValue([
      makeTxWithSeparatedTransferInstructions(),
    ]);

    mocks.upsert.mockResolvedValue({
      error: { message: "database unavailable" },
    });

    const indexer = new NftIndexerPolling();
    const previousSignature = "previous-signature";

    (indexer as any).lastSignature.set(SLAB, previousSignature);

    await (indexer as any).indexNftEventsForSlab(SLAB, 1);

    expect(mocks.upsert).toHaveBeenCalledTimes(1);
    expect(mocks.captureException).toHaveBeenCalledTimes(1);
    expect(mocks.captureException).toHaveBeenCalledWith(
      expect.any(Error),
      {
        tags: {
          context: "nft-indexer-process-tx",
          slabAddress: SLAB,
        },
      },
    );

    expect(
      (indexer as any).lastSignature.get(SLAB),
    ).toBe(previousSignature);
  });

});
