/**
 * Tests for NftIndexerPolling — focused on issue #134 (L-3):
 * portfolio getAccountInfo null/error must SKIP the record, not fall back
 * to the poll-loop slab address.
 *
 * We invoke the private `processTransaction` method directly via a cast to
 * `any` to avoid requiring a full async polling cycle for unit tests.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { PublicKey } from '@solana/web3.js';

// ---------------------------------------------------------------------------
// Mocks (must come before imports of the module under test)
// ---------------------------------------------------------------------------

const mockGetAccountInfo = vi.fn();

vi.mock('@percolatorct/sdk', () => ({
  IX_TAG: {
    TransferPortfolioOwnership: 72,
    MintPositionNft: 64,
    BurnPositionNft: 66,
  },
}));

const mockUpsert = vi.fn().mockResolvedValue({ error: null });

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'],
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
    getAccountInfo: mockGetAccountInfo,
  })),
  getSupabase: vi.fn(() => ({
    from: vi.fn(() => ({
      upsert: mockUpsert,
    })),
  })),
  getMarkets: vi.fn(async () => [{ slab_address: SLAB }]),
  getNetwork: vi.fn(() => 'devnet'),
  withRetry: vi.fn(async (fn: any) => fn()),
  captureException: vi.fn(),
  decodeBase58: vi.fn((data: string) => {
    // Return a 35-byte v17 TransferPortfolioOwnership payload:
    //   tag(1) + new_owner([u8;32]) + asset_index(u16 LE)
    const buf = new Uint8Array(35);
    buf[0] = 72; // IX_TAG.TransferPortfolioOwnership
    buf.set(new PublicKey(TRADER).toBytes(), 1);
    buf[33] = 0; buf[34] = 0; // asset_index = 0
    return buf;
  }),
}));

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SLAB = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const PORTFOLIO = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';
const DIFFERENT_SLAB = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL';

/** Build a minimal ParsedTransactionWithMeta with a TransferPortfolioOwnership ix. */
function makeParsedTx(portfolioAccount = PORTFOLIO) {
  return {
    meta: { err: null },
    transaction: {
      message: {
        instructions: [
          {
            programId: { toBase58: () => PROGRAM_ID },
            data: 'some-base58-data',
            accounts: [
              { toBase58: () => TRADER },           // [0] mint_auth
              { toBase58: () => portfolioAccount }, // [1] portfolio
              { toBase58: () => SLAB },             // [2] nft_registry
            ],
          },
        ],
      },
    },
  };
}

/** Build portfolio account data with `slabAddress` embedded at bytes [16:48]. */
function makePortfolioData(slabAddress: string): Uint8Array {
  const data = new Uint8Array(64);
  data.set(new PublicKey(slabAddress).toBytes(), 16);
  return data;
}

// ---------------------------------------------------------------------------
// Import after mocks
// ---------------------------------------------------------------------------

import { NftIndexerPolling, PortfolioFetchUnavailableError } from '../../src/services/NftIndexer.js';
import * as shared from '@percolatorct/shared';

// ---------------------------------------------------------------------------
// Helpers to invoke private methods
// ---------------------------------------------------------------------------

type PrivateIndexer = {
  processTransaction(
    tx: any,
    signature: string,
    slabAddress: string,
    programIds: Set<string>,
    slot: number,
    timestamp: number,
  ): Promise<boolean>;
  upsertNftEvent(event: any): Promise<void>;
};

function asPrivate(indexer: NftIndexerPolling): PrivateIndexer {
  return indexer as any;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('NftIndexerPolling — processTransaction (#134: skip-on-null portfolio)', () => {
  const programIds = new Set([PROGRAM_ID]);

  beforeEach(() => {
    vi.clearAllMocks();
    mockUpsert.mockResolvedValue({ error: null });
  });

  it('upserts the NFT event when portfolio account returns correct slab', async () => {
    mockGetAccountInfo.mockResolvedValue({ data: makePortfolioData(SLAB) });

    const indexer = new NftIndexerPolling();
    const didIndex = await asPrivate(indexer).processTransaction(
      makeParsedTx(),
      SIG,
      SLAB,
      programIds,
      100,
      1_000_000,
    );

    expect(mockGetAccountInfo).toHaveBeenCalled();
    expect(didIndex).toBe(true);
    expect(mockUpsert).toHaveBeenCalledWith(
      expect.objectContaining({
        signature: SIG,
        slab: SLAB,
        event_type: 'transfer',
      }),
      expect.objectContaining({ onConflict: 'signature' }),
    );
  });

  it('skips the NFT event when portfolio getAccountInfo returns null (#134)', async () => {
    // Old behaviour: actualSlab defaulted to slabAddress → event was written with wrong attribution.
    // New behaviour: actualSlab is null → skip.
    mockGetAccountInfo.mockResolvedValue(null);

    const indexer = new NftIndexerPolling();
    const didIndex = await asPrivate(indexer).processTransaction(
      makeParsedTx(),
      SIG,
      SLAB,
      programIds,
      100,
      1_000_000,
    );

    expect(didIndex).toBe(false);
    expect(mockUpsert).not.toHaveBeenCalled();
  });

  it('does not write the NFT event when portfolio getAccountInfo keeps failing (#134/#165)', async () => {
    // #134's guarantee is unchanged: an unverified portfolio must NEVER be
    // written, because unverified attribution is wrong attribution.
    // #165 changes only the MECHANISM — it now raises instead of returning
    // false, so the poll loop can tell a transient failure apart from a
    // deterministic skip and hold its cursor rather than dropping the event.
    mockGetAccountInfo.mockRejectedValue(new Error('RPC 503 Service Unavailable'));

    const indexer = new NftIndexerPolling();

    await expect(
      asPrivate(indexer).processTransaction(
        makeParsedTx(),
        SIG,
        SLAB,
        programIds,
        100,
        1_000_000,
      ),
    ).rejects.toBeInstanceOf(PortfolioFetchUnavailableError);

    expect(mockUpsert).not.toHaveBeenCalled();
  });

  it('skips when portfolio account data is too short to read slab address (#134)', async () => {
    // Account exists but data is only 10 bytes — cannot read slab at offset 16:48
    mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(10) });

    const indexer = new NftIndexerPolling();
    const didIndex = await asPrivate(indexer).processTransaction(
      makeParsedTx(),
      SIG,
      SLAB,
      programIds,
      100,
      1_000_000,
    );

    expect(didIndex).toBe(false);
    expect(mockUpsert).not.toHaveBeenCalled();
  });

  it('skips when derived slab does not match poll-loop slab (#134)', async () => {
    // Portfolio account points to a DIFFERENT slab (cross-market transaction)
    mockGetAccountInfo.mockResolvedValue({ data: makePortfolioData(DIFFERENT_SLAB) });

    const indexer = new NftIndexerPolling();
    const didIndex = await asPrivate(indexer).processTransaction(
      makeParsedTx(),
      SIG,
      SLAB,
      programIds,
      100,
      1_000_000,
    );

    // actualSlab !== slabAddress → skip
    expect(didIndex).toBe(false);
    expect(mockUpsert).not.toHaveBeenCalled();
  });
});

describe('NftIndexerPolling — #165: cursor must not advance past a transient failure', () => {
  const programIds = new Set([PROGRAM_ID]);
  const OTHER_SIG = '5'.repeat(87);

  /**
   * Drives indexNftEventsForSlab with one signature in the window, so the only
   * variable is what the portfolio fetch does.
   */
  function armPoll() {
    const getSignaturesForAddress = vi.fn().mockResolvedValue([
      { signature: SIG, err: null, slot: 100, blockTime: 1_000_000 },
    ]);
    const getParsedTransactions = vi.fn().mockResolvedValue([makeParsedTx()]);
    vi.mocked(shared.getConnection).mockReturnValue({
      getSignaturesForAddress,
      getParsedTransactions,
      getAccountInfo: mockGetAccountInfo,
    } as any);
    return { getSignaturesForAddress };
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockUpsert.mockResolvedValue({ error: null });
  });

  it('holds the cursor when the portfolio fetch fails transiently', async () => {
    armPoll();
    mockGetAccountInfo.mockRejectedValue(new Error('RPC 503 Service Unavailable'));

    const indexer = new NftIndexerPolling();
    await asPrivate(indexer).indexNftEventsForSlab(SLAB);

    // Cursor left unset → the next poll re-fetches this window and the NFT
    // ownership-transfer event gets another chance. Advancing here would put
    // the signature below the cursor forever.
    expect(asPrivate(indexer).lastSignature.get(SLAB)).toBeUndefined();
    expect(mockUpsert).not.toHaveBeenCalled();
  });

  it('advances the cursor on a DETERMINISTIC skip (portfolio absent)', async () => {
    // The other half. Holding the cursor for a permanent condition would spin
    // the same window forever — re-fetching cannot make the account exist.
    armPoll();
    mockGetAccountInfo.mockResolvedValue(null);

    const indexer = new NftIndexerPolling();
    await asPrivate(indexer).indexNftEventsForSlab(SLAB);

    expect(asPrivate(indexer).lastSignature.get(SLAB)).toBe(SIG);
    expect(mockUpsert).not.toHaveBeenCalled();
  });

  it('advances the cursor on a successful index', async () => {
    armPoll();
    mockGetAccountInfo.mockResolvedValue({ data: makePortfolioData(SLAB) });

    const indexer = new NftIndexerPolling();
    await asPrivate(indexer).indexNftEventsForSlab(SLAB);

    expect(asPrivate(indexer).lastSignature.get(SLAB)).toBe(SIG);
    expect(mockUpsert).toHaveBeenCalled();
  });

  it('retries the portfolio fetch instead of giving up on the first blip', async () => {
    // The bare getAccountInfo had no retry wrapper — every other RPC call in
    // the indexer has one. A single blip should not cost the event at all.
    armPoll();
    // NOT mockImplementationOnce: indexNftEventsForSlab's own
    // getSignaturesForAddress call goes through withRetry first and would
    // consume it, leaving the portfolio fetch on the no-retry default.
    vi.mocked(shared.withRetry).mockImplementation(async (fn: any) => {
      try {
        return await fn();
      } catch {
        return await fn(); // one retry, mirroring withRetry's real contract
      }
    });
    mockGetAccountInfo
      .mockRejectedValueOnce(new Error('RPC 503 Service Unavailable'))
      .mockResolvedValue({ data: makePortfolioData(SLAB) });

    const indexer = new NftIndexerPolling();
    await asPrivate(indexer).indexNftEventsForSlab(SLAB);

    expect(mockUpsert).toHaveBeenCalled();
    expect(asPrivate(indexer).lastSignature.get(SLAB)).toBe(SIG);
  });
});
