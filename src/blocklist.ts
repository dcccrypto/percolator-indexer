import { createLogger } from "@percolatorct/shared";

const logger = createLogger("indexer:blocklist");

/**
 * Slabs the indexer must ignore completely.
 *
 * WHY THIS EXISTS
 * ---------------
 * The indexer discovers markets from chain and auto-registers anything it finds
 * (StatsCollector.syncMarkets). That is the right default, but it means deleting
 * a retired market from the database does NOT retire it: the next discovery
 * cycle sees the slab still on chain and inserts the row straight back, usually
 * within a minute. The frontend's own blocklist only hides rows from the API —
 * it does not stop them being recreated here.
 *
 * So retiring a market takes both halves: this list stops the indexer
 * re-registering it, and lib/blocklist.ts stops the frontend showing it.
 *
 * Blocked slabs are skipped for BOTH registration and trade ingest. Ingest
 * matters as much as registration: trades carry an FK to markets(slab_address),
 * so a fill on a deleted market would fail the insert anyway — this turns that
 * into a clean skip instead of a retry-then-error on every delivery.
 *
 * KEEP IN SYNC with percolator-launch/app/lib/blocklist.ts.
 */

/**
 * The devnet-1 lineup plus every slab created during the devnet-2.0 launch
 * testing on 2026-07-26..29. All were created before the launch wizard applied
 * real LP guardrails and a sane price-move budget, so their on-chain matcher
 * params are write-once wrong (the matcher has no update instruction) — they
 * can only be replaced, never repaired.
 */
const BLOCKED: readonly string[] = [
  // devnet-1 lineup
  "gHey79gB1xGQyXne8yEHoKmGi6jrEVigLwxSXQrYkD3",
  "7FBXdrm1vQ4ktQJjMwurq4cAHkVB1gKoZ7Hx3CAQv6P4",
  "8SHhSKuY9cun15Y2Q9p9SNEV86zzSWbeP4e59xLAv99h",
  "BLAHwD5wZ3Wo6naHD4GTT6zpYFcyLWAviEWR4zT7C36p",
  "BPgSUbDsxZ9bkauWgd6eQ8oLHVx6pSsvfAjPGsS2Sso8",
  "CseeeuKKbgNU38VRukG38mTdcPJ4KWci5GmFikEtp1X5",
  // devnet-2.0 launch-testing slabs (2026-07-27..29)
  "D6QgPGvo5KGFzYCuzk4U9tDm6UbEpCrjWwu9SGQTfQeU",
  "GzQCM1DLMDXkbX85kVB2Un12aKc62ZRN5RdKGjqnNsbX",
  "4gM5qkkmsSqnBXHXtbM4pqGZ36sheo3cYL1jpdDfsJrS",
  "BgWFGPgNasesbiihEhadYuDdHAckSTu6AMvEBBkrdfmn",
  "GHCLa7oMUZo7qTwV8YH5RrPJGHG7z9sZ3y19dAAsgE2e",
  "54Bbsy7q5L5LhusWkKeCon7StywWa8Vezb5zw5pfBo2o",
  "CZJHRKQMHNUVy2muC7iojovnTgGtyVnpjCk1QpqheUZ5",
  "EuYE6qaNic3KhaRAtB9cM5YG62Z88dTcu3YQJNkKZQ3F",
  "43zufCcajU6H8ySqjG9vDcwFCbjGPhRCSJJAEf3E6Hiw",
  "14RFDSTK6eJ3VKprgfAafU3kqgYRVCATMV7f5Ukf2pzh",
  "2Md6RJoxQG89bKh7uzhhcHqwAmgGTJN9PN7EkZt1ZTPC",
  "XxCeVcNDHqEuB7GDx6zMPKN5iwvskPYAJgpy51TLuy6",
  "DE2c59suA6NVxRMHvhEhaWJxtBQu8XSMB2CM8wxwyoT7",
  "EtgRphLa69F15krir2E1kZL6LCCuQHDS9Cher3hmYunJ",
  "5kSw1fX8Ps2kBkVU4bc1qHgUQ8AKFXHkqoq2u2ztcdJs",
  // 2026-07-29 launch-verification markets. Both funded and wrapper-owned, but
  // both seeded with insurance = 0 — the step-4 idempotency check mistook the
  // backing-bucket collateral for the insurance seed and skipped TopUpInsurance
  // (tag 9 appears in neither launch). Insurance is write-once at creation.
  "H9ey1RBnVoBBit2o7EUCPZWJLMNtQpuA6QiqGmM95ZJ4",  // FRANK
  "4hJ9hUotH6BwUXVmgLGmXWHfg3YLjnmA8fwAtjex3wBU",  // Percolator
  // 2026-07-30 clean slate: every market still live on the current wrapper,
  // retired together so the board starts empty. These were all created before
  // registration was consolidated onto the markets row, so none of them has a
  // creator-written row — the indexer discovered each slab on chain and filled
  // in a placeholder (metadata_source='auto', symbol='UNKNOWN', no pool
  // binding), because POST /api/markets was sending oracle_mode='keeper' into a
  // column whose CHECK constraint only accepts pyth|hyperp|admin and failed on
  // every single keeper-oracle launch. Relaunch on the fixed path instead of
  // repairing rows that were never written by their creator.
  "5sDvEs2Zwn42ESkAmQm6Ycvi1XC3X8zHhhTDX1FX3hT7",  // Fauci
  "5xRkBU83ogswJnjzqMb1a2M41NczMzyajSLvrVAsAG9Z",  // ZERO
  "3bGWBK25iHH4FusT2c7JS7VjKxghtEHLWxXpLQarwRf3",  // TripleT-PERP
  "2DDBehzGAKJPzwZXZ9HbcHBEtdkoHPRPaGBDjMCqSAUv",  // unnamed
  "FaNFCmyputbCTvSGGmxe7EU1DyjagtGKf6eYPDTvmdFC",  // unnamed
  "6RobABa7gpPvN8WsoQuXgbKKinpURwGXzUS4NJiYNaPR",  // unnamed

  // ── 2026-07-31: retired after the CATE LP drain (bankrupt LP, permanent
  // bankruptcy hlock blocks every close/withdraw; Percolator launched on the
  // same raw-spot feed the drain exploited — both replaced by markets created
  // on the median-smoothed AuthMark keeper) ──
  "6CFMN29zPgsczCQUjeZZiWxVPDUKN52RJqBLvRsTTERn",  // CATE (bankrupt LP, hlocked)
  "5erG74dYhhjhB3ReATJmFPh1XRb9MVnUjVgV4BSM21c1",  // Percolator (pre-smoother launch)

  // ── 2026-07-31 sync backfill: every app-blocklist entry the indexer copy
  // was missing (35 pre-2026-07-27 old-wrapper/phantom slabs). Mostly inert
  // today — discovery only scans current program ids — but the file's own
  // KEEP-IN-SYNC contract was violated and the safety silently depended on
  // allProgramIds never re-including an old wrapper. ──
  "BxJPaMaCfEGTBsjZ8wfj3Yfzf4wpasmxKAEvqZZRcGPP",
  "HjBePQZnoZVftg9B52gyeuHGjBvt2f8FNCVP4FeoP3YT",
  "H5Vunzd2yAMygnpFiGUASDSx2s8P3bfPTzjCfrRsPeph",
  "3bmCyPee8GWJR5aPGTyN5EyyQJLzYyD8Wkg9m1Afd1SD",
  "3YDqCJGz88xGiPBiRvx4vrM51mWTiTZPZ95hxYDZqKpJ",
  "3ZKKwsKoo5UP28cYmMpvGpwoFpWLVgEWLQJCejJnECQn",
  "CRJH9Gtk7qQDdjzDufnAZdfa7AHisfvxCmVVvzpzQN9v",
  "J6UU4VHbYXpCAACr5o5xjUVmquagiP2NGbbMp68VUCX9",
  "8L47yqvQRLxZ6PzW3b9jawEM79CmokBvUzeLR7mvtyuU",
  "8kkED3uZznGzSidr8kYJPd3VhzSh7LVngNUx2V1qnW9L",
  "8pKtAV3z6iTKekieF9EenQ4tk1rkAVa9oYsqe7h1PGjx",
  "Eekuz2TgXRPq3rsp5brRW5hofxLdwt6KUXbLUQCKHK9G",
  "Av3zVrW5deLpLo1qZZ7yNJ5Lq5ja4Z9ixijVhV4MuRzE",
  "CrbDmfiooBUTFfGyMhJ1hpToCrBLAXXKySBwEnLHV6kj",
  "FhpPmmuh5UDAjvEjrYBPFwmj4CP4otvsYMxtTb46p1Ss",
  "7xozYEbKhEdjQn5pCAV8bUDQGugZttqZTduPeHkoqRb8",
  "3dp3e288oPjs5w92fg26cVYQMHGuUpsj8YbSFn6wrzp4",
  "8nzjXMvdkC4fRF491QkpKE6aFTLmEcpXEnbh4wQT4iUA",
  "3bmCyPeeDwAfLbhfnRpYJHkWVqAf3Q5JaWXGfZjbmjNp",
  "8eFFEFBY3HHbBgzxJJP5hyxdzMNMAumnYNhkWXErBM4c",
  "FLF9ghf6H4sfSexcQzDwse4gcGZKPb6qYCqo5Btat98",
  "8NY7rvQJXNTinJkAQG1GUV8NQ1hQzdtF7iWNjK9p7tQN",
  "9TGSmPLTLMii4UqstL629twGeVJ9Ndr8VD3pexnvQTsV",
  "5Rdxh3n4CbLEpzovbMtUJ7M3iaZkoso8jGdfVwkv2eV8",
  "GRAgHm9utZy6kWJj1ZpAVntbyFxCBJyZJ1nSJmiMPPpq",
  "6QSHWb4Vm1M6f1r14t1jB7Jc4en2uieQuLpKqey71Y2S",
  "4txSGha4zABqt2NUbBtbkzv3vA4rfi9J6Yr95adA4fc5",
  "DxrZXhTC11gCVtv4b2nkbszScgZPqm9DFqit5X7FvsF7",
  "7mzqfnuAhANvDV8PiqJBG3jehyv3rPrCMr9V6j2bCHPV",
  "Fs13SX1b33wRh3DBbh1NmkuHSz5Z89oRb2ew7aNn1jMH",
  "J9unPVyDykcoQyxGxF1MfSE6mGyaaCfZhGEAk5eQokXG",
  "8WNAuxLDvo3S5Yf9Z5sm2me69N4d1RLvxoS1tCnPpo83",
  "DeWGMtVo8VHjUJ5qsPXSZsQS9rFJhnB3gE4tPGWrEcCB",
  "dLKhJAVPgmgxJJWvbcGvfQUNBmc7wwjdQp8Jzpg4UGq",
  "9oBMLGXq9mLGa5DQapTL2gia9eM425dNvf4DUNoMrzz6",

  // ── 2026-08-01: the last two slabs on the current wrapper, completing the
  // clean slate. Both are small-tier (8538B) shells abandoned 2026-07-22 with
  // vault=0, insurance=0, c_tot=0 — no user funds, never traded. With these
  // every market on wrapper DhSkE7uT… (33 total) is retired, so the board is
  // empty from discovery down and only newly-launched markets can appear.
  "78enGzvjkwfnbTMsgXEt4jVckFQFkGZisggADc8gCN8W",
  "CZBmJF8mixe3pyw1sELaxJWEabhqxGdPhrf6UNzaQVoe",
];

/**
 * Extra slabs from the environment, comma-separated, so a market can be retired
 * without a redeploy. Merged with the list above rather than replacing it.
 */
function fromEnv(): string[] {
  const raw = process.env.INDEXER_BLOCKED_SLABS;
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

const blocked = new Set<string>([...BLOCKED, ...fromEnv()]);

if (blocked.size !== BLOCKED.length) {
  logger.info("Blocklist extended from INDEXER_BLOCKED_SLABS", {
    builtin: BLOCKED.length,
    total: blocked.size,
  });
}

/** True when the indexer must ignore this slab entirely. */
export function isBlockedSlab(slabAddress: string | null | undefined): boolean {
  return slabAddress != null && blocked.has(slabAddress);
}

/** Current blocklist size — for startup logging. */
export function blockedSlabCount(): number {
  return blocked.size;
}
