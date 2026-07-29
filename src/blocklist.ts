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
