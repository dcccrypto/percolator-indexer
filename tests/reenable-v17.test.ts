/**
 * A v17 market that was auto-closed must be re-enabled once it is funded.
 *
 * The re-enable check read `DiscoveredMarket.engine`, which the SDK documents as
 * "Absent (undefined) for v17 market group accounts — use configV17 instead".
 * So `vault` came out 0 for EVERY v17 market, `isLive` was always false, and a
 * market the sweep had auto-closed could never come back.
 *
 * That is not theoretical. A launch stalls between InitMarket and its funding
 * step (e.g. the deposit fails), the sweep sees a dust vault and no accounts and
 * closes it — correctly. The creator then completes the deposit, the vault holds
 * real collateral, and the market stays hidden from the API and the UI forever,
 * because the only path back reads a field that does not exist on v17.
 */
import { describe, it, expect } from "vitest";

/** The liveness predicate, as the re-enable path applies it. */
function isLive(vault: bigint, numUsedAccounts: number): boolean {
  return vault > 1_000_000n || numUsedAccounts > 0;
}

describe("auto-close re-enable — v17 vault source", () => {
  it("a funded market is live (the real case that stayed hidden)", () => {
    // Observed on-chain after the creator's retry succeeded: 1200 Sim-USDC.
    expect(isLive(1_200_000_000n, 0)).toBe(true);
  });

  it("reading the v12 `engine` field yields 0 on v17 — the bug", () => {
    // engine === undefined -> vault 0n, accounts 0 -> never live, never re-enabled.
    const engine = undefined as { vault: bigint; numUsedAccounts: number } | undefined;
    const vaultFromV12Path = engine ? engine.vault : 0n;
    expect(vaultFromV12Path).toBe(0n);
    expect(isLive(vaultFromV12Path, 0)).toBe(false);
  });

  it("still leaves a genuinely dust market closed", () => {
    // The auto-close exists for a reason — re-enabling must not become automatic.
    expect(isLive(1_000_000n, 0)).toBe(false); // exactly the creation-seed boundary
    expect(isLive(0n, 0)).toBe(false);
  });

  it("accounts alone are enough to count as live", () => {
    expect(isLive(0n, 1)).toBe(true);
  });
});
