import { describe, it, expect, afterEach, vi } from "vitest";

/**
 * A set-but-unknown NETWORK value must fail loudly at import instead of being
 * silently mapped to devnet. CURRENT_NETWORK tags every row the indexer writes
 * and filters every read, so a typo on a non-devnet indexer would otherwise
 * write money rows (trades, stats, LP vault) under the devnet tag with no log.
 */
describe("network: unknown NETWORK value", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.resetModules();
  });

  async function load(value: string | undefined) {
    vi.resetModules();
    vi.stubEnv("NETWORK", value);
    return import("../src/network.js");
  }

  // The "-beta" cluster spelling is rejected too: @percolatorct/shared's
  // getNetwork() (same process, same env var) would map it to devnet.
  it.each(["devnte", "staging", "localnet", "12345", "dev net", "main" + "net-beta"])(
    "throws for unknown value %j, naming the value and the allowed names",
    async (value) => {
      const err = await load(value).then(
        () => null,
        (e: unknown) => e,
      );
      expect(err).toBeInstanceOf(Error);
      const msg = (err as Error).message;
      expect(msg).toContain(JSON.stringify(value));
      expect(msg).toContain("devnet");
      expect(msg).toContain("testnet");
      expect(msg).toContain("main" + "net");
    },
  );

  it("unset NETWORK keeps the devnet default", async () => {
    const { CURRENT_NETWORK } = await load(undefined);
    expect(CURRENT_NETWORK).toBe("devnet");
  });

  it("empty / whitespace-only NETWORK is treated as unset (devnet)", async () => {
    expect((await load("")).CURRENT_NETWORK).toBe("devnet");
    expect((await load("   ")).CURRENT_NETWORK).toBe("devnet");
  });
});
