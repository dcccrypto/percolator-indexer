import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

/**
 * Tests for src/network.ts
 *
 * CURRENT_NETWORK is evaluated at module-load time, so we must set
 * process.env.NETWORK before each dynamic import and bust the module
 * cache between tests with vi.resetModules().
 */
describe("network", () => {
  const originalNetwork = process.env.NETWORK;

  beforeEach(() => {
    vi.resetModules();
    delete process.env.NETWORK;
  });

  afterEach(() => {
    if (originalNetwork !== undefined) {
      process.env.NETWORK = originalNetwork;
    } else {
      delete process.env.NETWORK;
    }
  });

  async function loadNetwork() {
    const mod = await import("../src/network.js");
    return mod as { CURRENT_NETWORK: string };
  }

  describe("defaults", () => {
    it('should default to "devnet" when NETWORK is not set', async () => {
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });

    it('should default to "devnet" when NETWORK is empty string', async () => {
      process.env.NETWORK = "";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });
  });

  describe("valid values", () => {
    it('should resolve "devnet"', async () => {
      process.env.NETWORK = "devnet";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });

    it('should resolve "testnet"', async () => {
      process.env.NETWORK = "testnet";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("testnet");
    });

    it('should resolve "mainnet"', async () => {
      process.env.NETWORK = "mainnet";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("mainnet");
    });
  });

  describe("case insensitivity", () => {
    it('should resolve "MAINNET" to "mainnet"', async () => {
      process.env.NETWORK = "MAINNET";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("mainnet");
    });

    it('should resolve "Testnet" to "testnet"', async () => {
      process.env.NETWORK = "Testnet";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("testnet");
    });
  });

  describe("whitespace trimming", () => {
    it("should trim spaces around value", async () => {
      process.env.NETWORK = "  mainnet  ";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("mainnet");
    });

    it('should default to "devnet" for whitespace-only value', async () => {
      process.env.NETWORK = "   ";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });
  });

  describe("invalid values throw at import", () => {
    it("should throw for unrecognized value", async () => {
      process.env.NETWORK = "staging";
      await expect(loadNetwork()).rejects.toThrow(/Unknown NETWORK value "staging"/);
    });

    it("should throw for numeric value", async () => {
      process.env.NETWORK = "12345";
      await expect(loadNetwork()).rejects.toThrow(/Unknown NETWORK value "12345"/);
    });

    // M22: the message must quote the value exactly as set (case and whitespace kept),
    // not the normalised one, so an operator sees what is really in the env.
    it("names the raw value, case and whitespace kept, in the error", async () => {
      process.env.NETWORK = " Staging\n";
      const err = await loadNetwork().then(() => null, (e: unknown) => e);
      expect(err).toBeInstanceOf(Error);
      expect((err as Error).message).toContain(JSON.stringify(" Staging\n"));
    });
  });

  // M05: secret stores and shell files leave \n or \r\n behind. Every kind of whitespace is
  // trimmed (as shared's boot validator does), not only ASCII spaces.
  describe("any whitespace is trimmed", () => {
    it.each([
      ["\tmainnet\n", "mainnet"],
      ["testnet\r\n", "testnet"],
      ["\n", "devnet"],
      ["\t \r\n", "devnet"],
    ])("%j -> %s", async (value, want) => {
      process.env.NETWORK = value;
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe(want);
    });
  });

  // M18: only NETWORK picks the cluster. With NETWORK unset, every other cluster-like
  // variable is ignored and the devnet default holds.
  describe("only NETWORK is read", () => {
    afterEach(() => vi.unstubAllEnvs());

    it("ignores other cluster env vars when NETWORK is unset", async () => {
      for (const k of ["SOLANA_NETWORK", "SOLANA_CLUSTER", "CLUSTER", "NETWORK_NAME", "NEXT_PUBLIC_DEFAULT_NETWORK", "NEXT_PUBLIC_NETWORK", "DEFAULT_NETWORK"]) {
        vi.stubEnv(k, "mainnet");
      }
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });
  });

  // M21: CURRENT_NETWORK is read once at import and never changes for the life of the
  // module, even if NETWORK changes later in the process.
  describe("fixed at import", () => {
    it("a later NETWORK change is not picked up", async () => {
      process.env.NETWORK = "testnet";
      const mod = await loadNetwork();
      process.env.NETWORK = "mainnet";
      await new Promise((r) => setTimeout(r, 20));
      expect(mod.CURRENT_NETWORK).toBe("testnet");
    });
  });
});
