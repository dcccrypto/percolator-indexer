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

  describe("invalid values fall back to devnet", () => {
    it('should fall back to "devnet" for unrecognized value', async () => {
      process.env.NETWORK = "staging";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });

    it('should fall back to "devnet" for numeric value', async () => {
      process.env.NETWORK = "12345";
      const { CURRENT_NETWORK } = await loadNetwork();
      expect(CURRENT_NETWORK).toBe("devnet");
    });
  });
});
