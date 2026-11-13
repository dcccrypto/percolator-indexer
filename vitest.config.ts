import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 10_000,
    // Unit tests run against fakes: no pacing between RPC calls (the limiter itself is tested in tests/rpc-limiter.test.ts).
    env: { TRADE_RPC_MIN_INTERVAL_MS: "0" },
  },
});
