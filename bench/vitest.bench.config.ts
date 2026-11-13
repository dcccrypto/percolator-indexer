import { defineConfig } from "vitest/config";

// Replay benches (not part of `pnpm test`): npx vitest run --config bench/vitest.bench.config.ts
export default defineConfig({
  test: {
    include: ["bench/**/*.bench.test.ts"],
    testTimeout: 600_000,
  },
});
