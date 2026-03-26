import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 120_000,      // 2 min per test (integration tests are slow)
    hookTimeout: 60_000,       // 1 min for setup/teardown
    fileParallelism: false,    // run test FILES sequentially (they share DBs)
    env: {
      NODE_OPTIONS: "--experimental-strip-types",
    },
  },
});
