import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 120_000,      // 2 min per test (integration tests are slow)
    hookTimeout: 60_000,       // 1 min for setup/teardown
    pool: "forks",             // isolate tests in separate processes
    poolOptions: {
      forks: { maxForks: 1 },  // sequential — tests share external state
    },
    env: {
      NODE_OPTIONS: "--experimental-strip-types",
    },
  },
});
