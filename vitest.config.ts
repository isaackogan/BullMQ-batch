import {defineConfig} from "vitest/config";

export default defineConfig({
    test: {
        include: ["test/**/*.test.ts"],
        testTimeout: 15_000,
        hookTimeout: 15_000,
        // Tests share a single Redis. Each test scopes its keys via a
        // randomized queue name. File parallelism is disabled so the
        // alphabetically-last `zz-cleanup.test.ts` runs after everything
        // else and can assert Redis is empty.
        pool: "forks",
        fileParallelism: false,
    },
});
