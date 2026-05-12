import {describe, expect, it} from "vitest";
import {rawClient} from "./helpers";

describe("redis cleanup", () => {
    it("no batch keys remain after the full suite", async () => {
        const client = rawClient();
        try {
            const keys = await client.keys("bull-batch:*");
            expect(keys, `orphaned keys left in redis:\n${keys.join("\n")}`).toEqual([]);
        } finally {
            await client.quit();
        }
    });
});
