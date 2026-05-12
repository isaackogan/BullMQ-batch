import {afterEach, describe, expect, it} from "vitest";
import {BatchQueue} from "../src";
import {cleanupQueue, makeQueue, makeWorker, rawClient, sleep, uniqueName, waitFor} from "./helpers";

interface Item {value: number}

/**
 * Faking a crashed flusher: write a pending HASH entry directly with an
 * "old enough" timestamp so the recovery threshold considers it orphaned.
 */
async function plantOrphan(
    queueName: string,
    items: Item[],
    ageMs: number,
): Promise<void> {
    const client = rawClient();
    try {
        const pendingKey = `bull-batch:${queueName}:batch:pending`;
        const ts = Date.now() - ageMs;
        const value = JSON.stringify({
            ts,
            items: items.map((i) => JSON.stringify(i)),
        });
        await client.hset(pendingKey, "ghost:1", value);
    } finally {
        await client.quit();
    }
}

describe("BatchQueue — fault tolerance", () => {
    let queue: BatchQueue<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await queue?.close();
        await cleanupQueue(name);
        queue = null;
    });

    it("recover() re-stages items from an orphaned pending entry", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 60_000,
            recoveryThresholdMs: 200,
            recoveryInterval: 60_000, // disable auto-recovery for this test
        });

        await plantOrphan(name, [{value: 100}, {value: 200}], 1000);

        const recovered = await queue.recover();
        expect(recovered).toBe(2);

        // After recovery the items are back in staging — force-flush to read.
        const job = await queue.flush();
        expect(job).toBeDefined();
        expect(job!.data).toEqual([{value: 100}, {value: 200}]);
    });

    it("recover() leaves entries inside the threshold untouched", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            recoveryThresholdMs: 5000,
            recoveryInterval: 60_000,
        });

        // Pending entry only 100ms old — well under the 5s threshold.
        await plantOrphan(name, [{value: 1}], 100);

        const recovered = await queue.recover();
        expect(recovered).toBe(0);
    });

    it("recover() returns 0 when there are no pending entries", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name);

        const recovered = await queue.recover();
        expect(recovered).toBe(0);
    });

    it("recovery poller auto-fires: orphaned items eventually appear as a batch job", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 80,
            pollInterval: 30,
            recoveryThresholdMs: 100,
            recoveryInterval: 50,
        });

        await plantOrphan(name, [{value: 11}, {value: 22}], 500);

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            await waitFor(() => jobs.length > 0, 4000, "recovered batch");
            expect(jobs[0].data).toEqual([{value: 11}, {value: 22}]);
        } finally {
            await worker.close();
        }
    });

    it("two BatchQueue instances racing on the same staging never double-flush", async () => {
        name = uniqueName();
        const opts = {maxBatchSize: 5, maxBatchWaitMs: 60_000};
        const q1 = makeQueue<Item>(name, opts);
        const q2 = makeQueue<Item>(name, opts);

        try {
            // Interleave adds across both producers. The size trigger fires
            // exactly once — every item appears in exactly one batch.
            const adds = [
                q1.add("batch", {value: 1}),
                q2.add("batch", {value: 2}),
                q1.add("batch", {value: 3}),
                q2.add("batch", {value: 4}),
                q1.add("batch", {value: 5}),
                q2.add("batch", {value: 6}),
                q1.add("batch", {value: 7}),
                q2.add("batch", {value: 8}),
                q1.add("batch", {value: 9}),
                q2.add("batch", {value: 10}),
            ];
            const results = await Promise.all(adds);

            const jobs = results.filter((j) => j !== undefined);
            // 10 items with size=5 ⇒ exactly two size-triggered jobs.
            expect(jobs).toHaveLength(2);

            const flat = jobs.flatMap((j) => j!.data).map((d) => d.value);
            expect(flat.sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        } finally {
            await q1.close();
            await q2.close();
            // sharing a name → only cleanup once
            await cleanupQueue(name);
        }
    });

    it("staging survives a queue close+reopen (items aren't lost)", async () => {
        name = uniqueName();
        const q1 = makeQueue<Item>(name, {maxBatchSize: 100, maxBatchWaitMs: 60_000});

        await q1.add("batch", {value: 42});
        await q1.add("batch", {value: 43});
        await q1.close();

        // Fresh instance against the same name — staging persists in Redis.
        const q2 = makeQueue<Item>(name, {maxBatchSize: 100, maxBatchWaitMs: 60_000});
        try {
            const job = await q2.flush();
            expect(job).toBeDefined();
            expect(job!.data).toEqual([{value: 42}, {value: 43}]);
        } finally {
            await q2.close();
            await cleanupQueue(name);
        }
    });
});
