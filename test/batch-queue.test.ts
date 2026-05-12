import {afterEach, describe, expect, it} from "vitest";
import {BatchQueue} from "../src";
import {cleanupQueue, makeQueue, makeWorker, sleep, uniqueName, waitFor} from "./helpers";

interface Item {value: number}

describe("BatchQueue — main cases", () => {
    let queue: BatchQueue<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await queue?.close();
        await cleanupQueue(name);
        queue = null;
    });

    it("size trigger: filling maxBatchSize flushes immediately and returns the job", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 3, maxBatchWaitMs: 10_000});

        const j1 = await queue.add("batch", {value: 1});
        const j2 = await queue.add("batch", {value: 2});
        const j3 = await queue.add("batch", {value: 3});

        // Only the 3rd add (which fills the batch) creates a job.
        expect(j1).toBeUndefined();
        expect(j2).toBeUndefined();
        expect(j3).toBeDefined();
        expect(j3!.data).toEqual([{value: 1}, {value: 2}, {value: 3}]);
    });

    it("time trigger: items below maxBatchSize flush after maxBatchWaitMs", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 100, maxBatchWaitMs: 150, pollInterval: 30});

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            await queue.add("batch", {value: 10});
            await queue.add("batch", {value: 20});

            await waitFor(() => jobs.length > 0, 3000, "time-flush job arrival");
            expect(jobs).toHaveLength(1);
            expect(jobs[0].data).toEqual([{value: 10}, {value: 20}]);
        } finally {
            await worker.close();
        }
    });

    it("minBatchSize gates time-triggered flush", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 50,
            minBatchSize: 5,
            maxBatchWaitMs: 80,
            pollInterval: 25,
        });

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            // Stage 2 items — below minBatchSize. Even after the timeout
            // elapses several times, no flush should happen.
            await queue.add("batch", {value: 1});
            await queue.add("batch", {value: 2});
            await sleep(400);
            expect(jobs).toHaveLength(0);

            // Push past minBatchSize; next poll iteration should flush.
            await queue.add("batch", {value: 3});
            await queue.add("batch", {value: 4});
            await queue.add("batch", {value: 5});

            await waitFor(() => jobs.length > 0, 3000, "min-gated flush");
            expect(jobs[0].data).toHaveLength(5);
        } finally {
            await worker.close();
        }
    });

    it("manual flush() emits whatever is staged, ignoring minBatchSize/timeout", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            minBatchSize: 10,
            maxBatchWaitMs: 60_000,
        });

        await queue.add("batch", {value: 7});
        await queue.add("batch", {value: 8});

        const job = await queue.flush("manual");
        expect(job).toBeDefined();
        expect(job!.data).toEqual([{value: 7}, {value: 8}]);
        expect(job!.name).toBe("manual");
    });

    it("manual flush() returns undefined when staging is empty", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name);
        const job = await queue.flush();
        expect(job).toBeUndefined();
    });

    it("addBulk produces the right number of batches for size > maxBatchSize", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 3, maxBatchWaitMs: 10_000});

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            // 10 items with size=3 → expect 3 size-triggered batches + 1
            // leftover that will time-flush. We force the leftover via flush().
            await queue.addBulk("batch", Array.from({length: 10}, (_, i) => ({value: i})));
            await queue.flush();

            await waitFor(() => jobs.length === 4, 3000, "addBulk batches");
            const totals = jobs.flatMap((j) => j.data).map((d) => d.value);
            expect(totals.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
            // No batch exceeds maxBatchSize.
            for (const j of jobs) expect(j.data.length).toBeLessThanOrEqual(3);
        } finally {
            await worker.close();
        }
    });
});
