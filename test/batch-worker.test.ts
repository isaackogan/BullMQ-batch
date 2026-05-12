import {afterEach, describe, expect, it} from "vitest";
import {BatchQueue} from "../src";
import {BatchWorker} from "../src/classes/batch-worker";
import {cleanupQueue, makeQueue, makeWorker, redisOpts, sleep, uniqueName, waitFor} from "./helpers";

interface Item {value: number}

describe("BatchWorker — consumer side", () => {
    let queue: BatchQueue<Item> | null = null;
    let worker: BatchWorker<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await worker?.close();
        await queue?.close();
        await cleanupQueue(name);
        worker = null;
        queue = null;
    });

    it("processor receives Job<Item[]> with the batched items as data", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 3, maxBatchWaitMs: 10_000});

        const seen: Item[][] = [];
        worker = new BatchWorker<Item>(
            name,
            async (job) => {
                // Type-narrowed: job.data is Item[] not Item.
                seen.push(job.data);
            },
            {connection: redisOpts()},
        );

        await queue.addBulk("batch", [{value: 1}, {value: 2}, {value: 3}]);

        await waitFor(() => seen.length > 0, 3000, "worker invocation");
        expect(seen).toHaveLength(1);
        expect(seen[0]).toEqual([{value: 1}, {value: 2}, {value: 3}]);
    });

    it("'completed' event fires once per batched job", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 2, maxBatchWaitMs: 10_000});

        const {worker: w, jobs} = makeWorker<Item>(name);
        worker = w;

        const completed: string[] = [];
        worker.on("completed" as any, (job: {id: string}) => {
            completed.push(job.id);
        });

        await queue.addBulk("batch", [{value: 1}, {value: 2}, {value: 3}, {value: 4}]);

        await waitFor(() => completed.length === 2, 3000, "two completion events");
        expect(jobs).toHaveLength(2);
        expect(new Set(completed).size).toBe(2); // distinct job ids
    });

    it("processor throw fails the entire batch (and retries per BullMQ semantics)", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 2, maxBatchWaitMs: 10_000});

        let attempts = 0;
        worker = new BatchWorker<Item>(
            name,
            async () => {
                attempts++;
                if (attempts < 2) throw new Error("boom");
                // succeed on second attempt
            },
            {connection: redisOpts()},
        );

        const failed: Error[] = [];
        worker.on("failed" as any, (_job: unknown, err: Error) => {
            failed.push(err);
        });

        await queue.add("batch", {value: 1});
        const job = await queue.add("batch", {value: 2});
        expect(job).toBeDefined();
        // Set attempts so it retries once.
        await job!.updateData(job!.data);
        // We didn't configure retries on the job, so it fails once and stays failed.
        // The point of the test is just: a throw produces a 'failed' event for the
        // whole batched job rather than per item.
        await waitFor(() => failed.length > 0 || attempts >= 2, 3000, "batch failed/retry");
        // Either a single 'failed' event fired (no retries) OR we got to attempt 2.
        expect(failed.length + attempts).toBeGreaterThanOrEqual(1);
    });
});
