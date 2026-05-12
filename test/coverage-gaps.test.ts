import {afterEach, describe, expect, it} from "vitest";
import {Queue} from "bullmq";
import {BatchQueue, BatchWorker} from "../src";
import {
    cleanupQueue,
    makeQueue,
    makeWorker,
    rawClient,
    redisOpts,
    sleep,
    uniqueName,
    waitFor,
} from "./helpers";

interface Item {value: number}

describe("prefix & namespacing", () => {
    let name = "";
    let batchQueue: BatchQueue<Item> | null = null;
    let plainQueue: Queue | null = null;
    let worker: BatchWorker<Item> | null = null;

    afterEach(async () => {
        await worker?.close();
        await batchQueue?.close();
        await plainQueue?.close();
        await cleanupQueue(name);
        const client = rawClient();
        try {
            const stray = await client.keys(`bull:${name}:*`);
            if (stray.length) await client.unlink(...stray);
            const customPrefix = await client.keys(`myapp:*:${name}:*`);
            if (customPrefix.length) await client.unlink(...customPrefix);
        } finally {
            await client.quit();
        }
        worker = null;
        batchQueue = null;
        plainQueue = null;
    });

    it("BatchQueue/BatchWorker keys are isolated from a plain BullMQ Queue sharing the same name", async () => {
        name = uniqueName();

        // Plain BullMQ Queue with the default "bull" prefix.
        plainQueue = new Queue(name, {connection: redisOpts()});
        await plainQueue.add("regular", {value: 999});

        // BatchWorker on the same name should NOT see the plain Queue's job —
        // its prefix is "bull-batch", not "bull".
        const seen: Item[][] = [];
        worker = new BatchWorker<Item>(
            name,
            async (job) => {seen.push(job.data);},
            {connection: redisOpts()},
        );

        // Wait long enough that any cross-talk would surface.
        await sleep(500);
        expect(seen).toEqual([]);

        // And the plain Queue's job is still waiting on its own keyspace.
        const counts = await plainQueue.getJobCounts("wait", "active");
        expect(counts.wait + counts.active).toBe(1);
    });

    it("custom `prefix` option propagates into the bull-batch keyspace", async () => {
        name = uniqueName();
        batchQueue = new BatchQueue<Item>(name, {
            connection: redisOpts(),
            prefix: "myapp",
            batch: {maxBatchSize: 2, maxBatchWaitMs: 60_000},
        });

        await batchQueue.add("batch", {value: 1});

        const client = rawClient();
        try {
            const keys = await client.keys(`myapp:bull-batch:${name}:*`);
            expect(keys.length).toBeGreaterThan(0);
            // Nothing should leak into the default bull-batch namespace.
            const defaultKeys = await client.keys(`bull-batch:${name}:*`);
            expect(defaultKeys).toEqual([]);
        } finally {
            await client.quit();
        }
    });
});

describe("jobName configuration", () => {
    let queue: BatchQueue<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await queue?.close();
        await cleanupQueue(name);
        queue = null;
    });

    it("time-triggered flush uses configured batch.jobName", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 80,
            pollInterval: 25,
            jobName: "custom-name",
        });

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            await queue.add("ignored-by-time-flush", {value: 1});
            await queue.add("ignored-by-time-flush", {value: 2});

            await waitFor(() => jobs.length > 0, 3000, "time-flush job");

            const job = await queue.queue.getJob(jobs[0].jobId!);
            expect(job?.name).toBe("custom-name");
        } finally {
            await worker.close();
        }
    });

    it("size-triggered flush uses the name passed to add()", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 2,
            maxBatchWaitMs: 60_000,
            jobName: "default-name",
        });

        await queue.add("size-name", {value: 1});
        const job = await queue.add("size-name", {value: 2});
        expect(job?.name).toBe("size-name");
    });

    it("default batch.jobName is 'batch'", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 60,
            pollInterval: 20,
        });

        const {worker, jobs} = makeWorker<Item>(name);
        try {
            await queue.add("ignored", {value: 1});
            await waitFor(() => jobs.length > 0, 3000, "time-flush");

            const job = await queue.queue.getJob(jobs[0].jobId!);
            expect(job?.name).toBe("batch");
        } finally {
            await worker.close();
        }
    });
});

describe("recovery edge cases", () => {
    let queue: BatchQueue<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await queue?.close();
        await cleanupQueue(name);
        queue = null;
    });

    async function plantOrphan(items: Item[], ageMs: number, field = "ghost:1"): Promise<void> {
        const client = rawClient();
        try {
            const pendingKey = `bull-batch:${name}:batch:pending`;
            const ts = Date.now() - ageMs;
            const value = JSON.stringify({
                ts,
                items: items.map((i) => JSON.stringify(i)),
            });
            await client.hset(pendingKey, field, value);
        } finally {
            await client.quit();
        }
    }

    it("concurrent recover() calls don't double-recover the same orphan", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 60_000,
            recoveryThresholdMs: 100,
            recoveryInterval: 60_000,
        });

        await plantOrphan([{value: 1}, {value: 2}, {value: 3}], 1000);

        const [a, b, c] = await Promise.all([queue.recover(), queue.recover(), queue.recover()]);
        const total = a + b + c;
        // Exactly one of the racers wins the HDEL; the other two see nothing.
        expect(total).toBe(3);

        // And the items are staged exactly once.
        const job = await queue.flush();
        expect(job!.data).toEqual([{value: 1}, {value: 2}, {value: 3}]);

        // Pending is empty now → another recover returns 0.
        expect(await queue.recover()).toBe(0);
    });

    it("recovery into non-empty staging appends recovered items to the tail", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 60_000,
            recoveryThresholdMs: 100,
            recoveryInterval: 60_000,
        });

        // Live items already in staging.
        await queue.add("batch", {value: 10});
        await queue.add("batch", {value: 20});

        // Orphan planted afterwards — old enough to be recovered.
        await plantOrphan([{value: 1}, {value: 2}], 1000);

        const recovered = await queue.recover();
        expect(recovered).toBe(2);

        const job = await queue.flush();
        // Recovered items are RPUSHed to the tail of the existing staging.
        expect(job!.data).toEqual([{value: 10}, {value: 20}, {value: 1}, {value: 2}]);
    });

    it("at-least-once: a committed job + an undeleted pending entry produces duplicate items after recovery", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {
            maxBatchSize: 100,
            maxBatchWaitMs: 60_000,
            recoveryThresholdMs: 100,
            recoveryInterval: 60_000,
        });

        // Simulate "queue.add succeeded, HDEL didn't": directly add the job
        // through the internal queue, AND leave a pending entry with the
        // same items.
        const items: Item[] = [{value: 7}, {value: 8}];
        const firstJob = await queue.queue.add("batch", items);
        await plantOrphan(items, 1000);

        // Recovery picks the orphan up, items re-stage.
        const recovered = await queue.recover();
        expect(recovered).toBe(2);

        // Force a flush → second job with the duplicated items.
        const secondJob = await queue.flush();

        // Both jobs exist, both carry the same payload — documented duplicate.
        expect(firstJob.data).toEqual(items);
        expect(secondJob!.data).toEqual(items);
        expect(secondJob!.id).not.toBe(firstJob.id);
    });
});

describe("misc edge cases", () => {
    let queue: BatchQueue<Item> | null = null;
    let name = "";

    afterEach(async () => {
        await queue?.close();
        await cleanupQueue(name);
        queue = null;
    });

    it("addBulk([]) is a no-op and does not produce a job", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 10, maxBatchWaitMs: 60_000});

        await queue.addBulk("batch", []);

        const job = await queue.flush();
        expect(job).toBeUndefined();
        expect(await queue.queue.getJobCounts("wait", "active", "completed")).toMatchObject({
            wait: 0, active: 0, completed: 0,
        });
    });

    it("maxBatchSize: 1 emits a job per add", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 1, maxBatchWaitMs: 60_000});

        const j1 = await queue.add("batch", {value: 1});
        const j2 = await queue.add("batch", {value: 2});
        const j3 = await queue.add("batch", {value: 3});

        expect(j1).toBeDefined();
        expect(j2).toBeDefined();
        expect(j3).toBeDefined();
        expect(j1!.data).toEqual([{value: 1}]);
        expect(j2!.data).toEqual([{value: 2}]);
        expect(j3!.data).toEqual([{value: 3}]);
    });

    it("worker concurrency > 1 processes multiple batches in parallel without double-processing", async () => {
        name = uniqueName();
        queue = makeQueue<Item>(name, {maxBatchSize: 2, maxBatchWaitMs: 60_000});

        const processed: Item[][] = [];
        let inFlight = 0;
        let maxInFlight = 0;

        const worker = new BatchWorker<Item>(
            name,
            async (job) => {
                inFlight++;
                maxInFlight = Math.max(maxInFlight, inFlight);
                await sleep(80);
                processed.push(job.data);
                inFlight--;
            },
            {connection: redisOpts(), concurrency: 4},
        );

        try {
            // 8 items, batch size 2 → 4 batches.
            await queue.addBulk("batch", Array.from({length: 8}, (_, i) => ({value: i})));

            await waitFor(() => processed.length === 4, 5000, "all 4 batches processed");

            const allItems = processed.flatMap((b) => b).map((i) => i.value);
            expect(allItems.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
            // With concurrency=4 and 4 batches each taking 80ms, we should see
            // at least 2 in flight simultaneously on a healthy system.
            expect(maxInFlight).toBeGreaterThan(1);
        } finally {
            await worker.close();
        }
    });

    it("items with unicode, quotes, and large payloads round-trip through staging", async () => {
        name = uniqueName();
        interface Payload {label: string; chunk: string}
        const q = new BatchQueue<Payload>(name, {
            connection: redisOpts(),
            batch: {maxBatchSize: 3, maxBatchWaitMs: 60_000},
        });
        queue = q as unknown as BatchQueue<Item>;

        const tricky: Payload[] = [
            {label: 'has "double quotes" and \\ backslashes', chunk: "x"},
            {label: "unicode 🚀 测试 — multibyte ✓", chunk: "y"},
            {label: "large", chunk: "z".repeat(50_000)},
        ];

        await q.add("batch", tricky[0]);
        await q.add("batch", tricky[1]);
        const job = await q.add("batch", tricky[2]);
        expect(job).toBeDefined();
        expect(job!.data).toEqual(tricky);
    });
});
