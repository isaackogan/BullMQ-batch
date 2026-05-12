import {randomUUID} from "node:crypto";
import IORedis, {Redis} from "ioredis";
import {BatchQueue, BatchQueueOptions, BatchProcessor, BatchWorker} from "../src";
import {WorkerOptions} from "bullmq";

export const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

/**
 * Per-test connection options. BullMQ docs require `maxRetriesPerRequest:
 * null` for the *worker's* blocking client; we use the same for the queue's
 * client too so tests don't drag in surprising retry timeouts.
 */
export const redisOpts = () => ({
    host: "localhost",
    port: 6379,
});

/** Direct ioredis client for setup / inspection / cleanup. */
export function rawClient(): Redis {
    return new IORedis(redisOpts());
}

/** Generate a unique queue name so tests don't collide. */
export function uniqueName(prefix = "test"): string {
    return `${prefix}-${randomUUID()}`;
}

/**
 * Build a BatchQueue with sensible test defaults. `batch.maxBatchSize` and
 * friends can be overridden via `batchOverrides`.
 */
export function makeQueue<T = unknown>(
    name: string,
    batchOverrides: Partial<BatchQueueOptions["batch"]> = {},
): BatchQueue<T> {
    return new BatchQueue<T>(name, {
        connection: redisOpts(),
        batch: {
            maxBatchSize: 5,
            minBatchSize: 1,
            maxBatchWaitMs: 300,
            pollInterval: 50,
            recoveryThresholdMs: 500,
            recoveryInterval: 100,
            ...batchOverrides,
        },
    });
}

/**
 * Build a BatchWorker. Resolves to a tuple of `{worker, jobs}`, where `jobs`
 * is an array that the processor appends to on each invocation — handy for
 * inspecting what arrived.
 */
export function makeWorker<T = unknown>(
    name: string,
    processor: BatchProcessor<T> = async () => {},
    opts: Partial<WorkerOptions> = {},
): {worker: BatchWorker<T>; jobs: Array<{data: T[]; jobId?: string}>} {
    const jobs: Array<{data: T[]; jobId?: string}> = [];
    const worker = new BatchWorker<T>(
        name,
        async (job) => {
            jobs.push({data: job.data, jobId: job.id});
            return processor(job);
        },
        {connection: redisOpts(), ...opts},
    );
    return {worker, jobs};
}

/** Wait for a predicate to become true, polling every 10ms. */
export async function waitFor(
    predicate: () => boolean | Promise<boolean>,
    timeoutMs = 5000,
    label = "predicate",
): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await new Promise((r) => setTimeout(r, 10));
    }
    throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms`);
}

/** Sleep helper for explicit time-gated tests. */
export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Best-effort cleanup: unlink every key under the queue's prefixed keyspace.
 */
export async function cleanupQueue(name: string, prefix = "bull-batch"): Promise<void> {
    const client = rawClient();
    try {
        const pattern = `${prefix}:${name}:*`;
        const keys = await client.keys(pattern);
        if (keys.length) await client.unlink(...keys);
    } finally {
        await client.quit();
    }
}
