import {QueueOptions} from "bullmq";

/**
 * Batch configuration for a {@link BatchQueue}.
 */
export interface BatchOptions {
    /**
     * Hard cap on items per batched job. When the staging list reaches this
     * length, a batch is flushed immediately on the next `add`.
     */
    maxBatchSize: number;

    /**
     * Minimum items required for a *time-triggered* flush. Below this
     * threshold the flush poller keeps waiting even after `maxBatchWaitMs`
     * elapses. Size-triggered flushes ignore this. Default `1` — flush
     * whatever is staged once the timeout fires.
     */
    minBatchSize?: number;

    /**
     * Max wait (ms) between the head item's arrival and a time-triggered
     * flush. Default `5000`.
     */
    maxBatchWaitMs?: number;

    /**
     * BullMQ job `name` used when a flushed batch is added to the internal
     * queue. Default `"batch"`.
     */
    jobName?: string;

    /**
     * How often (ms) the flush poller wakes to evaluate the time gate.
     * Default `Math.max(50, floor(maxBatchWaitMs / 4))`.
     */
    pollInterval?: number;

    /**
     * Pending entries older than this (ms) are considered orphaned and
     * recovered by `batchRecover`. Must be comfortably larger than expected
     * `queue.add` latency to avoid recovering an in-flight flush — recovery
     * of a live flush produces at-least-once duplicates downstream.
     * Default `30000` (30s).
     */
    recoveryThresholdMs?: number;

    /**
     * How often (ms) to run orphan recovery against the pending HASH.
     * Default `Math.max(5000, recoveryThresholdMs / 6)`.
     */
    recoveryInterval?: number;
}

export interface BatchQueueOptions extends QueueOptions {
    batch: BatchOptions;
}
