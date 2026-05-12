import {randomUUID} from "node:crypto";
import {Job, Queue} from "bullmq";
import {RedisConnection} from "bullmq/dist/esm/classes/redis-connection";
import {BatchQueueOptions} from "../types";
import {ExtractDataType, ExtractNameType, ExtractResultType} from "../types/type-helpers";
import {withBatchPrefix} from "../utilities/prefix-handling";
import {BatchPopResult, BatcherClient, registerBatchScripts} from "../commands";

/**
 * Batches items via a Redis-side staging list and emits a single BullMQ job
 * (`data = items[]`) once the size threshold or the time threshold is hit.
 *
 * Composition only — BatchQueue does not extend `Queue`. The internal queue
 * is exposed via the `queue` field for advanced operations (pause, resume,
 * `getJobCounts`, etc.).
 *
 * Generic parameters mirror BullMQ's `Queue<...>` (same 6-param pass-through
 * shape), so `DataType` is the *item* type and the internal queue holds
 * batches typed as `DataType[]`.
 *
 * Fault tolerance: a flush is a two-phase commit. The pop atomically moves
 * items from the staging list into a "pending" HASH entry; only after
 * `queue.add` succeeds does the entry get deleted. If the process dies
 * between those steps, the entry becomes an orphan and is reclaimed by a
 * recurring `batchRecover` pass that re-stages its items.
 *
 * This makes the queue at-least-once: a crash mid-flush will not lose
 * items, but in a pathological case where `queue.add` succeeds and the
 * subsequent `HDEL` does not (and recovery later fires), the same items
 * may appear in two batches. Set `recoveryThresholdMs` well above expected
 * `queue.add` latency to keep duplicates rare.
 */
export class BatchQueue<
    DataTypeOrJob = any,
    DefaultResultType = any,
    DefaultNameType extends string = string,
    DataType = ExtractDataType<DataTypeOrJob, DataTypeOrJob>,
    ResultType = ExtractResultType<DataTypeOrJob, DefaultResultType>,
    NameType extends string = ExtractNameType<DataTypeOrJob, DefaultNameType>,
> {
    /** Underlying queue holding batched jobs (`Job.data` is `DataType[]`). */
    public readonly queue: Queue<DataType[], ResultType, NameType>;

    /**
     * Resolved configuration — every defaulted/derived value computed once
     * in the constructor. Methods read settings off this single object
     * instead of carrying ~10 `private readonly` scalars.
     */
    private readonly config: {
        readonly maxBatchSize: number;
        readonly minBatchSize: number;
        readonly maxBatchWaitMs: number;
        readonly batchJobName: NameType;
        readonly pollIntervalMs: number;
        readonly recoveryThresholdMs: number;
        readonly recoveryIntervalMs: number;
        readonly flusherId: string;
        readonly stagingKey: string;
        readonly firstTsKey: string;
        readonly pendingKey: string;
        readonly pendingSeqKey: string;
    };

    private flushTimer: NodeJS.Timeout | null = null;
    private recoveryTimer: NodeJS.Timeout | null = null;
    private closing = false;

    /**
     * Resolves to the Redis client once it's connected AND our Lua scripts
     * are registered. Every method that needs the client awaits this — so a
     * call made before the underlying connection lands simply queues up
     * behind the single in-flight setup promise, and there's no race window
     * where we'd try to `EVALSHA` a script that hasn't been defined yet.
     */
    private readonly clientReady: Promise<BatcherClient>;

    /**
     * Construct a BatchQueue. The internal `Queue` is created immediately with
     * a `bull-batch`-prefixed keyspace so it cannot collide with regular
     * BullMQ queues sharing the same name.
     *
     * @param name        Queue name. Reused for the BullMQ internal queue and
     *                    for namespacing the staging keys.
     * @param opts        Queue options, plus a required `batch` block.
     * @param Connection  Optional Redis connection class — passed through to
     *                    the internal `Queue` constructor unchanged.
     */
    constructor(name: string, opts: BatchQueueOptions, Connection?: typeof RedisConnection) {
        const {batch, prefix, ...rest} = opts;

        this.queue = new Queue<DataType[], ResultType, NameType>(
            name,
            {
                ...rest,
                prefix: withBatchPrefix(prefix)
            },
            Connection,
        );

        const maxBatchWaitMs = batch.maxBatchWaitMs ?? 5000;
        const recoveryThresholdMs = batch.recoveryThresholdMs ?? 30_000;

        this.config = {
            maxBatchSize: batch.maxBatchSize,
            minBatchSize: batch.minBatchSize ?? 1,
            maxBatchWaitMs,
            pollIntervalMs: batch.pollInterval ?? Math.max(50, Math.floor(maxBatchWaitMs / 4)),
            recoveryThresholdMs,
            recoveryIntervalMs: batch.recoveryInterval ?? Math.max(5000, Math.floor(recoveryThresholdMs / 6)),
            batchJobName: (batch.jobName ?? "batch") as NameType,
            flusherId: randomUUID(),
            stagingKey: this.queue.toKey("batch:staging"),
            firstTsKey: this.queue.toKey("batch:first-ts"),
            pendingKey: this.queue.toKey("batch:pending"),
            pendingSeqKey: this.queue.toKey("batch:pending-seq"),
        };

        // Kick off connection + script registration eagerly. Every method
        // that needs the client awaits `this.clientReady`.
        this.clientReady = this.queue.client.then((c) => {
            const client = c as unknown as BatcherClient;
            registerBatchScripts(client);
            return client;
        });

        this.startFlushPoller();
        this.startRecoveryPoller();
    }

    /**
     * Stage an item. If this push fills the batch (length === `maxBatchSize`),
     * the batch is atomically popped into a pending entry, enqueued as a
     * single BullMQ job, and the pending entry is committed (deleted).
     * Otherwise the item waits for either the next size-trigger or a
     * timeout-triggered flush from the poller.
     *
     * @param name   Job name applied to the resulting batched job (only used
     *               if this call triggers a size-based flush).
     * @param data   Item to append to the current batch.
     * @returns      The BullMQ job created if this call triggered a flush, or
     *               `undefined` if the item was simply staged.
     */
    async add(name: NameType, data: DataType): Promise<Job<DataType[], ResultType, NameType> | undefined> {
        const client = await this.clientReady;

        // Atomic: appends the item, and if the size threshold is reached,
        // also moves the popped batch into the pending HASH for commit.
        const popped = await client.batchStage!(
            this.config.stagingKey,
            this.config.firstTsKey,
            this.config.pendingKey,
            this.config.pendingSeqKey,
            JSON.stringify(data),
            this.config.maxBatchSize,
            Date.now().toString(),
            this.config.flusherId,
        );

        return this.commitIfPopped(client, name, popped);
    }

    /**
     * Stage many items sequentially. Each item may individually trigger a
     * size-based flush; this method awaits each flush before staging the
     * next item so all resulting jobs are created in the same call chain.
     *
     * @param name   Job name applied to any batched jobs created during this
     *               call.
     * @param items  Items to stage in order.
     * @returns      Resolves once every item has been staged (and any flushes
     *               they triggered have been enqueued).
     */
    async addBulk(name: NameType, items: DataType[]): Promise<void> {
        for (const item of items) {
            await this.add(name, item);
        }
    }

    /**
     * Force-flush whatever is currently staged, regardless of `minBatchSize`
     * or `maxBatchWaitMs`. Useful for graceful shutdown or test setups.
     *
     * @param name   Job name applied to the resulting batched job. Defaults
     *               to the configured `jobName` (or `"batch"`).
     * @returns      The BullMQ job created if there were any staged items, or
     *               `undefined` if staging was empty.
     */
    async flush(name: NameType = this.config.batchJobName): Promise<Job<DataType[], ResultType, NameType> | undefined> {
        const client = await this.clientReady;

        // Attempt to flush a batch
        const popped = await client.batchTryFlush!(
            this.config.stagingKey,
            this.config.firstTsKey,
            this.config.pendingKey,
            this.config.pendingSeqKey,
            this.config.maxBatchSize,
            1,             // ignore minBatchSize on manual flush
            0,             // ignore timeout — flush right now
            Date.now().toString(),
            this.config.flusherId,
        );

        // Commits prevent lost jobs when things die
        return this.commitIfPopped(client, name, popped);
    }

    /**
     * Run one pass of orphan recovery: any pending entries older than
     * `recoveryThresholdMs` have their items re-staged.
     *
     * Called automatically on startup and every `recoveryInterval`, but
     * exposed for tests / explicit operations.
     *
     * @returns Number of items recovered in this pass.
     */
    async recover(): Promise<number> {
        const client = await this.clientReady;

        return client.batchRecover!(
            this.config.pendingKey,
            this.config.stagingKey,
            this.config.firstTsKey,
            this.config.recoveryThresholdMs,
            Date.now().toString(),
        );
    }

    /**
     * Stop both pollers and close the underlying queue.
     *
     * Note: any items still in staging at close time are *not* automatically
     * flushed. Call {@link flush} before `close` if you want a graceful drain.
     *
     * @returns Resolves once the internal queue has fully closed.
     */
    async close(): Promise<void> {
        this.closing = true;

        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = null;
        }

        if (this.recoveryTimer) {
            clearInterval(this.recoveryTimer);
            this.recoveryTimer = null;
        }

        await this.queue.close();
    }

    /**
     * Phase 2 of the two-phase flush: turn the popped pending entry into a
     * BullMQ job, then delete the pending entry so recovery won't re-stage it.
     *
     * If `queue.add` throws, the pending entry is intentionally left in place:
     * `batchRecover` will eventually re-stage its items. If `queue.add`
     * succeeds but the subsequent HDEL fails, recovery may eventually
     * re-stage them anyway — which produces at-least-once duplicates.
     */
    private async commitIfPopped(
        client: BatcherClient,
        name: NameType,
        popped: BatchPopResult | null | undefined,
    ): Promise<Job<DataType[], ResultType, NameType> | undefined> {

        if (!popped) return undefined;
        const [field, rawItems] = popped;

        if (rawItems.length === 0) {
            // Defensive: shouldn't happen, but commit and bail out cleanly.
            await client.hdel(this.config.pendingKey, field).catch((): void => undefined);
            return undefined;
        }

        const items = rawItems.map((s) => JSON.parse(s) as DataType);
        const job = await this.queue.add(name, items);

        // Best-effort commit. Failures here only risk a duplicate — never lose
        // items — because recovery is timestamp-gated.
        await client.hdel(this.config.pendingKey, field).catch((err): void => {
            (this.queue as unknown as {emit(e: string, err: Error): void}).emit(
                "error",
                err as Error,
            );
        });

        return job;
    }

    /**
     * Periodic flush poller — drives the time-based flush gate.
     */
    private startFlushPoller(): void {
        this.flushTimer = setInterval(() => {
            if (this.closing) return;

            this.tryTimedFlush().catch((err): void => {
                (this.queue as unknown as {emit(e: string, err: Error): void}).emit(
                    "error",
                    err as Error,
                );
            });
        }, this.config.pollIntervalMs);

        this.flushTimer.unref?.();
    }

    /**
     * Periodic recovery poller — reclaims orphaned pending entries left
     * behind by dead flushers. First pass fires `recoveryIntervalMs` after
     * construction (no eager pre-interval pass), so configuring a large
     * `recoveryInterval` reliably suppresses auto-recovery.
     */
    private startRecoveryPoller(): void {
        this.recoveryTimer = setInterval(() => {
            if (this.closing) return;

            this.recover().catch((err): void => {
                (this.queue as unknown as {emit(e: string, err: Error): void}).emit(
                    "error",
                    err as Error,
                );
            });
        }, this.config.recoveryIntervalMs);

        this.recoveryTimer.unref?.();
    }

    private async tryTimedFlush(): Promise<void> {
        const client = await this.clientReady;

        const popped = await client.batchTryFlush!(
            this.config.stagingKey,
            this.config.firstTsKey,
            this.config.pendingKey,
            this.config.pendingSeqKey,
            this.config.maxBatchSize,
            this.config.minBatchSize,
            this.config.maxBatchWaitMs,
            Date.now().toString(),
            this.config.flusherId,
        );

        await this.commitIfPopped(client, this.config.batchJobName, popped);
    }
}
