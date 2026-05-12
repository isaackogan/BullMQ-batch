# bullmq-batch

Distributed batching extensions for [BullMQ](https://docs.bullmq.io/). Stage individual items in Redis and process them as a single batched job once a size or time threshold is hit.

## Why

BullMQ jobs are per-item by default. When downstream work benefits from batching (bulk database writes, bulk API calls, vectorized processing), you typically end up rolling your own staging buffer, timeout logic, and crash recovery. `bullmq-batch` provides:

- `BatchQueue` for staging items and emitting batched jobs.
- `BatchWorker` for consuming those batched jobs with proper typing.
- Atomic Redis-side staging via Lua, with two-phase commit and orphan recovery for at-least-once delivery.

## How it works

Under the hood, `BatchQueue` wraps a regular BullMQ `Queue` and `BatchWorker` is a thin subclass of a regular BullMQ `Worker`. The only thing this library adds is atomic Redis-side staging: items sit in a Lua-managed staging list until the size or time threshold is reached, at which point a single normal BullMQ job is emitted carrying the whole batch as its `data` array.

This is intended for queues that genuinely process work in batches (bulk inserts, bulk API calls), where one job per batch is the natural unit of work. It is not a fan-out helper. If you want N items to produce N jobs, use BullMQ directly.

## Install

```bash
npm install bullmq-batch bullmq
```

`bullmq` is a peer dependency (>=5).

## Quick start

```ts
import { BatchQueue, BatchWorker } from "bullmq-batch";

type Event = { userId: string; type: string };

const connection = { host: "127.0.0.1", port: 6379 };

const queue = new BatchQueue<Event>("events", {
    connection,
    batch: {
        maxBatchSize: 100,
        maxBatchWaitMs: 2000,
    },
});

const worker = new BatchWorker<Event>(
    "events",
    async (job) => {
        // job.data is Event[]
        await writeEventsToDb(job.data);
    },
    { connection },
);

await queue.add("event", { userId: "u1", type: "login" });
await queue.add("event", { userId: "u2", type: "click" });
```

A batched job fires when either:

1. The staging list reaches `maxBatchSize` items (size trigger), or
2. `maxBatchWaitMs` has elapsed since the head item arrived and at least `minBatchSize` items are staged (time trigger).

## API

### BatchQueue

```ts
new BatchQueue<DataType>(name, opts)
```

`opts` extends BullMQ's `QueueOptions` with a required `batch` block:

| Option | Default | Description |
| --- | --- | --- |
| `maxBatchSize` | (required) | Hard cap on items per batched job. |
| `minBatchSize` | `1` | Minimum items for a time-triggered flush. |
| `maxBatchWaitMs` | `5000` | Max wait (ms) between head item arrival and time flush. |
| `jobName` | `"batch"` | BullMQ job name applied to flushed batches. |
| `pollInterval` | `max(50, maxBatchWaitMs / 4)` | Flush poller interval (ms). |
| `recoveryThresholdMs` | `30000` | Age (ms) at which a pending entry is considered orphaned. |
| `recoveryInterval` | `max(5000, recoveryThresholdMs / 6)` | Orphan recovery poller interval (ms). |

Methods:

```ts
// Stage one item. Returns the resulting Job if this call triggered a flush.
await queue.add("event", { userId: "u1", type: "login" });

// Stage many items sequentially.
await queue.addBulk("event", [event1, event2, event3]);

// Force-flush whatever is currently staged, ignoring min size and timeout.
await queue.flush();

// Run one pass of orphan recovery. Returns the number of items recovered.
await queue.recover();

// Stop pollers and close the underlying queue. Does not auto-flush.
await queue.close();
```

The underlying BullMQ `Queue` is exposed as `queue.queue` for advanced operations (`pause`, `resume`, `getJobCounts`, etc.).

### BatchWorker

```ts
new BatchWorker<DataType>(name, processor, opts)
```

A thin `Worker` subclass with two differences from a regular BullMQ worker:

1. `job.data` is typed as `DataType[]` instead of `DataType`.
2. The Redis prefix is automatically aligned with `BatchQueue`'s keyspace.

```ts
const worker = new BatchWorker<Event>(
    "events",
    async (job) => {
        for (const event of job.data) {
            await process(event);
        }
    },
    { connection, concurrency: 4 },
);
```

## Graceful shutdown

`close` does not auto-flush, so call `flush` first if you want to drain staged items:

```ts
await queue.flush();
await queue.close();
await worker.close();
```

## Fault tolerance

A flush is a two-phase commit:

1. `batchStage` / `batchTryFlush` atomically moves items from the staging list into a pending HASH entry.
2. After `queue.add` succeeds, the pending entry is deleted.

If the process dies between those steps, the entry becomes an orphan and is reclaimed by a recurring `batchRecover` pass that re-stages its items. This makes delivery at-least-once: a crash mid-flush will not lose items, but in the pathological case where `queue.add` succeeds and the subsequent `HDEL` does not, the same items can appear in two batches. Keep `recoveryThresholdMs` comfortably above expected `queue.add` latency to make duplicates rare.

## Keyspace

`BatchQueue` and `BatchWorker` automatically apply a `bull-batch` prefix so they cannot collide with regular BullMQ queues sharing the same name. Pass the same `prefix` option to both; it is wrapped consistently on each side.

## Re-exports

For convenience, `bullmq-batch` re-exports everything from `bullmq`, so you can import `Job`, `Queue`, `Worker`, `QueueEvents`, etc. from this package directly.

```ts
import { BatchQueue, BatchWorker, QueueEvents } from "bullmq-batch";
```

## License

MIT
