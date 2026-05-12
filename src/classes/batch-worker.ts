import {Worker, WorkerOptions} from "bullmq";
import {RedisConnection} from "bullmq/dist/esm/classes/redis-connection";
import {withBatchPrefix} from "../utilities/prefix-handling";
import {BatchProcessor} from "../types";

/**
 * A thin Worker subclass typed for batched jobs.
 *
 * Behavior is identical to a regular `Worker` — same per-job lifecycle, same
 * retry / DLQ / stalled-recovery, same telemetry. The only differences:
 *  1. `job.data` is typed as `DataType[]` instead of `DataType`.
 *  2. The Redis prefix is forced through `withBatchPrefix` so it matches the
 *     `BatchQueue`'s keyspace.
 */
export class BatchWorker<
    DataType = any,
    DefaultResultType = any,
    DefaultNameType extends string = string,
> extends Worker<DataType[], DefaultResultType, DefaultNameType> {
    constructor(
        name: string,
        processor: BatchProcessor<DataType, DefaultResultType, DefaultNameType>,
        opts: WorkerOptions,
        Connection?: typeof RedisConnection,
    ) {
        super(
            name,
            processor,
            {...opts, prefix: withBatchPrefix(opts.prefix)},
            Connection,
        );
    }
}
