import {Job, Processor} from "bullmq";

/**
 * Convenience alias for a job carrying a batch of items.
 *
 * `BatchQueue` produces jobs whose `data` is an array — `Job<DataType[]>`.
 * The worker just needs that type narrowing.
 */
export type BatchJob<
    DataType = any,
    DefaultResultType = any,
    DefaultNameType extends string = string,
> = Job<DataType[], DefaultResultType, DefaultNameType>;

export type BatchProcessor<
    DataType = any,
    DefaultResultType = any,
    DefaultNameType extends string = string,
> = Processor<DataType[], DefaultResultType, DefaultNameType>;
