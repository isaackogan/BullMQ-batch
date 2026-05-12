/**
 * Redis key suffix appended to the user-supplied prefix (or used standalone
 * when none is given) for BatchQueue and BatchWorker.
 *
 * BullMQ namespaces every key as `{prefix}:{queueName}:{type}`. Forcing this
 * trailing segment into the prefix isolates batch keys from regular BullMQ
 * keys sharing the same name and parent prefix.
 */
export const BATCH_JOB_PREFIX = 'bull-batch';

/**
 * Appends BATCH_JOB_PREFIX to the given prefix, or returns it standalone if no
 *
 * @param prefix Optional user-supplied prefix. Can be the same as the prefix used for regular BullMQ queues, since BATCH_JOB_PREFIX isolates batch keys from regular keys.
 */
export const withBatchPrefix = (prefix?: string): string => prefix ? `${prefix}:${BATCH_JOB_PREFIX}` : BATCH_JOB_PREFIX;
