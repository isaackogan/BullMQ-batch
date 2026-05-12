import {existsSync, readFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
// In source (tests) this file lives in src/commands alongside the .lua files.
// In the bundled output (dist/index.js) the .lua files are copied to dist/commands.
const COMMANDS_DIR = existsSync(join(HERE, 'batchStage-4.lua'))
    ? HERE
    : join(HERE, 'commands');
const INCLUDE_RE = /^---?\s*@include\s+(["'])(.+?)\1\s*$/gm;

/**
 * Recursively inline `--- @include "path"` directives. Each include is loaded
 * at most once per top-level resolution.
 */
function inlineIncludes(filePath: string, seen: Set<string> = new Set()): string {
    const abs = resolve(filePath);
    if (seen.has(abs)) return '';
    seen.add(abs);

    const raw = readFileSync(abs, 'utf8');
    return raw.replace(INCLUDE_RE, (_match, _quote, ref: string) => {
        const target = ref.endsWith('.lua') ? ref : `${ref}.lua`;
        return inlineIncludes(join(dirname(abs), target), seen);
    });
}

/**
 * Result tuple returned by both `batchStage` and `batchTryFlush` when a flush
 * fires: `[pendingField, items[]]`. The caller MUST `HDEL pendingKey
 * pendingField` once `queue.add(items)` has succeeded — otherwise
 * `batchRecover` will eventually re-stage the items.
 */
export type BatchPopResult = [string, string[]];

export interface BatcherClient {
    defineCommand(name: string, opts: {numberOfKeys: number; lua: string}): void;
    hdel(key: string, field: string): Promise<number>;

    batchStage?: (
        stagingKey: string,
        firstTsKey: string,
        pendingKey: string,
        pendingSeqKey: string,
        item: string,
        maxBatchSize: number,
        now: string,
        flusherId: string,
    ) => Promise<BatchPopResult | null>;

    batchTryFlush?: (
        stagingKey: string,
        firstTsKey: string,
        pendingKey: string,
        pendingSeqKey: string,
        maxBatchSize: number,
        minBatchSize: number,
        maxBatchWaitMs: number,
        now: string,
        flusherId: string,
    ) => Promise<BatchPopResult | null>;

    batchRecover?: (
        pendingKey: string,
        stagingKey: string,
        firstTsKey: string,
        thresholdMs: number,
        now: string,
    ) => Promise<number>;
}

/**
 * Registers the staging / flush / recovery scripts on the given ioredis
 * client. Idempotent per client — subsequent calls are no-ops.
 */
export function registerBatchScripts(client: BatcherClient): void {
    if (!client.batchStage) {
        client.defineCommand('batchStage', {
            numberOfKeys: 4,
            lua: inlineIncludes(join(COMMANDS_DIR, 'batchStage-4.lua')),
        });
    }
    if (!client.batchTryFlush) {
        client.defineCommand('batchTryFlush', {
            numberOfKeys: 4,
            lua: inlineIncludes(join(COMMANDS_DIR, 'batchTryFlush-4.lua')),
        });
    }
    if (!client.batchRecover) {
        client.defineCommand('batchRecover', {
            numberOfKeys: 3,
            lua: inlineIncludes(join(COMMANDS_DIR, 'batchRecover-3.lua')),
        });
    }
}
