import type { Pool } from 'mysql2/promise';
import type { FullSyncTask, FullSyncBatch, SyncConfig } from '../types.js';
/**
 * Full-Sync Collector:
 * Reads all rows from a source table using keyset pagination (primary key),
 * yields batches of rows to be consumed by the replayer.
 */
export declare class FullSyncCollector {
    private pool;
    constructor(sourcePool: Pool, _config: SyncConfig);
    /**
     * Given a task, stream batches of rows through the returned async iterator.
     * Uses keyset pagination on the primary key for efficient batching.
     */
    collect(task: FullSyncTask): AsyncGenerator<FullSyncBatch>;
    /** Fallback for tables without primary key */
    private collectWithLimitOffset;
}
//# sourceMappingURL=full-collector.d.ts.map