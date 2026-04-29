import type { Pool } from 'mysql2/promise';
import type { RowChange, SyncConfig } from '../types.js';
/**
 * Incremental-sync Replayer:
 * Applies RowChange events (insert/update/delete) to the target database.
 * Supports concurrent execution for higher throughput.
 */
export declare class IncrementalReplayer {
    private pool;
    constructor(targetPool: Pool, config: SyncConfig);
    /**
     * Apply a single row change to the target.
     * Maps column indices back to column names by querying table metadata.
     */
    apply(change: RowChange): Promise<void>;
    /**
     * Apply a batch of changes in a single transaction for efficiency.
     */
    applyBatch(changes: RowChange[]): Promise<void>;
    private applyInsert;
    private applyUpdate;
    private applyDelete;
    private buildInsert;
    private buildUpdate;
    private buildDelete;
    /** Column info cache */
    private columnCache;
    /** Pre-warm the column cache for a table (avoids slow first query during replay) */
    warmCache(database: string, table: string): Promise<void>;
    private getTableColumns;
}
//# sourceMappingURL=incr-replayer.d.ts.map