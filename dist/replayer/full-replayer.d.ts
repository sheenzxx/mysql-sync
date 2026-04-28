import type { Pool } from 'mysql2/promise';
import type { FullSyncBatch, SyncConfig } from '../types.js';
export type AckCallback = (err?: Error) => void;
/**
 * Full-sync Replayer:
 * Writes batches of rows into the target MySQL database.
 * Uses INSERT with batch values and optional REPLACE for conflict handling.
 */
export declare class FullSyncReplayer {
    private pool;
    private batchSize;
    constructor(targetPool: Pool, config: SyncConfig);
    /**
     * Write a batch of rows to the target table.
     * Uses INSERT IGNORE to skip duplicates (for resumable full sync).
     * Returns the number of rows written.
     */
    writeBatch(batch: FullSyncBatch): Promise<number>;
    /**
     * Initialize target schema: create database and tables if they don't exist.
     * Copies table structure from source.
     */
    initTargetSchema(sourcePool: Pool, database: string, table: string): Promise<void>;
    /**
     * Disable FK checks and enable before bulk insert
     */
    prepareForBulkInsert(): Promise<void>;
    /**
     * Re-enable constraints after bulk insert
     */
    finalizeBulkInsert(): Promise<void>;
}
//# sourceMappingURL=full-replayer.d.ts.map