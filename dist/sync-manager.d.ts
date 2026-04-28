import { EventEmitter } from 'node:events';
import type { Pool } from 'mysql2/promise';
import type { SyncObject, SyncConfig } from './types.js';
/**
 * Dynamic Sync Manager:
 * Monitors the source database for table changes (adds/removes)
 * and emits events so the pipeline can adapt dynamically during
 * incremental sync mode.
 */
export declare class SyncManager extends EventEmitter {
    private sourcePool;
    private config;
    private activeObjects;
    private checkInterval;
    constructor(sourcePool: Pool, config: SyncConfig);
    /** Compute the initial set of sync objects */
    resolveSyncObjects(): Promise<SyncObject[]>;
    /** Start periodic checks for new/changed tables */
    startPeriodicCheck(intervalMs?: number): void;
    /** Stop periodic checks */
    stopPeriodicCheck(): void;
    /** Check for changes in sync objects */
    private checkForChanges;
    /** Check if a table is in the exclude list */
    private isExcluded;
}
//# sourceMappingURL=sync-manager.d.ts.map