import type { SyncConfig } from '../types.js';
/**
 * Pipeline orchestrator:
 * Manages the full lifecycle of a sync operation:
 * 1. Connect to source and target
 * 2. Resolve sync objects
 * 3. Run full sync (concurrent table workers)
 * 4. Switch to incremental binlog sync
 * 5. Handle dynamic object changes
 */
export declare class SyncOrchestrator {
    private config;
    private sourcePool;
    private targetPool;
    private fullCollector;
    private fullReplayer;
    private incrCollector;
    private incrReplayer;
    private positionManager;
    private syncManager;
    private fullSyncTasks;
    private running;
    private incrementalActive;
    stats: {
        fullSync: {
            tablesTotal: number;
            tablesCompleted: number;
            rowsRead: number;
            rowsWritten: number;
            startTime: number;
        };
        incrementalSync: {
            inserts: number;
            updates: number;
            deletes: number;
            ddl: number;
            errors: number;
            startTime: number;
        };
    };
    constructor(config: SyncConfig);
    /** Start the sync pipeline */
    start(): Promise<void>;
    /** Stop the sync pipeline */
    stop(): Promise<void>;
    /** Prepare target schema: create databases and tables */
    private prepareTargetSchema;
    /** Run full sync for all tables */
    private runFullSync;
    /** Start incremental binlog sync */
    private startIncrementalSync;
    /** Run full sync for a single table (used for dynamic additions) */
    private runSingleTableFullSync;
    /** Print current stats */
    printStats(): void;
}
//# sourceMappingURL=orchestrator.d.ts.map