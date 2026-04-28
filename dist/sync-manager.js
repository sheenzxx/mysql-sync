import { EventEmitter } from 'node:events';
import { getDatabases, getTables } from './db.js';
import { logger } from './logger.js';
/**
 * Dynamic Sync Manager:
 * Monitors the source database for table changes (adds/removes)
 * and emits events so the pipeline can adapt dynamically during
 * incremental sync mode.
 */
export class SyncManager extends EventEmitter {
    sourcePool;
    config;
    activeObjects = new Set(); // "db.table" keys
    checkInterval = null;
    constructor(sourcePool, config) {
        super();
        this.sourcePool = sourcePool;
        this.config = config;
    }
    /** Compute the initial set of sync objects */
    async resolveSyncObjects() {
        const objects = [];
        const { syncObjects } = this.config;
        if (syncObjects.databases && syncObjects.databases.length > 0) {
            for (const db of syncObjects.databases) {
                const tables = await getTables(this.sourcePool, db);
                for (const table of tables) {
                    if (!this.isExcluded(db, table)) {
                        objects.push({ database: db, table });
                    }
                }
            }
        }
        if (syncObjects.tables && syncObjects.tables.length > 0) {
            for (const tbl of syncObjects.tables) {
                if (!this.isExcluded(tbl.database, tbl.table)) {
                    // Avoid duplicates
                    if (!objects.some(o => o.database === tbl.database && o.table === tbl.table)) {
                        objects.push({ database: tbl.database, table: tbl.table });
                    }
                }
            }
        }
        // If neither databases nor tables specified, discover all
        if (objects.length === 0) {
            const dbs = await getDatabases(this.sourcePool);
            for (const db of dbs) {
                const tables = await getTables(this.sourcePool, db);
                for (const table of tables) {
                    objects.push({ database: db, table });
                }
            }
        }
        for (const obj of objects) {
            this.activeObjects.add(`${obj.database}.${obj.table}`);
        }
        logger.info(`Resolved ${objects.length} sync objects`);
        return objects;
    }
    /** Start periodic checks for new/changed tables */
    startPeriodicCheck(intervalMs = 30000) {
        if (this.checkInterval)
            return;
        this.checkInterval = setInterval(() => this.checkForChanges(), intervalMs);
        logger.info(`Periodic sync check started (every ${intervalMs}ms)`);
    }
    /** Stop periodic checks */
    stopPeriodicCheck() {
        if (this.checkInterval) {
            clearInterval(this.checkInterval);
            this.checkInterval = null;
        }
    }
    /** Check for changes in sync objects */
    async checkForChanges() {
        try {
            const currentObjects = await this.resolveSyncObjects();
            const currentSet = new Set(currentObjects.map(o => `${o.database}.${o.table}`));
            // Find new objects
            for (const obj of currentObjects) {
                const key = `${obj.database}.${obj.table}`;
                if (!this.activeObjects.has(key)) {
                    this.activeObjects.add(key);
                    logger.info(`New sync object detected: ${key}`);
                    this.emit('object-added', obj);
                }
            }
            // Find removed objects
            for (const key of this.activeObjects) {
                if (!currentSet.has(key)) {
                    this.activeObjects.delete(key);
                    const [db, ...tblParts] = key.split('.');
                    const tbl = tblParts.join('.');
                    logger.info(`Sync object removed: ${key}`);
                    this.emit('object-removed', { database: db, table: tbl });
                }
            }
        }
        catch (err) {
            logger.error('Error checking for sync object changes:', err);
        }
    }
    /** Check if a table is in the exclude list */
    isExcluded(database, table) {
        const excludes = this.config.syncObjects.excludeTables ?? [];
        return excludes.some(e => e.database === database && e.table === table);
    }
}
//# sourceMappingURL=sync-manager.js.map