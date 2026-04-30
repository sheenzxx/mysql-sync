import mysql from 'mysql2/promise';
import type { Pool } from 'mysql2/promise';
import type { SyncConfig, FullSyncTask, FullSyncBatch, RowChange, SyncObject, BinlogPosition } from '../types.js';
import { logger } from '../logger.js';
import { createPool, getColumns, getPrimaryKeys, getMasterStatus, checkBinlogEnabled, checkBinlogFormat, closePool } from '../db.js';
import { FullSyncCollector } from '../collector/full-collector.js';
import { FullSyncReplayer } from '../replayer/full-replayer.js';
import { IncrementalCollector } from '../collector/incremental.js';
import { IncrementalReplayer } from '../replayer/incr-replayer.js';
import { PositionManager } from '../position-manager.js';
import { SyncManager } from '../sync-manager.js';
import { WorkerPool } from './worker-pool.js';

/**
 * Pipeline orchestrator:
 * Manages the full lifecycle of a sync operation:
 * 1. Connect to source and target
 * 2. Resolve sync objects
 * 3. Run full sync (concurrent table workers)
 * 4. Switch to incremental binlog sync
 * 5. Handle dynamic object changes
 */
export class SyncOrchestrator {
  private config: SyncConfig;
  private sourcePool!: Pool;
  private targetPool!: Pool;

  private fullCollector!: FullSyncCollector;
  private fullReplayer!: FullSyncReplayer;
  private incrCollector!: IncrementalCollector;
  private incrReplayer!: IncrementalReplayer;
  private positionManager!: PositionManager;
  private syncManager!: SyncManager;

  private fullSyncTasks: FullSyncTask[] = [];
  private running = false;
  private incrementalActive = false;

  stats = {
    fullSync: {
      tablesTotal: 0,
      tablesCompleted: 0,
      rowsRead: 0,
      rowsWritten: 0,
      startTime: 0,
    },
    incrementalSync: {
      inserts: 0,
      updates: 0,
      deletes: 0,
      errors: 0,
      startTime: 0,
    },
  };

  constructor(config: SyncConfig) {
    this.config = config;
  }

  /** Start the sync pipeline */
  async start(): Promise<void> {
    this.running = true;

    try {
      // Connect to source and target
      logger.info('Connecting to databases...');
      this.sourcePool = createPool(this.config.source);
      this.targetPool = createPool(this.config.target);

      // Verify connections
      await this.sourcePool.query('SELECT 1');
      await this.targetPool.query('SELECT 1');
      logger.info('Database connections established');

      // Initialize components
      this.fullCollector = new FullSyncCollector(this.sourcePool, this.config);
      this.fullReplayer = new FullSyncReplayer(this.targetPool, this.config);
      this.incrReplayer = new IncrementalReplayer(this.targetPool, this.config);
      this.positionManager = new PositionManager(this.config.incrementalSync.positionFile);
      this.syncManager = new SyncManager(this.sourcePool, this.config);

      // Resolve sync objects
      const syncObjects = await this.syncManager.resolveSyncObjects();
      this.stats.fullSync.tablesTotal = syncObjects.length;

      if (syncObjects.length === 0) {
        logger.warn('No tables to sync');
        return;
      }

      // Prepare target schema
      await this.prepareTargetSchema(syncObjects);

      // Run full sync
      if (this.config.fullSync.enabled) {
        await this.runFullSync(syncObjects);
      }

      // Start incremental sync
      if (this.config.incrementalSync.enabled && this.running) {
        await this.startIncrementalSync(syncObjects);
      }

    } catch (err) {
      logger.error('Sync orchestration failed:', err);
      throw err;
    }
  }

  /** Stop the sync pipeline */
  async stop(): Promise<void> {
    this.running = false;
    this.incrementalActive = false;

    if (this.incrCollector) {
      this.incrCollector.stop();
    }

    this.syncManager?.stopPeriodicCheck();

    // Close connections
    if (this.sourcePool) await closePool(this.sourcePool).catch(() => {});
    if (this.targetPool) await closePool(this.targetPool).catch(() => {});

    logger.info('Sync stopped');
  }

  /** Prepare target schema: create databases and tables */
  private async prepareTargetSchema(objects: SyncObject[]): Promise<void> {
    logger.info('Preparing target schema...');
    const seen = new Set<string>();

    // Disable FK checks for bulk load
    if (this.config.fullSync.enabled) {
      await this.fullReplayer.prepareForBulkInsert();
    }

    for (const obj of objects) {
      const key = `${obj.database}.${obj.table}`;
      if (seen.has(key)) continue;
      seen.add(key);

      try {
        await this.fullReplayer.initTargetSchema(this.sourcePool, obj.database, obj.table);
        logger.info(`  Target schema ready: ${key}`);
      } catch (err) {
        logger.warn(`  Failed to prepare ${key}:`, err);
      }
    }
  }

  /** Run full sync for all tables */
  private async runFullSync(objects: SyncObject[]): Promise<void> {
    logger.info('=== Starting full sync ===');
    this.stats.fullSync.startTime = Date.now();

    // Prepare sync tasks
    const tasks: FullSyncTask[] = [];
    for (const obj of objects) {
      const columns = await getColumns(this.sourcePool, obj.database, obj.table);
      const pks = await getPrimaryKeys(this.sourcePool, obj.database, obj.table);
      tasks.push({
        database: obj.database,
        table: obj.table,
        columns: columns.map(c => c.name),
        primaryKeys: pks,
        batchSize: this.config.fullSync.batchSize,
      });
    }
    this.fullSyncTasks = tasks;

    // Process tables concurrently
    const tablePool = new WorkerPool<FullSyncTask>(
      this.config.fullSync.workerCount,
      async (task) => {
        const batchPool = new WorkerPool<FullSyncBatch>(
          2, // Write 2 batches concurrently
          async (batch) => {
            const written = await this.fullReplayer.writeBatch(batch);
            this.stats.fullSync.rowsWritten += written;
          },
        );

        batchPool.on('error', (err) => {
          logger.error(`Batch write error for ${task.database}.${task.table}:`, err);
        });

        // Collect all batches and feed to the writer pool
        for await (const batch of this.fullCollector.collect(task)) {
          if (!this.running) break;
          this.stats.fullSync.rowsRead += batch.rows.length;
          batchPool.push(batch);
          this.stats.fullSync.rowsWritten += batch.rows.length; // optimistic
        }

        await batchPool.drain();
        this.stats.fullSync.tablesCompleted++;
        logger.info(`[${this.stats.fullSync.tablesCompleted}/${this.stats.fullSync.tablesTotal}] Full sync done: ${task.database}.${task.table}`);
      },
    );

    tablePool.on('error', (err, task) => {
      logger.error(`Table sync error for ${(task as FullSyncTask).database}.${(task as FullSyncTask).table}:`, err);
    });

    for (const task of tasks) {
      tablePool.push(task);
    }

    await tablePool.drain();

    // Re-enable FK checks
    await this.fullReplayer.finalizeBulkInsert();

    const elapsed = ((Date.now() - this.stats.fullSync.startTime) / 1000).toFixed(1);
    logger.info(`=== Full sync complete (${elapsed}s) ===`);
    logger.info(`  Tables: ${this.stats.fullSync.tablesCompleted}/${this.stats.fullSync.tablesTotal}`);
    logger.info(`  Rows: ${this.stats.fullSync.rowsWritten} written`);
  }

  /** Start incremental binlog sync */
  private async startIncrementalSync(syncObjects: SyncObject[]): Promise<void> {
    // Check binlog configuration
    const binlogEnabled = await checkBinlogEnabled(this.sourcePool);
    if (!binlogEnabled) {
      logger.error('Binlog is not enabled on the source server. Incremental sync requires binlog (ROW format).');
      return;
    }

    const binlogFormat = await checkBinlogFormat(this.sourcePool);
    if (binlogFormat !== 'ROW') {
      logger.error(`Binlog format is ${binlogFormat}, but ROW format is required for incremental sync.`);
      return;
    }

    logger.info('=== Starting incremental sync (binlog) ===');
    this.stats.incrementalSync.startTime = Date.now();

    // Pre-warm column cache for all known tables to avoid slow
    // information_schema queries during concurrent event replay
    for (const obj of syncObjects) {
      await this.incrReplayer.warmCache(obj.database, obj.table);
    }
    logger.info(`Column cache warmed for ${syncObjects.length} tables`);

    // Get starting position
    let startPos: BinlogPosition | null = await this.positionManager.load();

    if (!startPos) {
      // Get current master position
      const masterStatus = await getMasterStatus(this.sourcePool);
      if (!masterStatus) {
        logger.error('Could not determine binlog position');
        return;
      }
      startPos = {
        filename: masterStatus.filename,
        position: masterStatus.position,
      };
      logger.info(`Starting binlog from current position: ${startPos.filename}:${startPos.position}`);
    } else {
      logger.info(`Resuming binlog from position: ${startPos.filename}:${startPos.position}`);
    }

    this.incrementalActive = true;

    // Per-table worker pools ensure events for the same table are
    // processed sequentially, preventing race conditions where a
    // DELETE runs before its corresponding INSERT completes.
    const tablePools = new Map<string, WorkerPool<RowChange>>();

    const processor = async (change: RowChange) => {
      try {
        await this.incrReplayer.apply(change);
        switch (change.type) {
          case 'insert': this.stats.incrementalSync.inserts++; break;
          case 'update': this.stats.incrementalSync.updates++; break;
          case 'delete': this.stats.incrementalSync.deletes++; break;
        }
      } catch (err) {
        this.stats.incrementalSync.errors++;
        logger.error(`Failed to apply ${change.type} on ${change.database}.${change.table}:`, err);
      }
    };

    const getTablePool = (database: string, table: string): WorkerPool<RowChange> => {
      const key = `${database}.${table}`;
      let pool = tablePools.get(key);
      if (!pool) {
        pool = new WorkerPool<RowChange>(1, processor);
        pool.on('error', (err) => {
          logger.error(`Incremental worker error (${key}):`, err);
        });
        tablePools.set(key, pool);
      }
      return pool;
    };

    // Start periodic sync object check
    this.syncManager.on('object-added', (obj: SyncObject) => {
      logger.info(`New table detected, starting full sync: ${obj.database}.${obj.table}`);
      // Start a mini full sync for the new table
      this.runSingleTableFullSync(obj).catch(err => {
        logger.error(`Failed to sync new table ${obj.database}.${obj.table}:`, err);
      });
    });

    this.syncManager.startPeriodicCheck(30000);

    // Start binlog collector
    this.incrCollector = new IncrementalCollector(this.config.source, this.config.incrementalSync.serverId);

    this.incrCollector.on('change', (change: RowChange) => {
      if (!this.incrementalActive) return;
      getTablePool(change.database, change.table).push(change);
    });

    this.incrCollector.on('position', (pos: BinlogPosition) => {
      this.positionManager.save(pos).catch(err => {
        logger.warn('Failed to save position:', err);
      });
    });

    await this.incrCollector.start(startPos);
  }

  /** Run full sync for a single table (used for dynamic additions) */
  private async runSingleTableFullSync(obj: SyncObject): Promise<void> {
    try {
      await this.fullReplayer.initTargetSchema(this.sourcePool, obj.database, obj.table);
      const columns = await getColumns(this.sourcePool, obj.database, obj.table);
      const pks = await getPrimaryKeys(this.sourcePool, obj.database, obj.table);

      const task: FullSyncTask = {
        database: obj.database,
        table: obj.table,
        columns: columns.map(c => c.name),
        primaryKeys: pks,
        batchSize: this.config.fullSync.batchSize,
      };

      for await (const batch of this.fullCollector.collect(task)) {
        if (!this.running) break;
        await this.fullReplayer.writeBatch(batch);
      }

      logger.info(`Dynamic full sync complete: ${obj.database}.${obj.table}`);
    } catch (err) {
      logger.error(`Dynamic full sync failed for ${obj.database}.${obj.table}:`, err);
    }
  }

  /** Print current stats */
  printStats(): void {
    const fs = this.stats.fullSync;
    const inc = this.stats.incrementalSync;

    logger.info('=== Sync Stats ===');
    logger.info(`Full sync: ${fs.tablesCompleted}/${fs.tablesTotal} tables, ${fs.rowsWritten} rows`);
    if (inc.startTime > 0) {
      const elapsed = ((Date.now() - inc.startTime) / 1000).toFixed(0);
      logger.info(`Incremental sync: +${inc.inserts} / ~${inc.updates} / -${inc.deletes} (${inc.errors} err, ${elapsed}s)`);
    }
  }
}
