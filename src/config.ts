import { readFileSync, existsSync } from 'node:fs';
import type { SyncConfig } from './types.js';
import { logger } from './logger.js';

/** Default configuration values */
const DEFAULTS: Partial<SyncConfig> = {
  mode: 'full+incremental',
  fullSync: {
    enabled: true,
    workerCount: 4,
    batchSize: 1000,
    batchQueueSize: 10,
  },
  incrementalSync: {
    enabled: true,
    workerCount: 2,
    positionFile: './binlog-position.json',
    serverId: Math.floor(Math.random() * 100000) + 1000,
  },
  syncObjects: {
    databases: undefined,
    tables: undefined,
    excludeTables: [],
  },
};

/** Load configuration from a JSON file */
export function loadConfigFile(filePath: string): SyncConfig {
  if (!existsSync(filePath)) {
    throw new Error(`Config file not found: ${filePath}`);
  }
  const raw = readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(raw);
  return normalizeConfig(parsed);
}

/** Build configuration from CLI options + defaults */
export function buildConfig(cliOptions: Record<string, any>): SyncConfig {
  const config: SyncConfig = {
    source: {
      host: cliOptions.sourceHost || 'localhost',
      port: cliOptions.sourcePort || 3306,
      user: cliOptions.sourceUser || 'root',
      password: cliOptions.sourcePassword || '',
      database: cliOptions.sourceDatabase || undefined,
    },
    target: {
      host: cliOptions.targetHost || 'localhost',
      port: cliOptions.targetPort || 3306,
      user: cliOptions.targetUser || 'root',
      password: cliOptions.targetPassword || '',
    },
    mode: cliOptions.mode || 'full+incremental',
    syncObjects: {
      databases: cliOptions.databases ? cliOptions.databases.split(',').map((s: string) => s.trim()) : undefined,
      tables: cliOptions.tables
        ? cliOptions.tables.split(',').map((s: string) => {
            const [db, ...tbl] = s.trim().split('.');
            return { database: db, table: tbl.join('.') };
          })
        : undefined,
      excludeTables: cliOptions.excludeTables
        ? cliOptions.excludeTables.split(',').map((s: string) => {
            const [db, ...tbl] = s.trim().split('.');
            return { database: db, table: tbl.join('.') };
          })
        : [],
    },
    fullSync: {
      enabled: cliOptions.fullSync !== false,
      workerCount: cliOptions.fullSyncWorkers || 4,
      batchSize: cliOptions.batchSize || 1000,
      batchQueueSize: cliOptions.queueSize || 10,
    },
    incrementalSync: {
      enabled: cliOptions.incrementalSync !== false,
      workerCount: cliOptions.incrWorkers || 2,
      positionFile: cliOptions.positionFile || './binlog-position.json',
      serverId: cliOptions.serverId || Math.floor(Math.random() * 100000) + 1000,
    },
  };

  return normalizeConfig(config);
}

/** Validate and apply defaults */
export function normalizeConfig(config: SyncConfig): SyncConfig {
  // Apply defaults
  if (!config.mode) config.mode = DEFAULTS.mode as SyncConfig['mode'];
  config.fullSync = { ...DEFAULTS.fullSync, ...config.fullSync } as SyncConfig['fullSync'];
  config.incrementalSync = { ...DEFAULTS.incrementalSync, ...config.incrementalSync } as SyncConfig['incrementalSync'];
  config.syncObjects = {
    ...DEFAULTS.syncObjects,
    ...config.syncObjects,
  };

  // Validate
  if (!config.source.host) throw new Error('Source host is required');
  if (!config.source.user) throw new Error('Source user is required');
  if (!config.target.host) throw new Error('Target host is required');
  if (!config.target.user) throw new Error('Target user is required');

  if (config.fullSync.workerCount < 1) config.fullSync.workerCount = 1;
  if (config.fullSync.batchSize < 1) config.fullSync.batchSize = 100;
  if (config.incrementalSync.workerCount < 1) config.incrementalSync.workerCount = 1;

  logger.info('Configuration loaded:');
  logger.info(`  Mode: ${config.mode}`);
  logger.info(`  Source: ${config.source.user}@${config.source.host}:${config.source.port}`);
  logger.info(`  Target: ${config.target.user}@${config.target.host}:${config.target.port}`);
  logger.info(`  Full sync: ${config.fullSync.enabled ? `enabled (${config.fullSync.workerCount} workers, batch ${config.fullSync.batchSize})` : 'disabled'}`);
  logger.info(`  Incremental sync: ${config.incrementalSync.enabled ? `enabled (${config.incrementalSync.workerCount} workers)` : 'disabled'}`);

  return config;
}
