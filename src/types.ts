/** Database column metadata */
export interface ColumnInfo {
  name: string;
  type: string;       // MySQL column type name (VARCHAR, INT, etc.)
  isNullable: boolean;
  isPrimary: boolean;
  columnType: number;  // MySQL internal type code
  metadata: Buffer;    // Type-specific metadata from binlog
}

/** Table metadata */
export interface TableInfo {
  database: string;
  table: string;
  columns: ColumnInfo[];
  primaryKeys: string[];
  fullName: string;      // `db`.`table`
}

/** Sync target descriptor */
export interface SyncObject {
  database: string;
  table: string;
}

/** Row data representation */
export type RowData = Record<string, unknown>;

/** Row change event from binlog */
export interface RowChange {
  type: 'insert' | 'update' | 'delete' | 'ddl';
  database: string;
  table: string;
  before?: RowData;   // for update/delete
  after?: RowData;    // for insert/update
  timestamp: number;
  /** DDL metadata (populated when type === 'ddl') */
  ddl?: {
    ddlType: string;           // normalized: 'CREATE TABLE', 'ALTER TABLE', etc.
    sql: string;               // raw SQL from binlog
    affectedTables?: string[]; // for RENAME: both old and new table names
  };
}

/** Binlog position */
export interface BinlogPosition {
  filename: string;
  position: number;
  gtid?: string;
}

/** Full sync task for a table */
export interface FullSyncTask {
  database: string;
  table: string;
  columns: string[];
  primaryKeys: string[];
  batchSize: number;
}

/** Full sync batch result */
export interface FullSyncBatch {
  database: string;
  table: string;
  rows: RowData[];
  isLast: boolean;
}

/** Sync statistics */
export interface SyncStats {
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
}

/** Configuration for a MySQL connection */
export interface MySQLConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database?: string;
  charset?: string;
  connectTimeout?: number;
}

/** Main sync configuration */
export interface SyncConfig {
  source: MySQLConnectionConfig;
  target: MySQLConnectionConfig;

  /** Sync mode: full, incremental, or both */
  mode: 'full' | 'incremental' | 'full+incremental';

  /** Schemas/tables to sync */
  syncObjects: {
    databases?: string[];                        // sync all tables in these databases
    tables?: { database: string; table: string }[]; // specific tables
    excludeTables?: { database: string; table: string }[];
  };

  /** Full sync options */
  fullSync: {
    enabled: boolean;
    workerCount: number;        // concurrent table workers
    batchSize: number;          // rows per SELECT batch
    batchQueueSize: number;     // max queued batches per table
  };

  /** Incremental sync options */
  incrementalSync: {
    enabled: boolean;
    workerCount: number;        // concurrent event appliers
    positionFile: string;       // path to save binlog position
    serverId: number;           // unique slave server ID
  };
}
