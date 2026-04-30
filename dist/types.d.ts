/** Database column metadata */
export interface ColumnInfo {
    name: string;
    type: string;
    isNullable: boolean;
    isPrimary: boolean;
    columnType: number;
    metadata: Buffer;
}
/** Table metadata */
export interface TableInfo {
    database: string;
    table: string;
    columns: ColumnInfo[];
    primaryKeys: string[];
    fullName: string;
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
    before?: RowData;
    after?: RowData;
    timestamp: number;
    /** DDL metadata (populated when type === 'ddl') */
    ddl?: {
        ddlType: string;
        sql: string;
        affectedTables?: string[];
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
        databases?: string[];
        tables?: {
            database: string;
            table: string;
        }[];
        excludeTables?: {
            database: string;
            table: string;
        }[];
    };
    /** Full sync options */
    fullSync: {
        enabled: boolean;
        workerCount: number;
        batchSize: number;
        batchQueueSize: number;
    };
    /** Incremental sync options */
    incrementalSync: {
        enabled: boolean;
        workerCount: number;
        positionFile: string;
        serverId: number;
    };
}
//# sourceMappingURL=types.d.ts.map