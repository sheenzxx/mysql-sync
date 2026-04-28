import mysql from 'mysql2/promise';
import type { MySQLConnectionConfig } from './types.js';
/** Create a connection pool to a MySQL instance */
export declare function createPool(config: MySQLConnectionConfig): mysql.Pool;
/** Get list of databases in the instance */
export declare function getDatabases(pool: mysql.Pool): Promise<string[]>;
/** Get list of tables in a database */
export declare function getTables(pool: mysql.Pool, database: string): Promise<string[]>;
/** Get column info for a table */
export declare function getColumns(pool: mysql.Pool, database: string, table: string): Promise<{
    name: string;
    type: string;
    isNullable: boolean;
    isPrimary: boolean;
    columnType: number;
}[]>;
/** Get primary key columns */
export declare function getPrimaryKeys(pool: mysql.Pool, database: string, table: string): Promise<string[]>;
/** Estimate row count for a table */
export declare function estimateRowCount(pool: mysql.Pool, database: string, table: string): Promise<number>;
/** Get global variable */
export declare function getGlobalVariable(pool: mysql.Pool, name: string): Promise<string>;
/** Check if binlog is enabled on source */
export declare function checkBinlogEnabled(pool: mysql.Pool): Promise<boolean>;
/** Get current binlog position */
export declare function getMasterStatus(pool: mysql.Pool): Promise<{
    filename: string;
    position: number;
} | null>;
/** Check if binlog is ROW format */
export declare function checkBinlogFormat(pool: mysql.Pool): Promise<string>;
/** Close pool */
export declare function closePool(pool: mysql.Pool): Promise<void>;
//# sourceMappingURL=db.d.ts.map