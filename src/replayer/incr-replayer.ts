import mysql from 'mysql2/promise';
import type { Pool } from 'mysql2/promise';
import type { RowChange, RowData, SyncConfig } from '../types.js';
import { logger } from '../logger.js';

/**
 * Incremental-sync Replayer:
 * Applies RowChange events (insert/update/delete) to the target database.
 * Supports concurrent execution for higher throughput.
 */
export class IncrementalReplayer {
  private pool: Pool;

  constructor(targetPool: Pool, config: SyncConfig) {
    this.pool = targetPool;
  }

  /**
   * Apply a single row change to the target.
   * Maps column indices back to column names by querying table metadata.
   */
  async apply(change: RowChange): Promise<void> {
    switch (change.type) {
      case 'insert':
        await this.applyInsert(change);
        break;
      case 'update':
        await this.applyUpdate(change);
        break;
      case 'delete':
        await this.applyDelete(change);
        break;
    }
  }

  /**
   * Apply a batch of changes in a single transaction for efficiency.
   */
  async applyBatch(changes: RowChange[]): Promise<void> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();
      for (const change of changes) {
        const columns = await this.getTableColumns(change.database, change.table);
        const pkColumns = columns.filter(c => c.isPrimary).map(c => c.name);
        const colNames = columns.map(c => c.name);

        switch (change.type) {
          case 'insert': {
            const mapped = mapRowToColumns(change.after ?? {}, colNames);
            if (Object.keys(mapped).length === 0) continue;
            await this.buildInsert(conn, change.database, change.table, mapped);
            break;
          }
          case 'update': {
            const beforeMapped = mapRowToColumns(change.before ?? {}, colNames);
            const afterMapped = mapRowToColumns(change.after ?? {}, colNames);
            await this.buildUpdate(conn, change.database, change.table, beforeMapped, afterMapped, pkColumns);
            break;
          }
          case 'delete': {
            const beforeMapped = mapRowToColumns(change.before ?? {}, colNames);
            await this.buildDelete(conn, change.database, change.table, beforeMapped, pkColumns);
            break;
          }
        }
      }
      await conn.commit();
    } catch (err) {
      await conn.rollback();
      // Fallback: apply individually
      logger.warn('Batch apply failed, falling back to individual:');
      for (const change of changes) {
        try {
          await this.apply(change);
        } catch (e) {
          logger.error(`Failed to apply change:`, change, e);
        }
      }
    } finally {
      conn.release();
    }
  }

  private async applyInsert(change: RowChange): Promise<void> {
    const columns = await this.getTableColumns(change.database, change.table);
    const colNames = columns.map(c => c.name);

    const after = change.after ?? {};
    const mapped = mapRowToColumns(after, colNames);

    if (Object.keys(mapped).length === 0) return;
    await this.buildInsert(this.pool, change.database, change.table, mapped);
  }

  private async applyUpdate(change: RowChange): Promise<void> {
    const columns = await this.getTableColumns(change.database, change.table);
    const pkColumns = columns.filter(c => c.isPrimary).map(c => c.name);
    const colNames = columns.map(c => c.name);

    const before = change.before ?? {};
    const after = change.after ?? {};
    const beforeMapped = mapRowToColumns(before, colNames);
    const afterMapped = mapRowToColumns(after, colNames);

    await this.buildUpdate(this.pool, change.database, change.table, beforeMapped, afterMapped, pkColumns);
  }

  private async applyDelete(change: RowChange): Promise<void> {
    const columns = await this.getTableColumns(change.database, change.table);
    const pkColumns = columns.filter(c => c.isPrimary).map(c => c.name);
    const colNames = columns.map(c => c.name);

    const before = change.before ?? {};
    const beforeMapped = mapRowToColumns(before, colNames);

    await this.buildDelete(this.pool, change.database, change.table, beforeMapped, pkColumns);
  }

  // -----------------------------------------------------------------------
  // SQL builders
  // -----------------------------------------------------------------------

  private async buildInsert(
    conn: Pool | mysql.Connection,
    database: string,
    table: string,
    row: RowData,
  ): Promise<void> {
    const columns = Object.keys(row);
    if (columns.length === 0) return;

    const colList = columns.map(c => `\`${c}\``).join(', ');
    const placeholders = columns.map(() => '?').join(', ');
    const values = columns.map(c => row[c] ?? null);

    const sql = `INSERT IGNORE INTO \`${database}\`.\`${table}\` (${colList}) VALUES (${placeholders})`;
    await conn.execute(sql, values as mysql.ExecuteValues);
  }

  private async buildUpdate(
    conn: Pool | mysql.Connection,
    database: string,
    table: string,
    before: RowData,
    after: RowData,
    pkColumns: string[],
  ): Promise<void> {
    const setColumns = Object.keys(after).filter(c => c !== undefined);
    if (setColumns.length === 0) return;

    const setClauses = setColumns.map(c => `\`${c}\` = ?`).join(', ');

    // Build WHERE clause from primary key or all columns
    let whereClause: string;
    let whereValues: unknown[];

    if (pkColumns.length > 0) {
      whereClause = pkColumns.map(c => `\`${c}\` = ?`).join(' AND ');
      whereValues = pkColumns.map(c => before[c] !== undefined ? before[c] : after[c] ?? null);
    } else {
      const allCols = Object.keys(before).length > 0 ? Object.keys(before) : Object.keys(after);
      whereClause = allCols.map(c => `\`${c}\` = ?`).join(' AND ');
      whereValues = allCols.map(c => before[c] !== undefined ? before[c] : null);
    }

    const setValues = setColumns.map(c => after[c] ?? null);
    const sql = `UPDATE \`${database}\`.\`${table}\` SET ${setClauses} WHERE ${whereClause}`;
    await conn.execute(sql, [...setValues, ...whereValues] as mysql.ExecuteValues);
  }

  private async buildDelete(
    conn: Pool | mysql.Connection,
    database: string,
    table: string,
    row: RowData,
    pkColumns: string[],
  ): Promise<void> {
    let whereClause: string;
    let whereValues: unknown[];

    if (pkColumns.length > 0) {
      whereClause = pkColumns.map(c => `\`${c}\` = ?`).join(' AND ');
      whereValues = pkColumns.map(c => row[c] ?? null);
    } else {
      const cols = Object.keys(row);
      whereClause = cols.map(c => `\`${c}\` = ?`).join(' AND ');
      whereValues = cols.map(c => row[c] ?? null);
    }

    const sql = `DELETE FROM \`${database}\`.\`${table}\` WHERE ${whereClause}`;
    await conn.execute(sql, whereValues as mysql.ExecuteValues);
  }

  /** Column info cache */
  private columnCache = new Map<string, { name: string; type: string; isPrimary: boolean }[]>();

  /** Pre-warm the column cache for a table (avoids slow first query during replay) */
  async warmCache(database: string, table: string): Promise<void> {
    await this.getTableColumns(database, table);
  }

  private async getTableColumns(database: string, table: string): Promise<{ name: string; type: string; isPrimary: boolean }[]> {
    const key = `${database}.${table}`;
    if (this.columnCache.has(key)) {
      return this.columnCache.get(key) as any;
    }

    const [rows] = await this.pool.query<mysql.RowDataPacket[]>(
      `SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
              k.COLUMN_NAME IS NOT NULL AS IS_PRIMARY
       FROM information_schema.COLUMNS c
       LEFT JOIN information_schema.KEY_COLUMN_USAGE k
         ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME
        AND c.COLUMN_NAME = k.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'
       WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
       ORDER BY c.ORDINAL_POSITION`,
      [database, table],
    );

    const result = rows.map(r => ({
      name: r.COLUMN_NAME as string,
      type: r.DATA_TYPE as string,
      isPrimary: r.IS_PRIMARY === 1,
    }));

    this.columnCache.set(key, result);
    return result;
  }
}

/**
 * Map binlog row data (indexed by stringified column position) to
 * named columns.  Uses the actual column index as the key so that
 * sparse row images (e.g. MINIMAL binlog_row_image) are handled
 * correctly.
 */
function mapRowToColumns(row: RowData, colNames: string[]): RowData {
  const mapped: RowData = {};
  for (const [key, value] of Object.entries(row)) {
    const idx = Number(key);
    if (!isNaN(idx) && idx >= 0 && idx < colNames.length) {
      mapped[colNames[idx]] = value;
    } else {
      // Already a named key (should not happen with binlog source,
      // but keeps the door open for other sources).
      mapped[key] = value;
    }
  }
  return mapped;
}
