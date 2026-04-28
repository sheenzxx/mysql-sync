import mysql from 'mysql2/promise';
import type { Pool } from 'mysql2/promise';
import type { FullSyncTask, FullSyncBatch, RowData, SyncConfig } from '../types.js';
import { logger } from '../logger.js';

/**
 * Full-Sync Collector:
 * Reads all rows from a source table using keyset pagination (primary key),
 * yields batches of rows to be consumed by the replayer.
 */
export class FullSyncCollector {
  private pool: Pool;

  constructor(sourcePool: Pool, _config: SyncConfig) {
    this.pool = sourcePool;
  }

  /**
   * Given a task, stream batches of rows through the returned async iterator.
   * Uses keyset pagination on the primary key for efficient batching.
   */
  async *collect(task: FullSyncTask): AsyncGenerator<FullSyncBatch> {
    const { database, table, columns, primaryKeys, batchSize } = task;

    if (primaryKeys.length === 0) {
      logger.warn(`Table ${database}.${table} has no primary key, using LIMIT/OFFSET`);
      yield* this.collectWithLimitOffset(database, table, columns, batchSize);
      return;
    }

    const pk = primaryKeys[0];
    logger.info(`Full sync: ${database}.${table} (pk=${pk}, batch=${batchSize})`);

    let lastValue: unknown = null;
    let hasMore = true;
    let totalRows = 0;
    const colList = columns.map(c => `\`${c}\``).join(', ');

    while (hasMore) {
      const query = lastValue === null
        ? `SELECT ${colList} FROM \`${database}\`.\`${table}\` ORDER BY \`${pk}\` LIMIT ?`
        : `SELECT ${colList} FROM \`${database}\`.\`${table}\` WHERE \`${pk}\` > ? ORDER BY \`${pk}\` LIMIT ?`;

      const params = lastValue === null ? [batchSize] : [lastValue, batchSize];
      const [rows] = await this.pool.query<mysql.RowDataPacket[]>(query, params);

      if (rows.length === 0) {
        yield { database, table, rows: [], isLast: true };
        break;
      }

      const rowData: RowData[] = rows.map(r => ({ ...r }) as RowData);
      totalRows += rowData.length;
      lastValue = rows[rows.length - 1][pk];
      hasMore = rows.length >= batchSize;

      yield { database, table, rows: rowData, isLast: !hasMore };

      if (totalRows % 10000 === 0) {
        logger.info(`  ${database}.${table}: ${totalRows} rows collected`);
      }
    }

    logger.info(`  ${database}.${table} complete: ${totalRows} rows`);
  }

  /** Fallback for tables without primary key */
  private async *collectWithLimitOffset(
    database: string,
    table: string,
    columns: string[],
    batchSize: number,
  ): AsyncGenerator<FullSyncBatch> {
    const colList = columns.map(c => `\`${c}\``).join(', ');
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const [rows] = await this.pool.query<mysql.RowDataPacket[]>(
        `SELECT ${colList} FROM \`${database}\`.\`${table}\` LIMIT ? OFFSET ?`,
        [batchSize, offset],
      );

      if (rows.length === 0) {
        yield { database, table, rows: [], isLast: true };
        break;
      }

      const rowData: RowData[] = rows.map(r => ({ ...r }) as RowData);
      offset += rows.length;
      hasMore = rows.length >= batchSize;

      yield { database, table, rows: rowData, isLast: !hasMore };
    }
  }
}
