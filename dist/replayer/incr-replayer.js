import { logger } from '../logger.js';
/**
 * Incremental-sync Replayer:
 * Applies RowChange events (insert/update/delete) to the target database.
 * Supports concurrent execution for higher throughput.
 */
export class IncrementalReplayer {
    pool;
    constructor(targetPool, config) {
        this.pool = targetPool;
    }
    /**
     * Apply a single row change to the target.
     * Maps column indices back to column names by querying table metadata.
     */
    async apply(change) {
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
            case 'ddl':
                await this.applyDdl(change);
                break;
        }
    }
    /**
     * Apply a batch of changes in a single transaction for efficiency.
     */
    async applyBatch(changes) {
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
                        if (Object.keys(mapped).length === 0)
                            continue;
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
                    case 'ddl':
                        // DDL cannot be batched — rollback and apply individually
                        throw new Error('DDL cannot be batched');
                }
            }
            await conn.commit();
        }
        catch (err) {
            await conn.rollback();
            // Fallback: apply individually
            logger.warn('Batch apply failed, falling back to individual:');
            for (const change of changes) {
                try {
                    await this.apply(change);
                }
                catch (e) {
                    logger.error(`Failed to apply change:`, change, e);
                }
            }
        }
        finally {
            conn.release();
        }
    }
    async applyInsert(change) {
        const columns = await this.getTableColumns(change.database, change.table);
        const colNames = columns.map(c => c.name);
        const after = change.after ?? {};
        const mapped = mapRowToColumns(after, colNames);
        if (Object.keys(mapped).length === 0)
            return;
        await this.buildInsert(this.pool, change.database, change.table, mapped);
    }
    async applyUpdate(change) {
        const columns = await this.getTableColumns(change.database, change.table);
        const pkColumns = columns.filter(c => c.isPrimary).map(c => c.name);
        const colNames = columns.map(c => c.name);
        const before = change.before ?? {};
        const after = change.after ?? {};
        const beforeMapped = mapRowToColumns(before, colNames);
        const afterMapped = mapRowToColumns(after, colNames);
        await this.buildUpdate(this.pool, change.database, change.table, beforeMapped, afterMapped, pkColumns);
    }
    async applyDelete(change) {
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
    async buildInsert(conn, database, table, row) {
        const columns = Object.keys(row);
        if (columns.length === 0)
            return;
        const colList = columns.map(c => `\`${c}\``).join(', ');
        const placeholders = columns.map(() => '?').join(', ');
        const values = columns.map(c => row[c] ?? null);
        const sql = `INSERT IGNORE INTO \`${database}\`.\`${table}\` (${colList}) VALUES (${placeholders})`;
        await conn.execute(sql, values);
    }
    async buildUpdate(conn, database, table, before, after, pkColumns) {
        const setColumns = Object.keys(after).filter(c => c !== undefined);
        if (setColumns.length === 0)
            return;
        const setClauses = setColumns.map(c => `\`${c}\` = ?`).join(', ');
        // Build WHERE clause from primary key or all columns
        let whereClause;
        let whereValues;
        if (pkColumns.length > 0) {
            whereClause = pkColumns.map(c => `\`${c}\` = ?`).join(' AND ');
            whereValues = pkColumns.map(c => before[c] !== undefined ? before[c] : after[c] ?? null);
        }
        else {
            const allCols = Object.keys(before).length > 0 ? Object.keys(before) : Object.keys(after);
            whereClause = allCols.map(c => `\`${c}\` = ?`).join(' AND ');
            whereValues = allCols.map(c => before[c] !== undefined ? before[c] : null);
        }
        const setValues = setColumns.map(c => after[c] ?? null);
        const sql = `UPDATE \`${database}\`.\`${table}\` SET ${setClauses} WHERE ${whereClause}`;
        await conn.execute(sql, [...setValues, ...whereValues]);
    }
    async buildDelete(conn, database, table, row, pkColumns) {
        let whereClause;
        let whereValues;
        if (pkColumns.length > 0) {
            whereClause = pkColumns.map(c => `\`${c}\` = ?`).join(' AND ');
            whereValues = pkColumns.map(c => row[c] ?? null);
        }
        else {
            const cols = Object.keys(row);
            whereClause = cols.map(c => `\`${c}\` = ?`).join(' AND ');
            whereValues = cols.map(c => row[c] ?? null);
        }
        const sql = `DELETE FROM \`${database}\`.\`${table}\` WHERE ${whereClause}`;
        await conn.execute(sql, whereValues);
    }
    /** Column info cache */
    columnCache = new Map();
    /** Pre-warm the column cache for a table (avoids slow first query during replay) */
    async warmCache(database, table) {
        await this.getTableColumns(database, table);
    }
    /** Apply a DDL statement to the target database */
    async applyDdl(change) {
        const ddl = change.ddl;
        if (!ddl)
            return;
        logger.info(`Applying DDL on target: ${ddl.ddlType} [${change.database}.${change.table || '(global)'}]`);
        // Use a dedicated connection to ensure correct database context
        const conn = await this.pool.getConnection();
        try {
            if (change.database) {
                await conn.query(`USE \`${change.database}\``);
            }
            await conn.query(ddl.sql);
            logger.info(`DDL applied successfully: ${ddl.ddlType}`);
        }
        finally {
            conn.release();
        }
        // Invalidate column cache for affected tables
        const tables = ddl.affectedTables && ddl.affectedTables.length > 0
            ? ddl.affectedTables
            : change.table ? [change.table] : [];
        for (const tbl of tables) {
            if (tbl)
                this.invalidateCache(change.database, tbl);
        }
    }
    /** Invalidate column cache for a table (call after DDL that modifies schema) */
    invalidateCache(database, table) {
        const key = `${database}.${table}`;
        this.columnCache.delete(key);
        logger.debug(`Column cache invalidated for ${key}`);
    }
    async getTableColumns(database, table) {
        const key = `${database}.${table}`;
        if (this.columnCache.has(key)) {
            return this.columnCache.get(key);
        }
        const [rows] = await this.pool.query(`SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
              k.COLUMN_NAME IS NOT NULL AS IS_PRIMARY
       FROM information_schema.COLUMNS c
       LEFT JOIN information_schema.KEY_COLUMN_USAGE k
         ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME
        AND c.COLUMN_NAME = k.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'
       WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
       ORDER BY c.ORDINAL_POSITION`, [database, table]);
        const result = rows.map(r => ({
            name: r.COLUMN_NAME,
            type: r.DATA_TYPE,
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
function mapRowToColumns(row, colNames) {
    const mapped = {};
    for (const [key, value] of Object.entries(row)) {
        const idx = Number(key);
        if (!isNaN(idx) && idx >= 0 && idx < colNames.length) {
            mapped[colNames[idx]] = value;
        }
        else {
            // Already a named key (should not happen with binlog source,
            // but keeps the door open for other sources).
            mapped[key] = value;
        }
    }
    return mapped;
}
//# sourceMappingURL=incr-replayer.js.map