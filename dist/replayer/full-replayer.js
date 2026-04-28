import { logger } from '../logger.js';
/**
 * Full-sync Replayer:
 * Writes batches of rows into the target MySQL database.
 * Uses INSERT with batch values and optional REPLACE for conflict handling.
 */
export class FullSyncReplayer {
    pool;
    batchSize;
    constructor(targetPool, config) {
        this.pool = targetPool;
        this.batchSize = config.fullSync.batchSize;
    }
    /**
     * Write a batch of rows to the target table.
     * Uses INSERT IGNORE to skip duplicates (for resumable full sync).
     * Returns the number of rows written.
     */
    async writeBatch(batch) {
        if (batch.rows.length === 0)
            return 0;
        const { database, table } = batch;
        const columns = Object.keys(batch.rows[0]);
        const colList = columns.map(c => `\`${c}\``).join(', ');
        const placeholders = columns.map(() => '?').join(', ');
        // Build batched INSERT
        const rawValues = [];
        for (const row of batch.rows) {
            for (const col of columns) {
                rawValues.push(row[col] ?? null);
            }
        }
        const values = rawValues;
        const valueGroups = [];
        for (let i = 0; i < batch.rows.length; i++) {
            valueGroups.push(`(${placeholders})`);
        }
        const sql = `INSERT IGNORE INTO \`${database}\`.\`${table}\` (${colList}) VALUES ${valueGroups.join(', ')}`;
        try {
            const [result] = await this.pool.execute(sql, values);
            return result.affectedRows;
        }
        catch (err) {
            // Fallback: write rows one by one
            logger.warn(`Batch insert failed for ${database}.${table}, falling back to row-by-row:`, err);
            let written = 0;
            for (const row of batch.rows) {
                try {
                    const rowValues = columns.map(c => row[c] ?? null);
                    await this.pool.execute(`INSERT IGNORE INTO \`${database}\`.\`${table}\` (${colList}) VALUES (${placeholders})`, rowValues);
                    written++;
                }
                catch (rowErr) {
                    logger.error(`Failed to insert row into ${database}.${table}:`, rowErr);
                }
            }
            return written;
        }
    }
    /**
     * Initialize target schema: create database and tables if they don't exist.
     * Copies table structure from source.
     */
    async initTargetSchema(sourcePool, database, table) {
        // Create database if not exists
        await this.pool.query(`CREATE DATABASE IF NOT EXISTS \`${database}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`);
        // Get CREATE TABLE from source
        const [createRows] = await sourcePool.query(`SHOW CREATE TABLE \`${database}\`.\`${table}\``);
        if (createRows.length === 0) {
            throw new Error(`Table ${database}.${table} not found in source`);
        }
        let createSQL = createRows[0]['Create Table'];
        // Prepend database name: CREATE TABLE `db`.`table` (...)
        createSQL = createSQL.replace(/CREATE TABLE\s+/i, `CREATE TABLE IF NOT EXISTS \`${database}\`.`);
        // Remove AUTO_INCREMENT=N to avoid conflicts
        createSQL = createSQL.replace(/AUTO_INCREMENT=\d+\s*/gi, '');
        try {
            await this.pool.query(createSQL);
        }
        catch (err) {
            if (err.errno !== 1050) {
                throw err;
            }
        }
    }
    /**
     * Disable FK checks and enable before bulk insert
     */
    async prepareForBulkInsert() {
        await this.pool.query('SET FOREIGN_KEY_CHECKS = 0');
        await this.pool.query('SET UNIQUE_CHECKS = 0');
        await this.pool.query('SET sql_log_bin = 0');
    }
    /**
     * Re-enable constraints after bulk insert
     */
    async finalizeBulkInsert() {
        await this.pool.query('SET FOREIGN_KEY_CHECKS = 1');
        await this.pool.query('SET UNIQUE_CHECKS = 1');
        await this.pool.query('SET sql_log_bin = 1');
    }
}
//# sourceMappingURL=full-replayer.js.map