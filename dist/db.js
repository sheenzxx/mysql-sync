import mysql from 'mysql2/promise';
/** Create a connection pool to a MySQL instance */
export function createPool(config) {
    return mysql.createPool({
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        database: config.database,
        charset: config.charset ?? 'utf8mb4',
        connectTimeout: config.connectTimeout ?? 10000,
        waitForConnections: true,
        connectionLimit: 20,
        maxIdle: 5,
        enableKeepAlive: true,
        keepAliveInitialDelay: 30000,
    });
}
/** Get list of databases in the instance */
export async function getDatabases(pool) {
    const [rows] = await pool.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','information_schema','performance_schema','sys')");
    return rows.map(r => r.SCHEMA_NAME);
}
/** Get list of tables in a database */
export async function getTables(pool, database) {
    const [rows] = await pool.query('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = \'BASE TABLE\'', [database]);
    return rows.map(r => r.TABLE_NAME);
}
/** Get column info for a table */
export async function getColumns(pool, database, table) {
    const [colRows] = await pool.query(`SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_TYPE,
            k.COLUMN_NAME IS NOT NULL AS IS_PRIMARY
     FROM information_schema.COLUMNS c
     LEFT JOIN information_schema.KEY_COLUMN_USAGE k
       ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME
      AND c.COLUMN_NAME = k.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'
     WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
     ORDER BY c.ORDINAL_POSITION`, [database, table]);
    // Map DATA_TYPE to MySQL internal type codes (for binlog parsing)
    const typeCodeMap = {
        tinyint: 1, smallint: 2, mediumint: 9, int: 3, bigint: 8,
        float: 4, double: 5, decimal: 246, '': 6,
        timestamp: 7, date: 10, time: 11, datetime: 12, year: 13,
        varchar: 15, char: 254, text: 252, tinytext: 249, mediumtext: 250, longtext: 251,
        blob: 252, tinyblob: 249, mediumblob: 250, longblob: 251,
        binary: 254, varbinary: 15,
        bit: 16, enum: 247, set: 248, json: 245,
        geometry: 255,
    };
    return colRows.map(r => ({
        name: r.COLUMN_NAME,
        type: r.DATA_TYPE,
        isNullable: r.IS_NULLABLE === 'YES',
        isPrimary: r.IS_PRIMARY === 1,
        columnType: typeCodeMap[r.DATA_TYPE] ?? 253,
    }));
}
/** Get primary key columns */
export async function getPrimaryKeys(pool, database, table) {
    const cols = await getColumns(pool, database, table);
    return cols.filter(c => c.isPrimary).map(c => c.name);
}
/** Estimate row count for a table */
export async function estimateRowCount(pool, database, table) {
    try {
        const [rows] = await pool.query('SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?', [database, table]);
        return Number(rows[0]?.TABLE_ROWS ?? 0);
    }
    catch {
        return 0;
    }
}
/** Get global variable */
export async function getGlobalVariable(pool, name) {
    const [rows] = await pool.query(`SHOW GLOBAL VARIABLES LIKE '${name.replace(/'/g, "\\'")}'`);
    return rows[0]?.Value ?? '';
}
/** Check if binlog is enabled on source */
export async function checkBinlogEnabled(pool) {
    const val = await getGlobalVariable(pool, 'log_bin');
    return val === 'ON' || val === '1';
}
/** Get current binlog position */
export async function getMasterStatus(pool) {
    try {
        const [rows] = await pool.query('SHOW MASTER STATUS');
        if (rows.length === 0)
            return null;
        return { filename: rows[0].File, position: rows[0].Position };
    }
    catch {
        return null;
    }
}
/** Check if binlog is ROW format */
export async function checkBinlogFormat(pool) {
    return getGlobalVariable(pool, 'binlog_format');
}
/** Close pool */
export async function closePool(pool) {
    await pool.end();
}
//# sourceMappingURL=db.js.map