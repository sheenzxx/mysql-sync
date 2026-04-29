// Show all tables and their columns in mysql_sync_src
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD
import mysql from 'mysql2/promise';

const c = await mysql.createConnection({
  host: process.env.SOURCE_HOST || '127.0.0.1',
  port: parseInt(process.env.SOURCE_PORT || '3306'),
  user: process.env.SOURCE_USER || 'root',
  password: process.env.SOURCE_PASSWORD || '',
});
const [tables] = await c.query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='mysql_sync_src'");
for (const t of tables) {
  const [cols] = await c.query("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql_sync_src' AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", [t.TABLE_NAME]);
  const colList = cols.map(c => `${c.COLUMN_NAME} (${c.DATA_TYPE}${c.COLUMN_KEY === 'PRI' ? ', PK' : ''})`).join(', ');
  const [cnt] = await c.query(`SELECT COUNT(*) as cnt FROM mysql_sync_src.\`${t.TABLE_NAME}\``);
  console.log(`${t.TABLE_NAME} [${cnt[0].cnt} rows]: ${colList}`);
}
await c.end();
