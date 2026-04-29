// Setup: create a clean test table with simple types
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD
import mysql from 'mysql2/promise';

const conn = await mysql.createConnection({
  host: process.env.SOURCE_HOST || '127.0.0.1',
  port: parseInt(process.env.SOURCE_PORT || '3306'),
  user: process.env.SOURCE_USER || 'root',
  password: process.env.SOURCE_PASSWORD || '',
});

// Drop and recreate test table
await conn.execute('DROP TABLE IF EXISTS mysql_sync_src.test_binlog_simple');
await conn.execute(`
  CREATE TABLE mysql_sync_src.test_binlog_simple (
    id INT NOT NULL,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`);

// Get current binlog position
const [rows] = await conn.execute('SHOW MASTER STATUS');
const masterStatus = rows[0];
console.log('Current binlog position:', masterStatus.File, masterStatus.Position);

// Insert known test rows
await conn.execute("INSERT INTO mysql_sync_src.test_binlog_simple VALUES (1, 'Alice', 'alice@test.com')");
await conn.execute("INSERT INTO mysql_sync_src.test_binlog_simple VALUES (2, 'Bob', 'bob@test.com')");
await conn.execute("INSERT INTO mysql_sync_src.test_binlog_simple VALUES (3, 'Charlie', 'charlie@test.com')");

// Update one row
await conn.execute("UPDATE mysql_sync_src.test_binlog_simple SET name='Alice-v2' WHERE id=1");

// Delete one row
await conn.execute("DELETE FROM mysql_sync_src.test_binlog_simple WHERE id=2");

console.log('Test data inserted. Connect to binlog at:', masterStatus.File, masterStatus.Position);

await conn.end();
