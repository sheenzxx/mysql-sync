// Test the full pipeline: full sync + incremental sync with live changes
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD,
//               TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWORD
import { spawn } from 'child_process';
import mysql from 'mysql2/promise';

const SRC_HOST = process.env.SOURCE_HOST || '127.0.0.1';
const SRC_PORT = process.env.SOURCE_PORT || '3306';
const SRC_USER = process.env.SOURCE_USER || 'root';
const SRC_PASS = process.env.SOURCE_PASSWORD || '';
const TGT_HOST = process.env.TARGET_HOST || '127.0.0.1';
const TGT_PORT = process.env.TARGET_PORT || '3307';
const TGT_USER = process.env.TARGET_USER || 'root';
const TGT_PASS = process.env.TARGET_PASSWORD || '';

const syncProcess = spawn('node', [
  'dist/index.js', 'sync',
  '--source-host', SRC_HOST, '--source-port', SRC_PORT,
  '--source-user', SRC_USER, '--source-password', SRC_PASS,
  '--target-host', TGT_HOST, '--target-port', TGT_PORT,
  '--target-user', TGT_USER, '--target-password', TGT_PASS,
  '--databases', 'mysql_sync_src',
  '--mode', 'full+incremental',
  '--full-sync-workers', '2',
  '--position-file', './test-position.json',
  '--server-id', '7777',
  '--verbose',
], { cwd: 'D:/mysql-sync' });

let output = '';
syncProcess.stdout.on('data', d => { output += d.toString(); process.stdout.write(d); });
syncProcess.stderr.on('data', d => { process.stderr.write(d); });

// Wait for full sync to complete
await new Promise(r => setTimeout(r, 15000));

console.log('\n=== Full sync should be done. Now inserting test changes... ===\n');

// Connect to source and make changes
const conn = await mysql.createConnection({
  host: SRC_HOST, port: parseInt(SRC_PORT), user: SRC_USER, password: SRC_PASS,
});

// Make some test changes
await conn.execute("INSERT INTO mysql_sync_src.bt2 (name, age) VALUES ('incr_test_1', 100)");
await conn.execute("INSERT INTO mysql_sync_src.bt2 (name, age) VALUES ('incr_test_2', 200)");
await conn.execute("UPDATE mysql_sync_src.bt2 SET name = 'incr_test_1_updated' WHERE name = 'incr_test_1'");
await conn.execute("DELETE FROM mysql_sync_src.bt2 WHERE name = 'incr_test_2'");

console.log('Changes made: 2 INSERTs, 1 UPDATE, 1 DELETE');

// Wait for sync to process
await new Promise(r => setTimeout(r, 8000));

// Verify target has the changes
const targetConn = await mysql.createConnection({
  host: TGT_HOST, port: parseInt(TGT_PORT), user: TGT_USER, password: TGT_PASS,
});

const [rows] = await targetConn.query("SELECT * FROM mysql_sync_src.bt2 ORDER BY id");
console.log('\nTarget bt2 table:');
for (const r of rows) {
  console.log(`  id=${r.id}  name=${r.name}  age=${r.age}`);
}

// Check the sync stats from the output
const statsMatch = output.match(/Incremental sync: \+(\d+) \/ ~(\d+) \/ -(\d+)/g);
if (statsMatch) {
  console.log('\nLast incremental stat:', statsMatch[statsMatch.length - 1]);
}

// Cleanup
syncProcess.kill('SIGINT');
await new Promise(r => setTimeout(r, 2000));

await conn.end();
await targetConn.end();
console.log('\nDone.');
