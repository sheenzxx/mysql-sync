// Test the full pipeline: full sync + incremental sync with live changes
import { spawn } from 'child_process';
import mysql from 'mysql2/promise';

const syncProcess = spawn('node', [
  'dist/index.js', 'sync',
  '--source-host', '127.0.0.1', '--source-port', '3306',
  '--source-user', 'dba', '--source-password', 'dba',
  '--target-host', 'test-pub-rm.pc.com.cn', '--target-port', '3307',
  '--target-user', 'nagios', '--target-password', 'supportdb',
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
  host: '127.0.0.1', port: 3306, user: 'dba', password: 'dba',
});

// Make some test changes
await conn.execute("INSERT INTO mysql_sync_src.bt2 (name, email) VALUES ('incr_test_1', 'incr1@test.com')");
await conn.execute("INSERT INTO mysql_sync_src.bt2 (name, email) VALUES ('incr_test_2', 'incr2@test.com')");
await conn.execute("UPDATE mysql_sync_src.bt2 SET name = 'incr_test_1_updated' WHERE name = 'incr_test_1'");
await conn.execute("DELETE FROM mysql_sync_src.bt2 WHERE name = 'incr_test_2'");

console.log('Changes made: 2 INSERTs, 1 UPDATE, 1 DELETE');

// Wait for sync to process
await new Promise(r => setTimeout(r, 5000));

// Verify target has the changes
const targetConn = await mysql.createConnection({
  host: 'test-pub-rm.pc.com.cn', port: 3307, user: 'nagios', password: 'supportdb',
});

const [rows] = await targetConn.query("SELECT * FROM mysql_sync_src.bt2 ORDER BY id");
console.log('\nTarget bt2 table:');
for (const r of rows) {
  console.log(`  id=${r.id}  name=${r.name}  email=${r.email}`);
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
