// Clean test: binlog from a known position with simple schema
import { BinlogClient } from './dist/binlog-client.js';

const config = {
  host: '127.0.0.1',
  port: 3306,
  user: 'dba',
  password: 'dba',
};

// Read position from command line args
const filename = process.argv[2] || 'mysql-bin.000088';
const position = parseInt(process.argv[3] || '4', 10);

let eventCount = 0;
let rowCount = 0;

const client = new BinlogClient(
  config,
  9999,
  function(change) {
    eventCount++;
    rowCount++;
    console.log(`\n[ROW #${rowCount}] ${change.type.toUpperCase()} on ${change.database}.${change.table}`);
    if (change.before) console.log('  before:', JSON.stringify(change.before));
    if (change.after) console.log('  after:', JSON.stringify(change.after));

    if (rowCount >= 5) {
      console.log('\nSUCCESS: Got 5 row changes!');
      client.stop();
      process.exit(0);
    }
  },
  function(pos) {
    console.log('[POS] ' + pos.filename + ':' + pos.position);
  },
);

console.log(`Starting binlog dump from ${filename}:${position}`);
client.start({ filename, position })
  .then(function() {
    console.log('Binlog dump started, waiting for events...');
  })
  .catch(function(err) {
    console.error('FAILED:', err.message);
    process.exit(1);
  });

setTimeout(function() {
  console.log('\nTIMEOUT: ' + eventCount + ' events, ' + rowCount + ' row changes after 30s');
  client.stop();
  process.exit(rowCount > 0 ? 0 : 1);
}, 30000);
