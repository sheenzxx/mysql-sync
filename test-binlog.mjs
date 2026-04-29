// Quick test: connect to binlog from position 4
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD
import { BinlogClient } from './dist/binlog-client.js';

const config = {
  host: process.env.SOURCE_HOST || '127.0.0.1',
  port: parseInt(process.env.SOURCE_PORT || '3306'),
  user: process.env.SOURCE_USER || 'root',
  password: process.env.SOURCE_PASSWORD || '',
};

let eventCount = 0;
let rowCount = 0;

const client = new BinlogClient(
  config,
  9998,
  function(change) {
    eventCount++;
    rowCount++;
    console.log('[ROW #' + rowCount + '] ' + change.type + ' on ' + change.database + '.' + change.table);
    if (change.after) console.log('  after:', JSON.stringify(change.after));
    if (rowCount >= 5) {
      console.log('SUCCESS: Got 5 row changes!');
      client.stop();
      process.exit(0);
    }
  },
  function(pos) {
    // console.log('[POS] ' + pos.filename + ':' + pos.position);
  },
);

client.start({ filename: 'mysql-bin.000088', position: 4 })
  .then(function() {
    console.log('Binlog dump started from position 4, waiting for events...');
  })
  .catch(function(err) {
    console.error('FAILED:', err.message);
    process.exit(1);
  });

setTimeout(function() {
  console.log('TIMEOUT: ' + eventCount + ' events, ' + rowCount + ' row changes after 30s');
  client.stop();
  process.exit(rowCount > 0 ? 0 : 1);
}, 30000);
