// Quick check: connectivity and database list
import mysql from 'mysql2/promise';

async function check(label, config) {
  try {
    const conn = await mysql.createConnection(config);
    const [dbs] = await conn.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','information_schema','performance_schema','sys')");
    console.log(`[${label}] OK - ${dbs.length} user databases:`, dbs.map(r => r.SCHEMA_NAME).join(', '));
    await conn.end();
    return true;
  } catch (e) {
    console.error(`[${label}] FAIL:`, e.message);
    return false;
  }
}

const ok1 = await check('SOURCE', { host: '127.0.0.1', port: 3306, user: 'dba', password: 'dba' });
const ok2 = await check('TARGET', { host: 'test-pub-rm.pc.com.cn', port: 3307, user: 'nagios', password: 'supportdb' });
console.log(ok1 && ok2 ? '\nBoth OK' : '\nSOME FAILED');
