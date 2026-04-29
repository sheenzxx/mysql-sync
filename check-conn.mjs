// Quick check: connectivity and database list
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD,
//               TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWORD
import mysql from 'mysql2/promise';

const SRC = {
  host: process.env.SOURCE_HOST || '127.0.0.1',
  port: parseInt(process.env.SOURCE_PORT || '3306'),
  user: process.env.SOURCE_USER || 'root',
  password: process.env.SOURCE_PASSWORD || '',
};
const TGT = {
  host: process.env.TARGET_HOST || '127.0.0.1',
  port: parseInt(process.env.TARGET_PORT || '3307'),
  user: process.env.TARGET_USER || 'root',
  password: process.env.TARGET_PASSWORD || '',
};

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

const ok1 = await check('SOURCE', SRC);
const ok2 = await check('TARGET', TGT);
console.log(ok1 && ok2 ? '\nBoth OK' : '\nSOME FAILED');
