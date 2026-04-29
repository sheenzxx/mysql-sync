// Raw socket test: dump raw bytes from MySQL COM_BINLOG_DUMP
// Set env vars: SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD
const net = require('net');
const crypto = require('crypto');

const HOST = process.env.SOURCE_HOST || '127.0.0.1';
const PORT = parseInt(process.env.SOURCE_PORT || '3306');
const USER = process.env.SOURCE_USER || 'root';
const PASSWORD = process.env.SOURCE_PASSWORD || '';

function sha1(data) { return crypto.createHash('sha1').update(data).digest(); }

function sha256Hash(seed, password) {
  if (!password) return Buffer.alloc(20);
  const hash1 = sha1(password);
  const hash2 = sha1(hash1);
  const seedPlusHash2 = Buffer.concat([seed.slice(0, 20), hash2]);
  const hash3 = sha1(seedPlusHash2);
  const output = Buffer.alloc(20);
  for (let i = 0; i < 20; i++) output[i] = hash1[i] ^ hash3[i];
  return output;
}

function readNullTermStr(buf, off) {
  const end = buf.indexOf(0, off.value);
  const str = buf.toString('ascii', off.value, end);
  off.value = end + 1;
  return str;
}

function readPacket(sock) {
  return new Promise((resolve) => {
    let buf = Buffer.alloc(0);
    function tryExtract() {
      if (buf.length < 4) return false;
      const len = buf.readUInt8(0) | (buf.readUInt8(1) << 8) | (buf.readUInt8(2) << 16);
      const total = 4 + len;
      if (buf.length < total) return false;
      resolve(buf.slice(0, total));
      return true;
    }
    sock.on('data', (data) => {
      buf = Buffer.concat([buf, data]);
      tryExtract();
    });
  });
}

function writePacket(sock, data, seq) {
  const header = Buffer.alloc(4);
  header[0] = data.length & 0xff;
  header[1] = (data.length >> 8) & 0xff;
  header[2] = (data.length >> 16) & 0xff;
  header[3] = seq;
  return new Promise((resolve) => {
    sock.write(Buffer.concat([header, data]), resolve);
  });
}

async function main() {
  const sock = net.createConnection({ host: HOST, port: PORT });

  await new Promise(resolve => sock.on('connect', resolve));
  console.log('Connected');

  // Read greeting
  const greeting = await readPacket(sock);
  const off = { value: 4 };
  const protoVer = greeting.readUInt8(off.value++);
  console.log('Protocol:', protoVer);
  readNullTermStr(greeting, off); // server version
  const connId = greeting.readUInt32LE(off.value); off.value += 4;
  const authData1 = greeting.slice(off.value, off.value + 8); off.value += 8;
  off.value += 1; // filler
  off.value += 4; // caps + charset + status
  const authLen = greeting.readUInt8(off.value++);
  off.value += 10;
  const part2Len = Math.max(13, authLen - 8);
  const authData2 = greeting.slice(off.value, off.value + part2Len - 1);
  const authPluginData = Buffer.concat([authData1, authData2]);
  console.log('Auth data length:', authPluginData.length);

  // Build handshake response
  const clientFlags = (1<<9) | (1<<15) | (1<<19) | (1<<0) | (1<<13) | (1<<3) | (1<<17);
  const pwHash = sha256Hash(authPluginData, PASSWORD);

  const payload = Buffer.alloc(200);
  let p = 0;
  payload.writeUInt32LE(clientFlags, p); p += 4;
  payload.writeUInt32LE(16777215, p); p += 4;
  payload[p] = 45; p += 1;
  p += 23;
  payload.write(USER, p, 'ascii'); p += USER.length;
  payload[p] = 0; p += 1;
  payload[p] = 20; p += 1;
  pwHash.copy(payload, p); p += 20;
  payload[p] = 0; p += 1;
  payload.write('mysql_native_password', p, 'ascii'); p += 21;
  payload[p] = 0; p += 1;

  await writePacket(sock, payload.slice(0, p), 1);

  const authResp = await readPacket(sock);
  console.log('Auth response first byte:', authResp[4].toString(16));

  // Register slave
  const regBuf = Buffer.alloc(50);
  let ro = 0;
  regBuf[ro] = 0x15; ro += 1; // COM_REGISTER_SLAVE
  regBuf.writeUInt32LE(9997, ro); ro += 4; // server_id
  regBuf[ro] = 0; ro += 1; // hostname
  regBuf[ro] = 0; ro += 1; // user
  regBuf[ro] = 0; ro += 1; // password
  regBuf.writeUInt16LE(0, ro); ro += 2; // port
  regBuf.writeUInt32LE(0, ro); ro += 4; // rank
  regBuf.writeUInt32LE(0, ro); ro += 4; // master_id

  await writePacket(sock, regBuf.slice(0, ro), 0);
  const regResp = await readPacket(sock);
  console.log('Register slave response:', regResp[4].toString(16));

  // Set checksum
  const checksumSql = 'SET @master_binlog_checksum = 0';
  const queryBuf = Buffer.alloc(1 + checksumSql.length);
  queryBuf[0] = 0x03; // COM_QUERY
  queryBuf.write(checksumSql, 1);

  await writePacket(sock, queryBuf, 0);
  const queryResp = await readPacket(sock);
  console.log('Set checksum response:', queryResp[4].toString(16));

  // COM_BINLOG_DUMP
  const filename = 'mysql-bin.000088';
  const pos = 4;
  const dumpBuf = Buffer.alloc(11 + filename.length + 1);
  let doff = 0;
  dumpBuf[doff] = 0x12; doff += 1; // COM_BINLOG_DUMP
  dumpBuf.writeUInt32LE(pos, doff); doff += 4;
  dumpBuf.writeUInt16LE(0, doff); doff += 2; // flags
  dumpBuf.writeUInt32LE(9997, doff); doff += 4; // server_id
  dumpBuf.write(filename, doff, 'ascii'); doff += filename.length;
  dumpBuf[doff] = 0; doff += 1;

  await writePacket(sock, dumpBuf.slice(0, doff), 0);
  console.log('Sent COM_BINLOG_DUMP', filename, pos);

  // Now read raw data after the OK/EOF response
  let rawData = Buffer.alloc(0);

  sock.on('data', (data) => {
    rawData = Buffer.concat([rawData, data]);
    if (rawData.length >= 5000) {
      console.log('\n=== Raw data after COM_BINLOG_DUMP (first 500 bytes) ===');
      for (let i = 0; i < Math.min(500, rawData.length); i += 16) {
        const hex = rawData.slice(i, i + 16).toString('hex').match(/.{1,2}/g).join(' ');
        const ascii = rawData.slice(i, i + 16).toString('ascii').replace(/[^\x20-\x7e]/g, '.');
        console.log(i.toString(16).padStart(6,'0') + '  ' + hex.padEnd(49) + '  ' + ascii);
      }
      console.log('\nTotal raw data bytes:', rawData.length);
      sock.destroy();
      process.exit(0);
    }
  });

  setTimeout(() => {
    console.log('Timeout after 15s, got', rawData.length, 'bytes');
    sock.destroy();
    process.exit(1);
  }, 15000);
}

main().catch(e => { console.error(e); process.exit(1); });
