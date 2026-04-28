import { createHash } from 'node:crypto';
import { createConnection } from 'node:net';
import type { MySQLConnectionConfig, BinlogPosition, RowChange, RowData } from './types.js';
import { logger } from './logger.js';

// ---------------------------------------------------------------------------
// MySQL protocol constants
// ---------------------------------------------------------------------------
const COM_QUERY = 0x03;
const COM_REGISTER_SLAVE = 0x15;
const COM_BINLOG_DUMP = 0x12;

// Binlog event types
enum EventType {
  UNKNOWN = 0,
  START_EVENT_V3 = 1,
  QUERY_EVENT = 2,
  STOP_EVENT = 3,
  ROTATE_EVENT = 4,
  FORMAT_DESCRIPTION_EVENT = 15,
  TABLE_MAP_EVENT = 19,
  WRITE_ROWS_EVENT_V1 = 23,
  UPDATE_ROWS_EVENT_V1 = 24,
  DELETE_ROWS_EVENT_V1 = 25,
  WRITE_ROWS_EVENT_V2 = 30,
  UPDATE_ROWS_EVENT_V2 = 31,
  DELETE_ROWS_EVENT_V2 = 32,
  GTID_EVENT = 33,
  XID_EVENT = 16,
}

// MySQL column type codes
const enum ColType {
  DECIMAL = 0, TINY = 1, SHORT = 2, LONG = 3, FLOAT = 4,
  DOUBLE = 5, NULL = 6, TIMESTAMP = 7, LONGLONG = 8, INT24 = 9,
  DATE = 10, TIME = 11, DATETIME = 12, YEAR = 13, NEWDATE = 14,
  VARCHAR = 15, BIT = 16, TIMESTAMP2 = 17, DATETIME2 = 18, TIME2 = 19,
  JSON = 245, NEWDECIMAL = 246, ENUM = 247, SET = 248,
  TINY_BLOB = 249, MEDIUM_BLOB = 250, LONG_BLOB = 251, BLOB = 252,
  VAR_STRING = 253, STRING = 254, GEOMETRY = 255,
}

// ---------------------------------------------------------------------------
// MySQL Protocol helpers
// ---------------------------------------------------------------------------

function leReadUint8(buf: Buffer, off: number): number {
  return buf.readUInt8(off);
}

function leReadUint16(buf: Buffer, off: number): number {
  return buf.readUInt16LE(off);
}

function leReadUint24(buf: Buffer, off: number): number {
  return buf.readUInt8(off) | (buf.readUInt8(off + 1) << 8) | (buf.readUInt8(off + 2) << 16);
}

function leReadUint32(buf: Buffer, off: number): number {
  return buf.readUInt32LE(off);
}

function leReadUint48(buf: Buffer, off: number): number {
  const lo = buf.readUInt32LE(off);
  const hi = buf.readUInt16LE(off + 4);
  return hi * 0x100000000 + lo;
}

function leReadInt64(buf: Buffer, off: number): bigint {
  return buf.readBigInt64LE(off);
}

function writeUint24(buf: Buffer, off: number, val: number): void {
  buf[off] = val & 0xff;
  buf[off + 1] = (val >> 8) & 0xff;
  buf[off + 2] = (val >> 16) & 0xff;
}

function leWriteUint32(buf: Buffer, off: number, val: number): void {
  buf.writeUInt32LE(val, off);
}

/** Read a length-encoded integer (MySQL packet format) */
function readLenencInt(buf: Buffer, off: { value: number }): number {
  const first = buf.readUInt8(off.value);
  off.value += 1;
  if (first < 0xfb) return first;
  if (first === 0xfc) {
    const val = buf.readUInt16LE(off.value);
    off.value += 2;
    return val;
  }
  if (first === 0xfd) {
    const val = leReadUint24(buf, off.value);
    off.value += 3;
    return val;
  }
  if (first === 0xfe) {
    const val = Number(buf.readBigUInt64LE(off.value));
    off.value += 8;
    return val;
  }
  return 0;
}

/** Read a length-encoded string */
function readLenencStr(buf: Buffer, off: { value: number }): string {
  const len = readLenencInt(buf, off);
  const str = buf.toString('utf8', off.value, off.value + len);
  off.value += len;
  return str;
}

/** Read a null-terminated string */
function readNullTermStr(buf: Buffer, off: { value: number }): string {
  const end = buf.indexOf(0, off.value);
  if (end < 0) {
    const str = buf.toString('utf8', off.value);
    off.value = buf.length;
    return str;
  }
  const str = buf.toString('utf8', off.value, end);
  off.value = end + 1;
  return str;
}

/** Read a fixed-length string */
function readFixedLenStr(buf: Buffer, off: { value: number }, len: number): string {
  const str = buf.toString('utf8', off.value, off.value + len);
  off.value += len;
  return str;
}

/** MySQL native_password hash: XOR(SHA1(password), SHA1(seed + SHA1(SHA1(password)))) */
function sha256Hash(seed: Buffer, password: string): Buffer {
  if (!password) return Buffer.alloc(20, 0);
  const hash1 = sha1(password);
  const hash2 = sha1(hash1);
  const seedPlusHash2 = Buffer.concat([seed.slice(0, 20), hash2]);
  const hash3 = sha1(seedPlusHash2);
  const output = Buffer.alloc(20);
  for (let i = 0; i < 20; i++) output[i] = hash1[i] ^ hash3[i];
  return output;
}

function sha1(data: string | Buffer): Buffer {
  return createHash('sha1').update(data).digest();
}

// ---------------------------------------------------------------------------
// Binary log event header (19 bytes)
// ---------------------------------------------------------------------------
interface EventHeader {
  timestamp: number;
  type: EventType;
  serverId: number;
  eventLength: number;
  nextPos: number;
  flags: number;
}

function parseEventHeader(buf: Buffer, off: number): EventHeader {
  return {
    timestamp: leReadUint32(buf, off),
    type: buf.readUInt8(off + 4) as EventType,
    serverId: leReadUint32(buf, off + 5),
    eventLength: leReadUint32(buf, off + 9),
    nextPos: leReadUint32(buf, off + 13),
    flags: leReadUint16(buf, off + 17),
  };
}

// ---------------------------------------------------------------------------
// Table map cache
// ---------------------------------------------------------------------------
interface TableMapEntry {
  database: string;
  table: string;
  colTypes: number[];
  colMetadata: Buffer[];
  colCount: number;
  nullBitmapSize: number;
}

// ---------------------------------------------------------------------------
// Binlog client
// ---------------------------------------------------------------------------

/** Callback for row changes */
export type RowChangeCallback = (change: RowChange) => void | Promise<void>;

export class BinlogClient {
  private socket: import('node:net').Socket | null = null;
  private config: MySQLConnectionConfig;
  private serverId: number;
  private buffer = Buffer.alloc(0);

  private tableMap: Map<number, TableMapEntry> = new Map();
  private rowsEventBuf = Buffer.alloc(0);

  /** Position tracking */
  private currentFilename = '';
  private currentPosition = 0;

  private onRowChange: RowChangeCallback;
  private onPosition?: (pos: BinlogPosition) => void;

  private stopped = false;
  private binlogStreamActive = false;
  private _readPacketResolve: (() => void) | null = null;
  private crc32Enabled = false; // detected from FORMAT_DESCRIPTION_EVENT

  constructor(
    config: MySQLConnectionConfig,
    serverId: number,
    onRowChange: RowChangeCallback,
    onPosition?: (pos: BinlogPosition) => void,
  ) {
    this.config = config;
    this.serverId = serverId;
    this.onRowChange = onRowChange;
    this.onPosition = onPosition;
  }

  /** Start binlog streaming from a given position */
  async start(from: BinlogPosition): Promise<void> {
    this.currentFilename = from.filename;
    this.currentPosition = from.position;
    this.stopped = false;

    logger.info(`Connecting to binlog at ${from.filename}:${from.position} ...`);

    return new Promise((resolve, reject) => {
      const sock = createConnection({ port: this.config.port, host: this.config.host });
      this.socket = sock;
      this.binlogStreamActive = false;

      sock.on('error', (err) => {
        if (!this.stopped) {
          logger.error('Binlog socket error:', err.message);
          reject(err);
        }
      });

      sock.on('close', () => {
        if (!this.stopped) {
          logger.warn('Binlog connection closed unexpectedly');
        }
      });

      // Single streaming data handler from the start.
      // readPacket() pulls complete MySQL packets from the same buffer,
      // avoiding the dual-consumption problem.
      sock.on('data', (data) => this.onData(data));

      sock.on('connect', () => {
        this.performHandshake()
          .then(() => this.sendQuery("SET @master_binlog_checksum = 0"))
          .then(() => this.registerSlave())
          .then(() => this.dumpBinlog(from))
          .then(() => {
            logger.info('Binlog dump started, streaming events...');
            resolve();
          })
          .catch(reject);
      });
    });
  }

  stop(): void {
    this.stopped = true;
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }

  get isRunning(): boolean {
    return this.socket !== null && !this.stopped;
  }

  getCurrentPosition(): BinlogPosition {
    return { filename: this.currentFilename, position: this.currentPosition };
  }

  // -----------------------------------------------------------------------
  // Socket data handling
  // -----------------------------------------------------------------------
  private onData(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, data]);
    // Notify readPacket if it's waiting for more data
    if (this._readPacketResolve) {
      this._readPacketResolve();
    }
    this.processPackets();
  }

  /* debug helper: log event type names */
  private eventTypeName(t: number): string {
    const names: Record<number, string> = {
      2: 'QUERY', 4: 'ROTATE', 15: 'FORMAT_DESC', 16: 'XID',
      19: 'TABLE_MAP', 23: 'WRITE_V1', 24: 'UPDATE_V1', 25: 'DELETE_V1',
      30: 'WRITE_V2', 31: 'UPDATE_V2', 32: 'DELETE_V2', 33: 'GTID',
    };
    return names[t] || `EVENT_${t}`;
  }

  private processPackets(): void {
    // Before binlog dump is established, packets are consumed by readPacket.
    // Don't even parse the buffer — readPacket() needs the raw data.
    if (!this.binlogStreamActive) return;

    while (this.buffer.length >= 4) {
      const packetLen = leReadUint24(this.buffer, 0);
      const totalLen = 4 + packetLen;

      if (this.buffer.length < totalLen) break;


      const payload = this.buffer.subarray(4, totalLen);
      this.buffer = this.buffer.subarray(totalLen);

      // Error packet
      if (payload.length > 0 && payload[0] === 0xff) {
        const errCode = leReadUint16(payload, 1);
        const errMsg = payload.toString('utf8', 3);
        logger.error(`MySQL error ${errCode}: ${errMsg}`);
        continue;
      }

      // All packets after dump are binlog event payloads.
      this.handleEventPayload(payload);
    }
  }

  private handleEventPayload(payload: Buffer): void {
    // MySQL non-blocking binlog protocol: each event is prefixed with
    // a 0x00 OK indicator byte. Strip it to get the raw binlog event.
    if (payload.length > 0 && payload[0] === 0x00) {
      payload = payload.subarray(1);
    }
    this.rowsEventBuf = Buffer.concat([this.rowsEventBuf, payload]);

    while (this.rowsEventBuf.length >= 19) {
      const hdr = parseEventHeader(this.rowsEventBuf, 0);
      if (this.rowsEventBuf.length < hdr.eventLength) break;

      const eventBuf = this.rowsEventBuf.subarray(0, hdr.eventLength);
      this.rowsEventBuf = this.rowsEventBuf.subarray(hdr.eventLength);
      // Strip 4-byte CRC32 checksum if present
      const dataLen = this.crc32Enabled ? eventBuf.length - 4 : eventBuf.length;
      const dataBuf = eventBuf.subarray(0, dataLen);

      try {
        this.processEvent(hdr, dataBuf);
      } catch (err: any) {
        logger.error(`Error processing event type ${hdr.type}: ${err.message}`);
      }

      if (hdr.nextPos > 0) {
        this.currentPosition = hdr.nextPos;
        if (this.onPosition) {
          this.onPosition({ filename: this.currentFilename, position: this.currentPosition });
        }
      }
    }
  }

  // -----------------------------------------------------------------------
  // Event processing
  // -----------------------------------------------------------------------
  private processEvent(hdr: EventHeader, buf: Buffer): void {
    // Skip high-frequency noise events unless debug
    if (hdr.type !== EventType.TABLE_MAP_EVENT && hdr.type !== EventType.FORMAT_DESCRIPTION_EVENT) {
      logger.debug(`Event: ${this.eventTypeName(hdr.type)} len=${hdr.eventLength} next=${hdr.nextPos}`);
    }
    switch (hdr.type) {
      case EventType.ROTATE_EVENT:
        this.processRotate(buf);
        break;
      case EventType.FORMAT_DESCRIPTION_EVENT:
        this.processFormatDesc(buf);
        break;
      case EventType.TABLE_MAP_EVENT:
        this.processTableMap(buf);
        break;
      case EventType.WRITE_ROWS_EVENT_V1:
      case EventType.WRITE_ROWS_EVENT_V2:
        this.processRowsEvent(buf, 'insert', hdr.type === EventType.WRITE_ROWS_EVENT_V1 ? 1 : 2);
        break;
      case EventType.UPDATE_ROWS_EVENT_V1:
      case EventType.UPDATE_ROWS_EVENT_V2:
        this.processRowsEvent(buf, 'update', hdr.type === EventType.UPDATE_ROWS_EVENT_V1 ? 1 : 2);
        break;
      case EventType.DELETE_ROWS_EVENT_V1:
      case EventType.DELETE_ROWS_EVENT_V2:
        this.processRowsEvent(buf, 'delete', hdr.type === EventType.DELETE_ROWS_EVENT_V1 ? 1 : 2);
        break;
      case EventType.XID_EVENT:
        // Transaction commit — position was already updated via nextPos
        break;
      case EventType.GTID_EVENT:
        // GTID info — we don't need it for basic sync
        break;
      default:
        // Ignore other events
        break;
    }
  }

  /** Detect CRC32 from FORMAT_DESCRIPTION_EVENT (first event in binlog). */
  private processFormatDesc(buf: Buffer): void {
    // In MySQL 5.6+, the checksum algorithm indicator is always present:
    //   - If CRC32 is enabled:  checksum_alg byte at buf.length-5, last 4 bytes = CRC32
    //   - If CRC32 is disabled: checksum_alg byte at buf.length-1, no trailing CRC32
    //
    // The valid values are 0 (OFF) or 1 (CRC32).  We check the position that
    // gives a valid enum value.
    const candidates = [
      { offset: buf.length - 5, hasCRC: true },
      { offset: buf.length - 1, hasCRC: false },
    ];
    for (const c of candidates) {
      if (c.offset >= 19) { // must be past the event header
        const alg = buf.readUInt8(c.offset);
        if (alg === 0 || alg === 1) {
          this.crc32Enabled = c.hasCRC && alg === 1;
          logger.info(`Binlog checksum: ${this.crc32Enabled ? 'CRC32' : 'OFF'} (alg=${alg})`);
          return;
        }
      }
    }
    // Default: assume no CRC32
    this.crc32Enabled = false;
  }

  private processRotate(buf: Buffer): void {
    const off = { value: 19 }; // skip header
    const nextPos = leReadUint48(buf, off.value); off.value += 8;
    const filename = readNullTermStr(buf, off);
    this.currentFilename = filename;
    logger.debug(`Rotate to ${filename}:${nextPos}`);
  }

  private processTableMap(buf: Buffer): void {
    const off = { value: 19 };
    const tableId = leReadUint48(buf, off.value); off.value += 6;
    const flags = leReadUint16(buf, off.value); off.value += 2;

    // MySQL TABLE_MAP_EVENT uses 1-byte length-prefixed strings for
    // database and table names, each followed by a null terminator.
    const dbLen = buf.readUInt8(off.value); off.value += 1;
    const database = buf.toString('utf8', off.value, off.value + dbLen);
    off.value += dbLen + 1; // +1 for null terminator

    const tblLen = buf.readUInt8(off.value); off.value += 1;
    const table = buf.toString('utf8', off.value, off.value + tblLen);
    off.value += tblLen + 1; // +1 for null terminator

    const colCount = readLenencInt(buf, off);
    const colTypes: number[] = [];
    for (let i = 0; i < colCount; i++) {
      colTypes.push(buf.readUInt8(off.value));
      off.value += 1;
    }

    // Column metadata
    const colMetaLen = readLenencInt(buf, off);
    const colMetadata: Buffer[] = [];
    const metaEnd = off.value + colMetaLen;
    for (let i = 0; i < colCount; i++) {
      const metaSize = columnMetadataSize(colTypes[i]);
      if (metaSize === 0) {
        colMetadata.push(Buffer.alloc(0));
      } else {
        colMetadata.push(buf.subarray(off.value, off.value + metaSize));
        off.value += metaSize;
      }
    }
    off.value = metaEnd;

    // Null bitmap (skip)
    const bitmapSize = (colCount + 7) >> 3;
    off.value += bitmapSize;

    this.tableMap.set(tableId, {
      database,
      table,
      colTypes,
      colMetadata,
      colCount,
      nullBitmapSize: bitmapSize,
    });

    logger.debug(`Table map: ${database}.${table} (id=${tableId}, cols=${colCount})`);
  }

  private processRowsEvent(buf: Buffer, changeType: 'insert' | 'update' | 'delete', version: 1 | 2): void {
    const off = { value: 19 };
    const tableId = leReadUint48(buf, off.value); off.value += 6;
    const flags = leReadUint16(buf, off.value); off.value += 2;

    if (version === 2) {
      const extraLen = leReadUint16(buf, off.value); off.value += 2;
      if (extraLen > 2) {
        off.value += extraLen - 2;
      }
    }

    const colCount = readLenencInt(buf, off);
    const mapEntry = this.tableMap.get(tableId);
    if (!mapEntry) {
      logger.warn(`No table map for tableId=${tableId}, skipping rows event`);
      return;
    }

    // Columns present bitmap
    const bitmapSize = (colCount + 7) >> 3;
    const colPresent1 = buf.subarray(off.value, off.value + bitmapSize); off.value += bitmapSize;

    let colPresent2: Buffer | null = null;
    if (changeType === 'update') {
      colPresent2 = buf.subarray(off.value, off.value + bitmapSize); off.value += bitmapSize;
    }

    // Determine which columns are present
    const colIndexes: number[] = [];
    const colIndexes2: number[] = [];
    for (let i = 0; i < colCount; i++) {
      const byteIdx = i >> 3;
      const bitIdx = i & 7;
      if (byteIdx < colPresent1.length && (colPresent1[byteIdx] & (1 << bitIdx)) !== 0) {
        colIndexes.push(i);
      }
      if (colPresent2 && byteIdx < colPresent2.length && (colPresent2[byteIdx] & (1 << bitIdx)) !== 0) {
        colIndexes2.push(i);
      }
    }

    const rows: { before?: RowData; after?: RowData }[] = [];

    while (off.value < buf.length) {
      if (changeType === 'delete' || changeType === 'update') {
        const row = this.readRow(buf, off, mapEntry, colIndexes);
        if (!row) break;
        const entry: { before?: RowData; after?: RowData } = { before: row };
        rows.push(entry);
      }

      if (changeType === 'insert' || changeType === 'update') {
        const indexes = changeType === 'update' ? colIndexes2 : colIndexes;
        const row = this.readRow(buf, off, mapEntry, indexes);
        if (!row && rows.length === 0) break;
        if (row) {
          if (rows.length > 0 && changeType === 'update') {
            rows[rows.length - 1].after = row;
          } else {
            rows.push({ after: row });
          }
        }
      }
    }

    // Emit row changes
    for (const row of rows) {
      const change: RowChange = {
        type: changeType,
        database: mapEntry.database,
        table: mapEntry.table,
        before: row.before,
        after: row.after,
        timestamp: Date.now(),
      };

      // Fire-and-forget callback
      const result = this.onRowChange(change);
      if (result && typeof result.then === 'function') {
        result.catch((err) => logger.error('Error handling row change:', err));
      }
    }
  }

  private readRow(buf: Buffer, off: { value: number }, map: TableMapEntry, colIndexes: number[]): RowData | null {
    if (off.value >= buf.length) return null;

    const row: RowData = {};

    // Row image null bitmap: 1 bit per column in image, no reserved bits.
    // (The "2 reserved bits" exist only at the MySQL internal API level,
    //  not in the on-wire binlog format for row images.)
    const nullBitmapSize = (colIndexes.length + 7) >> 3;
    const nullBitmap = buf.subarray(off.value, off.value + nullBitmapSize);
    off.value += nullBitmapSize;

    for (let i = 0; i < colIndexes.length; i++) {
      const colIdx = colIndexes[i];
      const bitPos = i;
      const byteIdx = bitPos >> 3;
      const bitIdx = bitPos & 7;
      const isNull = (nullBitmap[byteIdx] & (1 << bitIdx)) !== 0;

      if (isNull) continue;

      if (off.value >= buf.length) {
        break;
      }

      const colType = map.colTypes[colIdx];
      const meta = map.colMetadata[colIdx];
      const val = this.parseColumnValue(buf, off, colType, meta);
      // Use index as key — the caller should map to column names
      row[String(colIdx)] = val;
    }

    return Object.keys(row).length > 0 ? row : null;
  }

  private parseColumnValue(buf: Buffer, off: { value: number }, colType: number, meta: Buffer): unknown {
    switch (colType) {
      case ColType.TINY:
        return buf.readInt8(off.value++);

      case ColType.SHORT:
      case ColType.YEAR: {
        const v = buf.readInt16LE(off.value);
        off.value += 2;
        return v;
      }

      case ColType.INT24: {
        // 24-bit signed int
        let v = leReadUint24(buf, off.value);
        off.value += 3;
        if (v & 0x800000) v |= ~0xffffff;
        return v;
      }

      case ColType.LONG: {
        const v = buf.readInt32LE(off.value);
        off.value += 4;
        return v;
      }

      case ColType.LONGLONG: {
        const v = buf.readBigInt64LE(off.value);
        off.value += 8;
        return v;
      }

      case ColType.FLOAT: {
        const v = buf.readFloatLE(off.value);
        off.value += 4;
        return v;
      }

      case ColType.DOUBLE: {
        const v = buf.readDoubleLE(off.value);
        off.value += 8;
        return v;
      }

      case ColType.TIMESTAMP: {
        const sec = leReadUint32(buf, off.value);
        off.value += 4;
        return new Date(sec * 1000).toISOString().slice(0, 19).replace('T', ' ');
      }

      case ColType.TIMESTAMP2: {
        const sec = leReadUint32(buf, off.value); off.value += 4;
        let frac = 0;
        const fracLen = meta.length > 0 ? meta.readUInt8(0) : 0;
        if (fracLen > 0) {
          const fracBytes = (fracLen + 1) >> 1;
          frac = leReadUint24(buf, off.value); // simplified: only handle up to 3 bytes
          off.value += fracBytes;
        }
        const ms = sec * 1000 + Math.round(frac / 1000);
        return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
      }

      case ColType.DATE: {
        const packed = leReadUint24(buf, off.value);
        off.value += 3;
        const d = packed & 0x1f;
        const m = (packed >> 5) & 0x0f;
        const y = (packed >> 9) & 0x7fff;
        if (y === 0) return '0000-00-00';
        return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
      }

      case ColType.TIME: {
        const packed = leReadUint24(buf, off.value);
        off.value += 3;
        const neg = (packed & 0x800000) !== 0;
        const totalSec = neg ? 0x800000 - packed : packed;
        const h = Math.floor(totalSec / 3600);
        const m = Math.floor((totalSec % 3600) / 60);
        const s = totalSec % 60;
        return `${neg ? '-' : ''}${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
      }

      case ColType.TIME2: {
        // 3 bytes big-endian: sign(1) + unused(1) + hour(10) + min(6) + sec(6) = 24 bits
        const b0 = buf.readUInt8(off.value);
        const b1 = buf.readUInt8(off.value + 1);
        const b2 = buf.readUInt8(off.value + 2);
        const packed = (b0 << 16) | (b1 << 8) | b2;
        off.value += 3;

        const fracPrecision = meta.length > 0 ? meta.readUInt8(0) : 0;
        if (fracPrecision > 0) {
          const fracBytes = (fracPrecision + 1) >> 1;
          off.value += fracBytes;
        }

        const neg = (packed & 0x800000) !== 0;
        const h = (packed >> 12) & 0x3ff;
        const m = (packed >> 6) & 0x3f;
        const s = packed & 0x3f;
        return `${neg ? '-' : ''}${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
      }

      case ColType.DATETIME: {
        // Old DATETIME: 8 bytes little-endian (YYYYMMDDHHMMSS as 64-bit int)
        const packed = leReadInt64(buf, off.value);
        off.value += 8;
        const n = Number(packed);
        const d = n / 1000000;
        const t = n % 1000000;
        const y = Math.floor(d / 10000);
        const mo = Math.floor((d % 10000) / 100);
        const day = Math.floor(d % 100);
        const h = Math.floor(t / 10000);
        const mi = Math.floor((t % 10000) / 100);
        const s = Math.floor(t % 100);
        if (y === 0) return '0000-00-00 00:00:00';
        return `${y}-${String(mo).padStart(2, '0')}-${String(day).padStart(2, '0')} ${String(h).padStart(2, '0')}:${String(mi).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
      }

      case ColType.DATETIME2: {
        // DATETIME2: 5 bytes big-endian (40-bit packed integer)
        const b0 = buf.readUInt8(off.value);
        const b1 = buf.readUInt8(off.value + 1);
        const b2 = buf.readUInt8(off.value + 2);
        const b3 = buf.readUInt8(off.value + 3);
        const b4 = buf.readUInt8(off.value + 4);
        const packed = (b0 * 4294967296) + (b1 * 16777216) + (b2 * 65536) + (b3 * 256) + b4;
        off.value += 5;

        // Fractional seconds (0-3 bytes big-endian, depending on meta precision)
        const fracPrecision = meta.length > 0 ? meta.readUInt8(0) : 0;
        let frac = 0;
        if (fracPrecision > 0) {
          const fracBytes = (fracPrecision + 1) >> 1;
          for (let i = 0; i < fracBytes; i++) {
            frac = (frac << 8) | buf.readUInt8(off.value + i);
          }
          off.value += fracBytes;
        }

        // Bit layout: sign(1) + year_month(17) + day(5) + hour(5) + min(6) + sec(6) = 40
        const ymd = (packed >> 22) & 0x1ffff;
        const d2 = (packed >> 17) & 0x1f;
        const h = (packed >> 12) & 0x1f;
        const mi2 = (packed >> 6) & 0x3f;
        const s2 = packed & 0x3f;

        const y = Math.floor(ymd / 13);
        const mo = (ymd % 13) + 1;

        if (y === 0) return '0000-00-00 00:00:00';
        return `${y}-${String(mo).padStart(2, '0')}-${String(d2).padStart(2, '0')} ${String(h).padStart(2, '0')}:${String(mi2).padStart(2, '0')}:${String(s2).padStart(2, '0')}`;
      }

      case ColType.VARCHAR:
      case ColType.VAR_STRING: {
        const len = meta.length >= 2 ? meta.readUInt16LE(0) : 0;
        const actualLen = len < 256 ? buf.readUInt8(off.value) : leReadUint16(buf, off.value);
        off.value += len < 256 ? 1 : 2;
        const str = buf.toString('utf8', off.value, off.value + actualLen);
        off.value += actualLen;
        return str;
      }

      case ColType.STRING: {
        // Could be CHAR or ENUM
        const realType = meta.length >= 2 ? meta.readUInt8(0) >> 4 : 0;
        const fieldLen = meta.length >= 2 ? ((meta.readUInt8(0) & 0x0f) << 8) | meta.readUInt8(1) : 0;
        if (realType === ColType.ENUM) {
          const enumVal = fieldLen > 1 ? leReadUint16(buf, off.value) : buf.readUInt8(off.value);
          off.value += fieldLen > 1 ? 2 : 1;
          return enumVal;
        }
        const len = fieldLen < 256 ? buf.readUInt8(off.value) : leReadUint16(buf, off.value);
        off.value += fieldLen < 256 ? 1 : 2;
        const str = buf.toString('utf8', off.value, off.value + len);
        off.value += len;
        return str;
      }

      case ColType.BLOB:
      case ColType.TINY_BLOB:
      case ColType.MEDIUM_BLOB:
      case ColType.LONG_BLOB: {
        const lenSize = blobLenSize(colType);
        let len = 0;
        for (let i = 0; i < lenSize; i++) {
          len |= buf.readUInt8(off.value + i) << (8 * i);
        }
        off.value += lenSize;
        const val = buf.subarray(off.value, off.value + len);
        off.value += len;
        return val.toString('utf8');
      }

      case ColType.BIT: {
        const bitLen = meta.length >= 2 ? (meta.readUInt8(0) << 8) | meta.readUInt8(1) : 0;
        const byteLen = Math.floor((bitLen + 7) / 8);
        let val = 0n;
        for (let i = 0; i < byteLen; i++) {
          val = (val << 8n) | BigInt(buf.readUInt8(off.value + i));
        }
        off.value += byteLen;
        return Number(val);
      }

      case ColType.NEWDECIMAL: {
        // Skip decimal for now — complex binary format
        const precision = meta.length >= 2 ? meta.readUInt8(0) : 10;
        const scale = meta.length >= 2 ? meta.readUInt8(1) : 0;
        const byteSize = decimalBinarySize(precision, scale);
        const str = buf.toString('utf8', off.value, off.value + byteSize);
        off.value += byteSize;
        // Fallback to reading as string
        return str;
      }

      case ColType.JSON: {
        // Skip JSON for now
        return null;
      }

      case ColType.GEOMETRY: {
        return null;
      }

      default:
        // Unknown type — skip
        return null;
    }

    function decimalBinarySize(prec: number, scale: number): number {
      const intg = prec - scale;
      const intg0 = Math.floor(intg / 9);
      const frac0 = Math.floor(scale / 9);
      return intg0 * 4 + frac0 * 4 + (intg % 9 > 0 ? 4 : 0) + (scale % 9 > 0 ? 4 : 0);
    }
  }

  // -----------------------------------------------------------------------
  // MySQL Handshake with AuthSwitchRequest support
  // -----------------------------------------------------------------------
  private async performHandshake(): Promise<void> {
    if (!this.socket) throw new Error('Socket not connected');

    // Wait for greeting (raw TCP data includes 4-byte MySQL packet header)
    const rawGreeting = await this.readPacket();
    const off = { value: 4 }; // skip packet header (3 len + 1 seq)

    // Protocol version
    const protocolVer = rawGreeting.readUInt8(off.value++);
    if (protocolVer !== 10) {
      throw new Error(`Unsupported protocol version: ${protocolVer}`);
    }

    // Server version
    readNullTermStr(rawGreeting, off);
    const connId = leReadUint32(rawGreeting, off.value); off.value += 4;
    const authData1 = rawGreeting.subarray(off.value, off.value + 8); off.value += 8;
    off.value += 1; // filler
    off.value += 2; // caps low
    off.value += 1; // charset
    off.value += 2; // status
    off.value += 2; // caps high
    const authLen = rawGreeting.readUInt8(off.value++);
    off.value += 10; // reserved
    const part2Len = Math.max(13, authLen - 8);
    const authData2 = rawGreeting.subarray(off.value, off.value + part2Len - 1); off.value += part2Len - 1;
    const authPluginData = Buffer.concat([authData1, authData2]);

    // Build client flags (matching mysql2 defaults for MySQL 5.7)
    const clientFlags =
      (1 << 9) |   // PROTOCOL_41 = 512
      (1 << 15) |  // SECURE_CONNECTION = 32768
      (1 << 19) |  // PLUGIN_AUTH = 524288
      (1 << 0) |   // LONG_PASSWORD = 1
      (1 << 13) |  // TRANSACTIONS = 8192
      (1 << 3) |   // LONG_FLAG = 4
      (1 << 17);   // MULTI_RESULTS = 131072

    const pwHash = sha256Hash(authPluginData, this.config.password);

    // Build handshake response payload
    const payload = Buffer.alloc(200);
    let p = 0;
    payload.writeUInt32LE(clientFlags, p); p += 4;
    payload.writeUInt32LE(16777215, p); p += 4;
    payload[p] = 45; p += 1; // utf8mb4_general_ci
    p += 23; // zeros
    // Username
    payload.write(this.config.user, p, 'ascii'); p += this.config.user.length;
    payload[p] = 0; p += 1;
    // Auth response
    payload[p] = pwHash.length; p += 1;
    pwHash.copy(payload, p); p += pwHash.length;
    // No database
    payload[p] = 0; p += 1;
    // Auth plugin name
    payload.write('mysql_native_password', p, 'ascii'); p += 21;
    payload[p] = 0; p += 1;

    await this.writePacket(payload.subarray(0, p), 1);

    // Read response — may be OK (0x00), ERR (0xff), or AuthSwitch (0xfe)
    const authResp = await this.readPacket();
    const aStatus = authResp[4]; // payload starts at offset 4

    if (aStatus === 0xff) {
      const errCode = leReadUint16(authResp, 5);
      const errMsg = authResp.toString('utf8', 7);
      throw new Error(`Auth failed: ${errCode}: ${errMsg}`);
    }

    if (aStatus === 0xfe) {
      // AuthSwitchRequest — server wants us to re-auth with new challenge
      const asOff = { value: 5 }; // skip header + 0xfe
      const pluginName = readNullTermStr(authResp, asOff);
      const newAuthData = authResp.subarray(asOff.value, asOff.value + 20);
      logger.debug(`Auth switch to ${pluginName}`);

      // Send auth response: just the raw hash bytes
      const newHash = sha256Hash(newAuthData, this.config.password);
      await this.writePacket(newHash, 2);

      // Read final OK/ERR
      const finalResp = await this.readPacket();
      const fStatus = finalResp[4];
      if (fStatus === 0xff) {
        const errCode = leReadUint16(finalResp, 5);
        const errMsg = finalResp.toString('utf8', 7);
        throw new Error(`Auth switch failed: ${errCode}: ${errMsg}`);
      }
      // 0x00 = OK, proceed
    }

    // Auth OK — proceed with slave registration
  }

  // -----------------------------------------------------------------------
  // COM_REGISTER_SLAVE
  // -----------------------------------------------------------------------
  private async registerSlave(): Promise<void> {
    if (!this.socket) throw new Error('Socket not connected');

    const buf = Buffer.alloc(200);
    let off = 0;
    buf[off] = COM_REGISTER_SLAVE; off += 1;
    leWriteUint32(buf, off, this.serverId); off += 4;
    // slave hostname (length + string)
    buf[off] = 0; off += 1;
    // slave user (length + string) — empty
    buf[off] = 0; off += 1;
    // slave password (length + string) — empty
    buf[off] = 0; off += 1;
    // slave port (2)
    buf.writeUInt16LE(0, off); off += 2;
    // replication rank (4)
    leWriteUint32(buf, off, 0); off += 4;
    // master id (4)
    leWriteUint32(buf, off, 0); off += 4;

    await this.writePacket(buf.subarray(0, off), 0);
    const resp = await this.readPacket();
    if (resp[4] === 0xff) {
      const errCode = leReadUint16(resp, 5);
      const errMsg = resp.toString('utf8', 7);
      throw new Error(`Register slave failed: ${errCode}: ${errMsg}`);
    }
  }

  // -----------------------------------------------------------------------
  // COM_QUERY — execute SQL via raw connection
  // -----------------------------------------------------------------------
  private async sendQuery(sql: string): Promise<void> {
    if (!this.socket) throw new Error('Socket not connected');

    const queryBuf = Buffer.alloc(1 + Buffer.byteLength(sql));
    queryBuf[0] = COM_QUERY;
    queryBuf.write(sql, 1, 'utf8');

    await this.writePacket(queryBuf, 0);
    const resp = await this.readPacket();
    if (resp[4] === 0xff) {
      const errCode = leReadUint16(resp, 5);
      const errMsg = resp.toString('utf8', 7);
      throw new Error(`Query failed: ${errCode}: ${errMsg}`);
    }
    // OK packet — proceed
  }

  // -----------------------------------------------------------------------
  // COM_BINLOG_DUMP
  // -----------------------------------------------------------------------
  private async dumpBinlog(pos: BinlogPosition): Promise<void> {
    if (!this.socket) throw new Error('Socket not connected');

    const filename = pos.filename;
    const filenameBytes = Buffer.byteLength(filename);

    const buf = Buffer.alloc(11 + filenameBytes + 1);
    let off = 0;
    buf[off] = COM_BINLOG_DUMP; off += 1;
    leWriteUint32(buf, off, pos.position); off += 4;
    buf.writeUInt16LE(0, off); off += 2; // flags
    leWriteUint32(buf, off, this.serverId); off += 4;
    buf.write(filename, off, 'ascii'); off += filenameBytes;
    buf[off] = 0; off += 1;

    await this.writePacket(buf.subarray(0, off), 0);
    // Read OK response (usually empty OK packet)
    const resp = await this.readPacket();
    if (resp[4] === 0xff) {
      const errCode = leReadUint16(resp, 5);
      const errMsg = resp.toString('utf8', 7);
      throw new Error(`Binlog dump failed: ${errCode}: ${errMsg}`);
    }

    // Binlog dump is established — start processing events.
    // This flag allows processPackets to forward payloads to handleEventPayload.
    this.binlogStreamActive = true;
    // Process any binlog events already buffered (may have arrived in the
    // same TCP segment as the OK response).
    this.processPackets();
  }

  // -----------------------------------------------------------------------
  // Raw packet read/write
  // -----------------------------------------------------------------------
  /**
   * Read exactly one MySQL packet (4-byte header + payload) from this.buffer.
   * Falls back to waiting for onData to push more data.
   */
  private readPacket(): Promise<Buffer> {
    return new Promise((resolve) => {
      const tryExtract = (): boolean => {
        if (this.buffer.length < 4) return false;
        const packetLen = leReadUint24(this.buffer, 0);
        const totalLen = 4 + packetLen;
        if (this.buffer.length < totalLen) return false;
        const packet = this.buffer.subarray(0, totalLen);
        this.buffer = this.buffer.subarray(totalLen);
        resolve(packet);
        return true;
      };
      if (tryExtract()) return;
      // Wait for more data — onData will notify us.
      // Keep the callback alive until a full packet is extracted
      // (data may arrive in partial chunks).
      this._readPacketResolve = () => {
        if (tryExtract()) {
          this._readPacketResolve = null;
        }
      };
    });
  }

  private writePacket(data: Buffer, seq: number): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socket) {
        reject(new Error('Socket not connected'));
        return;
      }

      const header = Buffer.alloc(4);
      writeUint24(header, 0, data.length);
      header[3] = seq;

      this.socket.write(Buffer.concat([header, data]), (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function columnMetadataSize(colType: number): number {
  switch (colType) {
    case ColType.VARCHAR:
    case ColType.STRING:
    case ColType.ENUM:
    case ColType.SET:
    case ColType.BIT:
      return 2;
    case ColType.BLOB:
    case ColType.TINY_BLOB:
    case ColType.MEDIUM_BLOB:
    case ColType.LONG_BLOB:
    case ColType.DOUBLE:
    case ColType.FLOAT:
    case ColType.TIMESTAMP2:
    case ColType.DATETIME2:
    case ColType.TIME2:
      return 1;
    case ColType.NEWDECIMAL:
      return 2;
    default:
      return 0;
  }
}

function blobLenSize(colType: number): number {
  switch (colType) {
    case ColType.TINY_BLOB: return 1;
    case ColType.BLOB: return 2;
    case ColType.MEDIUM_BLOB: return 3;
    case ColType.LONG_BLOB: return 4;
    default: return 2;
  }
}
