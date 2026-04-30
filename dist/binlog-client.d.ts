import type { MySQLConnectionConfig, BinlogPosition, RowChange } from './types.js';
/** Callback for row changes */
export type RowChangeCallback = (change: RowChange) => void | Promise<void>;
export declare class BinlogClient {
    private socket;
    private config;
    private serverId;
    private buffer;
    private tableMap;
    private rowsEventBuf;
    /** Position tracking */
    private currentFilename;
    private currentPosition;
    private onRowChange;
    private onPosition?;
    private stopped;
    private binlogStreamActive;
    private _readPacketResolve;
    private crc32Enabled;
    constructor(config: MySQLConnectionConfig, serverId: number, onRowChange: RowChangeCallback, onPosition?: (pos: BinlogPosition) => void);
    /** Start binlog streaming from a given position */
    start(from: BinlogPosition): Promise<void>;
    stop(): void;
    get isRunning(): boolean;
    getCurrentPosition(): BinlogPosition;
    private onData;
    private eventTypeName;
    private processPackets;
    private handleEventPayload;
    private processEvent;
    /** Detect CRC32 from FORMAT_DESCRIPTION_EVENT (first event in binlog). */
    private processFormatDesc;
    private processRotate;
    private processTableMap;
    private processRowsEvent;
    private processQueryEvent;
    private readRow;
    private parseColumnValue;
    private performHandshake;
    private registerSlave;
    private sendQuery;
    private dumpBinlog;
    /**
     * Read exactly one MySQL packet (4-byte header + payload) from this.buffer.
     * Falls back to waiting for onData to push more data.
     */
    private readPacket;
    private writePacket;
}
//# sourceMappingURL=binlog-client.d.ts.map