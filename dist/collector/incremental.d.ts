import { EventEmitter } from 'node:events';
import type { BinlogPosition, MySQLConnectionConfig } from '../types.js';
/**
 * Incremental (binlog) collector.
 * Wraps BinlogClient and emits RowChange events.
 */
export declare class IncrementalCollector extends EventEmitter {
    private client;
    private sourceConfig;
    private serverId;
    constructor(sourceConfig: MySQLConnectionConfig, serverId: number);
    /**
     * Start collecting binlog events from a given position.
     */
    start(from: BinlogPosition): Promise<void>;
    stop(): void;
    getCurrentPosition(): BinlogPosition | null;
    get isRunning(): boolean;
    private handleRowChange;
    private handlePosition;
}
//# sourceMappingURL=incremental.d.ts.map