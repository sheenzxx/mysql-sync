import { EventEmitter } from 'node:events';
import { BinlogClient } from '../binlog-client.js';
import { logger } from '../logger.js';
/**
 * Incremental (binlog) collector.
 * Wraps BinlogClient and emits RowChange events.
 */
export class IncrementalCollector extends EventEmitter {
    client = null;
    sourceConfig;
    serverId;
    constructor(sourceConfig, serverId) {
        super();
        this.sourceConfig = sourceConfig;
        this.serverId = serverId;
    }
    /**
     * Start collecting binlog events from a given position.
     */
    async start(from) {
        this.client = new BinlogClient(this.sourceConfig, this.serverId, (change) => this.handleRowChange(change), (pos) => this.handlePosition(pos));
        await this.client.start(from);
        logger.info('Incremental collector started');
    }
    stop() {
        if (this.client) {
            this.client.stop();
            this.client = null;
        }
    }
    getCurrentPosition() {
        return this.client?.getCurrentPosition() ?? null;
    }
    get isRunning() {
        return this.client?.isRunning ?? false;
    }
    handleRowChange(change) {
        this.emit('change', change);
    }
    handlePosition(pos) {
        this.emit('position', pos);
    }
}
//# sourceMappingURL=incremental.js.map