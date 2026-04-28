import { EventEmitter } from 'node:events';
import type { RowChange, BinlogPosition, MySQLConnectionConfig } from '../types.js';
import { BinlogClient } from '../binlog-client.js';
import { logger } from '../logger.js';

/**
 * Incremental (binlog) collector.
 * Wraps BinlogClient and emits RowChange events.
 */
export class IncrementalCollector extends EventEmitter {
  private client: BinlogClient | null = null;
  private sourceConfig: MySQLConnectionConfig;
  private serverId: number;

  constructor(sourceConfig: MySQLConnectionConfig, serverId: number) {
    super();
    this.sourceConfig = sourceConfig;
    this.serverId = serverId;
  }

  /**
   * Start collecting binlog events from a given position.
   */
  async start(from: BinlogPosition): Promise<void> {
    this.client = new BinlogClient(
      this.sourceConfig,
      this.serverId,
      (change: RowChange) => this.handleRowChange(change),
      (pos: BinlogPosition) => this.handlePosition(pos),
    );

    await this.client.start(from);
    logger.info('Incremental collector started');
  }

  stop(): void {
    if (this.client) {
      this.client.stop();
      this.client = null;
    }
  }

  getCurrentPosition(): BinlogPosition | null {
    return this.client?.getCurrentPosition() ?? null;
  }

  get isRunning(): boolean {
    return this.client?.isRunning ?? false;
  }

  private handleRowChange(change: RowChange): void {
    this.emit('change', change);
  }

  private handlePosition(pos: BinlogPosition): void {
    this.emit('position', pos);
  }
}
