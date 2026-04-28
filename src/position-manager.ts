import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname } from 'node:path';
import type { BinlogPosition } from './types.js';
import { logger } from './logger.js';

/**
 * Manages persistence of binlog positions.
 * Saves/loads position to/from a JSON file.
 */
export class PositionManager {
  private filePath: string;
  private current: BinlogPosition | null = null;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  /** Load the last persisted position */
  async load(): Promise<BinlogPosition | null> {
    try {
      if (!existsSync(this.filePath)) {
        logger.info(`No position file found at ${this.filePath}`);
        return null;
      }
      const data = await readFile(this.filePath, 'utf8');
      const parsed = JSON.parse(data) as BinlogPosition;
      if (parsed.filename && typeof parsed.position === 'number') {
        this.current = parsed;
        logger.info(`Loaded position: ${parsed.filename}:${parsed.position}`);
        return parsed;
      }
    } catch (err) {
      logger.warn('Failed to load position file:', err);
    }
    return null;
  }

  /** Save a position */
  async save(pos: BinlogPosition): Promise<void> {
    this.current = pos;
    try {
      const dir = dirname(this.filePath);
      if (!existsSync(dir)) {
        await mkdir(dir, { recursive: true });
      }
      await writeFile(this.filePath, JSON.stringify(pos, null, 2), 'utf8');
    } catch (err) {
      logger.error('Failed to save position:', err);
    }
  }

  /** Get current position without loading from file */
  getCurrent(): BinlogPosition | null {
    return this.current;
  }

  /** Clear the persisted position */
  async clear(): Promise<void> {
    this.current = null;
    try {
      if (existsSync(this.filePath)) {
        await writeFile(this.filePath, '', 'utf8');
      }
    } catch {
      // ignore
    }
  }
}
