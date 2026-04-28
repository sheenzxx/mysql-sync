import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname } from 'node:path';
import { logger } from './logger.js';
/**
 * Manages persistence of binlog positions.
 * Saves/loads position to/from a JSON file.
 */
export class PositionManager {
    filePath;
    current = null;
    constructor(filePath) {
        this.filePath = filePath;
    }
    /** Load the last persisted position */
    async load() {
        try {
            if (!existsSync(this.filePath)) {
                logger.info(`No position file found at ${this.filePath}`);
                return null;
            }
            const data = await readFile(this.filePath, 'utf8');
            const parsed = JSON.parse(data);
            if (parsed.filename && typeof parsed.position === 'number') {
                this.current = parsed;
                logger.info(`Loaded position: ${parsed.filename}:${parsed.position}`);
                return parsed;
            }
        }
        catch (err) {
            logger.warn('Failed to load position file:', err);
        }
        return null;
    }
    /** Save a position */
    async save(pos) {
        this.current = pos;
        try {
            const dir = dirname(this.filePath);
            if (!existsSync(dir)) {
                await mkdir(dir, { recursive: true });
            }
            await writeFile(this.filePath, JSON.stringify(pos, null, 2), 'utf8');
        }
        catch (err) {
            logger.error('Failed to save position:', err);
        }
    }
    /** Get current position without loading from file */
    getCurrent() {
        return this.current;
    }
    /** Clear the persisted position */
    async clear() {
        this.current = null;
        try {
            if (existsSync(this.filePath)) {
                await writeFile(this.filePath, '', 'utf8');
            }
        }
        catch {
            // ignore
        }
    }
}
//# sourceMappingURL=position-manager.js.map