import type { BinlogPosition } from './types.js';
/**
 * Manages persistence of binlog positions.
 * Saves/loads position to/from a JSON file.
 */
export declare class PositionManager {
    private filePath;
    private current;
    constructor(filePath: string);
    /** Load the last persisted position */
    load(): Promise<BinlogPosition | null>;
    /** Save a position */
    save(pos: BinlogPosition): Promise<void>;
    /** Get current position without loading from file */
    getCurrent(): BinlogPosition | null;
    /** Clear the persisted position */
    clear(): Promise<void>;
}
//# sourceMappingURL=position-manager.d.ts.map