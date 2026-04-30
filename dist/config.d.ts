import type { SyncConfig } from './types.js';
/** Load configuration from a JSON file */
export declare function loadConfigFile(filePath: string): SyncConfig;
/** Build configuration from CLI options + defaults */
export declare function buildConfig(cliOptions: Record<string, any>): SyncConfig;
/** Validate and apply defaults */
export declare function normalizeConfig(config: SyncConfig): SyncConfig;
//# sourceMappingURL=config.d.ts.map