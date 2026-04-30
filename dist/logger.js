import { EventEmitter } from 'node:events';
export var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["DEBUG"] = 0] = "DEBUG";
    LogLevel[LogLevel["INFO"] = 1] = "INFO";
    LogLevel[LogLevel["WARN"] = 2] = "WARN";
    LogLevel[LogLevel["ERROR"] = 3] = "ERROR";
})(LogLevel || (LogLevel = {}));
const LEVEL_NAMES = {
    [LogLevel.DEBUG]: 'DEBUG',
    [LogLevel.INFO]: 'INFO',
    [LogLevel.WARN]: 'WARN',
    [LogLevel.ERROR]: 'ERROR',
};
let currentLevel = LogLevel.INFO;
const isBun = typeof process.versions.bun !== 'undefined';
/** EventEmitter for external log subscribers (e.g. web UI via SSE) */
export const logEmitter = new EventEmitter();
function timestamp() {
    const d = new Date();
    return d.toISOString().replace('T', ' ').slice(0, 23);
}
export function setLogLevel(level) {
    currentLevel = level;
}
function log(level, args) {
    if (level < currentLevel)
        return;
    const msg = args.map(a => (typeof a === 'object' ? JSON.stringify(a, null, 0) : String(a))).join(' ');
    const prefix = `[${timestamp()}] [${LEVEL_NAMES[level]}]`;
    const output = `${prefix} ${msg}`;
    // Emit for external subscribers (non-blocking)
    logEmitter.emit('log', { level: LEVEL_NAMES[level], message: msg, timestamp: timestamp(), full: output });
    if (isBun) {
        // Bun has enhanced console
        if (level >= LogLevel.ERROR)
            console.error(output);
        else
            console.log(output);
    }
    else {
        if (level >= LogLevel.ERROR)
            process.stderr.write(output + '\n');
        else
            process.stdout.write(output + '\n');
    }
}
export const logger = {
    debug: (...args) => log(LogLevel.DEBUG, args),
    info: (...args) => log(LogLevel.INFO, args),
    warn: (...args) => log(LogLevel.WARN, args),
    error: (...args) => log(LogLevel.ERROR, args),
};
//# sourceMappingURL=logger.js.map