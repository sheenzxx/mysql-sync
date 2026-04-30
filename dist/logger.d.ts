import { EventEmitter } from 'node:events';
export declare enum LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3
}
/** EventEmitter for external log subscribers (e.g. web UI via SSE) */
export declare const logEmitter: EventEmitter<[never]>;
export declare function setLogLevel(level: LogLevel): void;
export declare const logger: {
    debug: (...args: unknown[]) => void;
    info: (...args: unknown[]) => void;
    warn: (...args: unknown[]) => void;
    error: (...args: unknown[]) => void;
};
//# sourceMappingURL=logger.d.ts.map