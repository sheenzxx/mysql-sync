import { Console } from 'node:console';
import { format } from 'node:util';
import { EventEmitter } from 'node:events';

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

const LEVEL_NAMES: Record<LogLevel, string> = {
  [LogLevel.DEBUG]: 'DEBUG',
  [LogLevel.INFO]: 'INFO',
  [LogLevel.WARN]: 'WARN',
  [LogLevel.ERROR]: 'ERROR',
};

let currentLevel = LogLevel.INFO;
const isBun = typeof process.versions.bun !== 'undefined';

/** EventEmitter for external log subscribers (e.g. web UI via SSE) */
export const logEmitter = new EventEmitter();

function timestamp(): string {
  const d = new Date();
  return d.toISOString().replace('T', ' ').slice(0, 23);
}

export function setLogLevel(level: LogLevel): void {
  currentLevel = level;
}

function log(level: LogLevel, args: unknown[]): void {
  if (level < currentLevel) return;
  const msg = args.map(a => (typeof a === 'object' ? JSON.stringify(a, null, 0) : String(a))).join(' ');
  const prefix = `[${timestamp()}] [${LEVEL_NAMES[level]}]`;
  const output = `${prefix} ${msg}`;

  // Emit for external subscribers (non-blocking)
  logEmitter.emit('log', { level: LEVEL_NAMES[level], message: msg, timestamp: timestamp(), full: output });

  if (isBun) {
    // Bun has enhanced console
    if (level >= LogLevel.ERROR) console.error(output);
    else console.log(output);
  } else {
    if (level >= LogLevel.ERROR) process.stderr.write(output + '\n');
    else process.stdout.write(output + '\n');
  }
}

export const logger = {
  debug: (...args: unknown[]) => log(LogLevel.DEBUG, args),
  info: (...args: unknown[]) => log(LogLevel.INFO, args),
  warn: (...args: unknown[]) => log(LogLevel.WARN, args),
  error: (...args: unknown[]) => log(LogLevel.ERROR, args),
};
