import { EventEmitter } from 'node:events';
/** A generic worker pool for concurrent task execution */
export declare class WorkerPool<T> extends EventEmitter {
    private workers;
    private queue;
    private activeCount;
    private processor;
    private drainResolve;
    constructor(concurrency: number, processor: (item: T) => Promise<void>);
    /** Add an item to the work queue */
    push(item: T): void;
    /** Add multiple items at once */
    pushBatch(items: T[]): void;
    /** Get current queue depth */
    get queued(): number;
    /** Get number of actively processing workers */
    get active(): number;
    /** Total pending (queued + active) */
    get pending(): number;
    /** Wait until the queue is empty and all workers are idle */
    drain(): Promise<void>;
    private schedule;
    private processNext;
}
//# sourceMappingURL=worker-pool.d.ts.map