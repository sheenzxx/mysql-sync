import { EventEmitter } from 'node:events';
/** A generic worker pool for concurrent task execution */
export class WorkerPool extends EventEmitter {
    workers;
    queue = [];
    activeCount = 0;
    processor;
    drainResolve = null;
    constructor(concurrency, processor) {
        super();
        this.workers = concurrency;
        this.processor = processor;
    }
    /** Add an item to the work queue */
    push(item) {
        this.queue.push(item);
        this.processNext();
    }
    /** Add multiple items at once */
    pushBatch(items) {
        for (const item of items) {
            this.queue.push(item);
        }
        this.schedule();
    }
    /** Get current queue depth */
    get queued() {
        return this.queue.length;
    }
    /** Get number of actively processing workers */
    get active() {
        return this.activeCount;
    }
    /** Total pending (queued + active) */
    get pending() {
        return this.queue.length + this.activeCount;
    }
    /** Wait until the queue is empty and all workers are idle */
    async drain() {
        if (this.activeCount === 0 && this.queue.length === 0)
            return;
        return new Promise((resolve) => {
            this.drainResolve = resolve;
        });
    }
    schedule() {
        while (this.activeCount < this.workers && this.queue.length > 0) {
            this.processNext();
        }
    }
    processNext() {
        if (this.activeCount >= this.workers || this.queue.length === 0)
            return;
        const item = this.queue.shift();
        this.activeCount++;
        this.processor(item)
            .catch((err) => {
            this.emit('error', err, item);
        })
            .finally(() => {
            this.activeCount--;
            this.emit('complete', item);
            if (this.activeCount === 0 && this.queue.length === 0) {
                this.emit('idle');
                if (this.drainResolve) {
                    this.drainResolve();
                    this.drainResolve = null;
                }
            }
            this.schedule();
        });
    }
}
//# sourceMappingURL=worker-pool.js.map