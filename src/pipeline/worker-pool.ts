import { EventEmitter } from 'node:events';

/** A generic worker pool for concurrent task execution */
export class WorkerPool<T> extends EventEmitter {
  private workers: number;
  private queue: T[] = [];
  private activeCount = 0;
  private processor: (item: T) => Promise<void>;
  private drainResolve: (() => void) | null = null;

  constructor(concurrency: number, processor: (item: T) => Promise<void>) {
    super();
    this.workers = concurrency;
    this.processor = processor;
  }

  /** Add an item to the work queue */
  push(item: T): void {
    this.queue.push(item);
    this.processNext();
  }

  /** Add multiple items at once */
  pushBatch(items: T[]): void {
    for (const item of items) {
      this.queue.push(item);
    }
    this.schedule();
  }

  /** Get current queue depth */
  get queued(): number {
    return this.queue.length;
  }

  /** Get number of actively processing workers */
  get active(): number {
    return this.activeCount;
  }

  /** Total pending (queued + active) */
  get pending(): number {
    return this.queue.length + this.activeCount;
  }

  /** Wait until the queue is empty and all workers are idle */
  async drain(): Promise<void> {
    if (this.activeCount === 0 && this.queue.length === 0) return;
    return new Promise((resolve) => {
      this.drainResolve = resolve;
    });
  }

  private schedule(): void {
    while (this.activeCount < this.workers && this.queue.length > 0) {
      this.processNext();
    }
  }

  private processNext(): void {
    if (this.activeCount >= this.workers || this.queue.length === 0) return;

    const item = this.queue.shift()!;
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
