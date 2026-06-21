/**
 * In-memory event queue with retry tracking.
 *
 * Semantics:
 * - Events are appended via `enqueue()`.
 * - `dequeue(n)` removes up to n events from the front for sending.
 * - On failure, `requeue(events)` prepends events back to the front,
 *   preserving the original event_id and timestamp_client (idempotency).
 * - When the queue exceeds `maxSize`, the oldest events are dropped
 *   (loss is tolerated per CONTRACT.md §2.3; reported via onError).
 */

import type { DatacatEvent } from "../types/index.js";

export interface QueuedEvent {
  event: DatacatEvent;
  retryCount: number;
}

export class EventQueue {
  private queue: QueuedEvent[] = [];

  constructor(
    private readonly maxSize: number,
    private readonly onDrop?: (dropped: DatacatEvent[]) => void
  ) {}

  get size(): number {
    return this.queue.length;
  }

  enqueue(event: DatacatEvent): void {
    if (this.queue.length >= this.maxSize) {
      // Drop oldest events to make room
      const excess = this.queue.splice(0, this.queue.length - this.maxSize + 1);
      this.onDrop?.(excess.map((e) => e.event));
    }
    this.queue.push({ event, retryCount: 0 });
  }

  /**
   * Remove and return up to `n` events from the front of the queue.
   */
  dequeue(n: number): QueuedEvent[] {
    return this.queue.splice(0, Math.min(n, this.queue.length));
  }

  /**
   * Prepend events back to the front of the queue (on send failure).
   * The event_id and timestamp_client are preserved exactly — never regenerated.
   */
  requeue(items: QueuedEvent[]): void {
    this.queue.unshift(...items);
  }

  /** Peek without removing (for beacon flush). */
  drain(): QueuedEvent[] {
    const all = this.queue.slice();
    this.queue = [];
    return all;
  }

  isEmpty(): boolean {
    return this.queue.length === 0;
  }
}
