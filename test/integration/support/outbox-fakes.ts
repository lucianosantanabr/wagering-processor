import type { Clock, EventPublisher } from '@application/ports';
import type { Metrics } from '@application/observability';
import type { OutboxMessage } from '@domain/outbox-message';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Relógio controlável (backoff determinístico). */
export class FakeClock implements Clock {
  constructor(private t: Date = new Date()) {}
  now(): Date {
    return new Date(this.t);
  }
  advance(ms: number): void {
    this.t = new Date(this.t.getTime() + ms);
  }
}

/** Broker em memória: SÓ para isolar a lógica do publisher; o banco é sempre o PostgreSQL real. */
export class RecordingPublisher implements EventPublisher {
  readonly published: string[] = [];
  failAll = false;
  failIds = new Set<string>();
  delayMs = 0;

  async publish(m: OutboxMessage): Promise<void> {
    if (this.delayMs) await sleep(this.delayMs);
    if (this.failAll || this.failIds.has(m.id)) throw new Error('broker unavailable');
    this.published.push(m.id);
  }

  countOf(id: string): number {
    return this.published.filter((x) => x === id).length;
  }
}

export class RecordingMetrics implements Metrics {
  readonly counters = new Map<string, number>();
  readonly gauges = new Map<string, number>();
  readonly observations = new Map<string, number[]>();
  increment(name: string, _labels?: Record<string, string>, value = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + value);
  }
  observe(name: string, value: number): void {
    this.observations.set(name, [...(this.observations.get(name) ?? []), value]);
  }
  set(name: string, value: number): void {
    this.gauges.set(name, value);
  }
}
