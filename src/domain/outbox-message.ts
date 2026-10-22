import { exponentialBackoffMs } from './backoff';
import { InvalidOutboxStateError } from './errors';
import type { IntegrationEvent } from './integration-event';

export const OUTBOX_BACKOFF_BASE_MS = 1_000;
export const OUTBOX_BACKOFF_CAP_MS = 5 * 60_000;

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  /** O id da mensagem é o eventId: consumidores deduplicam por ele em caso de publicação duplicada. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON() as unknown as Record<string, unknown>,
      event.occurredAt,
      0,
    );
  }

  static rehydrate(s: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      s.id,
      s.aggregateId,
      s.eventType,
      s.payload,
      s.occurredAt,
      s.attempts,
      s.nextAttemptAt,
      s.publishedAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (this._nextAttemptAt === undefined || this._nextAttemptAt <= now);
  }

  markPublished(at: Date): void {
    if (!this.isPending()) {
      throw new InvalidOutboxStateError(`outbox message ${this.id} already published`);
    }
    this._publishedAt = at;
  }

  /** Incrementa attempts e calcula o próximo nextAttemptAt (backoff exponencial). */
  scheduleRetry(now: Date): void {
    if (!this.isPending()) {
      throw new InvalidOutboxStateError(`outbox message ${this.id} already published`);
    }
    const delay = exponentialBackoffMs(this._attempts, OUTBOX_BACKOFF_BASE_MS, OUTBOX_BACKOFF_CAP_MS);
    this._attempts += 1;
    this._nextAttemptAt = new Date(now.getTime() + delay);
  }
}
