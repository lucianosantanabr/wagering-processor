import { type Logger, type Metrics, noopLogger, noopMetrics } from './observability';
import { PollingWorker } from './polling-worker';
import type { Clock, EventPublisher, UnitOfWork } from './ports';

export interface PublishOutboxOptions {
  /** Mensagens reservadas por rodada. */
  batchSize: number;
  /** Publicações simultâneas dentro de uma rodada. */
  concurrency: number;
  /** Acima disso, cada falha é logada como `error` (possível mensagem venenosa / indisponibilidade longa). */
  alertAfterAttempts: number;
}

export const DEFAULT_PUBLISH_OPTIONS: PublishOutboxOptions = { batchSize: 50, concurrency: 10, alertAfterAttempts: 10 };

export interface PublishRound {
  claimed: number;
  published: number;
  failed: number;
}

/**
 * Transactional outbox, lado da publicação. Uma rodada = UMA transação SQL:
 *
 *   1. reserva mensagens vencidas com `FOR UPDATE SKIP LOCKED` (publishers concorrentes não se enxergam);
 *   2. publica no broker (o lock de linha segue válido durante a publicação);
 *   3. marca `published_at` (ou agenda retry com backoff) e comita.
 *
 * Garantia: AO MENOS UMA vez. Se o processo morrer entre 2 e 3, o lock é liberado, a linha continua
 * pendente e outra instância a republica: o consumidor deduplica por `eventId` (= id da mensagem).
 * Nenhum evento sai antes do commit da transação financeira, pois só existe na outbox depois dele.
 */
export class PublishOutbox {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
    private readonly options: PublishOutboxOptions = DEFAULT_PUBLISH_OPTIONS,
  ) {}

  async runOnce(): Promise<PublishRound> {
    return this.uow.run(async (repos) => {
      const due = await repos.outbox.lockDue(this.clock.now(), this.options.batchSize);
      let published = 0;
      let failed = 0;

      for (let i = 0; i < due.length; i += this.options.concurrency) {
        const slice = due.slice(i, i + this.options.concurrency);
        const outcomes = await Promise.allSettled(
          slice.map(async (m) => {
            const started = performance.now();
            await this.publisher.publish(m);
            this.metrics.observe('outbox_publish_duration_seconds', (performance.now() - started) / 1000);
          }),
        );

        for (const [idx, outcome] of outcomes.entries()) {
          const message = slice[idx]!;
          if (outcome.status === 'fulfilled') {
            message.markPublished(this.clock.now());
            published += 1;
          } else {
            message.scheduleRetry(this.clock.now());
            failed += 1;
            const ctx = {
              messageId: message.id,
              eventType: message.eventType,
              attempts: message.attempts,
              nextAttemptAt: message.nextAttemptAt?.toISOString(),
              error: outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason),
            };
            if (message.attempts >= this.options.alertAfterAttempts) this.logger.error('outbox publish keeps failing', ctx);
            else this.logger.warn('outbox publish failed; retry scheduled', ctx);
          }
          await repos.outbox.save(message);
        }
      }

      if (published) this.metrics.increment('outbox_published_total', {}, published);
      if (failed) this.metrics.increment('outbox_publish_failures_total', {}, failed);
      return { claimed: due.length, published, failed };
    });
  }

  /** Atualiza o gauge `outbox_lag_seconds` (idade da mensagem pendente mais antiga; 0 se não há). */
  async reportLag(): Promise<number> {
    const oldest = await this.uow.run((repos) => repos.outbox.oldestPendingOccurredAt());
    const lag = oldest ? Math.max(0, (this.clock.now().getTime() - oldest.getTime()) / 1000) : 0;
    this.metrics.set('outbox_lag_seconds', lag);
    return lag;
  }
}

/** Laço de fundo da outbox; a mecânica de polling/parada graciosa é do `PollingWorker`. */
export class OutboxPublisherWorker {
  private readonly worker: PollingWorker;

  constructor(
    private readonly publisher: Pick<PublishOutbox, 'runOnce' | 'reportLag'>,
    logger: Logger = noopLogger,
    private readonly options: { idleIntervalMs: number; batchSize: number } = { idleIntervalMs: 500, batchSize: 50 },
  ) {
    this.worker = new PollingWorker(
      'outbox publisher',
      async () => {
        const round = await this.publisher.runOnce();
        await this.publisher.reportLag();
        return round.claimed >= this.options.batchSize; // lote cheio => ainda há trabalho
      },
      options.idleIntervalMs,
      logger,
    );
  }

  start(): void {
    this.worker.start();
  }

  /** Espera a rodada em andamento terminar: nada fica reservado nem pela metade. */
  stop(): Promise<void> {
    return this.worker.stop();
  }
}