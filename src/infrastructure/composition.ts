import type { MikroORM } from '@mikro-orm/postgresql';
import { CreateWallet } from '@application/create-wallet';
import { type Logger, type Metrics, noopLogger, noopMetrics } from '@application/observability';
import type { Clock, EventPublisher, IdGenerator, UnitOfWork } from '@application/ports';
import { DEFAULT_PUBLISH_OPTIONS, PublishOutbox, type PublishOutboxOptions } from '@application/publish-outbox';
import { PollingWorker } from '@application/polling-worker';
import {
  DEFAULT_PENDING_REFERENCE_OPTIONS,
  ProcessPendingReferences,
} from '@application/process-pending-references';
import { DEFAULT_REFERENCE_POLICY, type ReferencePolicy } from '@application/reference-policy';
import { GetLedger, GetTransaction, GetWallet, ReconcileWallet } from '@application/queries';
import { SubmitWagerTransaction } from '@application/submit-wager-transaction';
import { WagerProcessor } from '@application/wager-processor';
import { WagerMessageHandler } from '@application/wager-message-handler';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { WagerQueues } from './messaging/sqs';
import { type ConsumerOptions, DEFAULT_CONSUMER_OPTIONS, SqsWagerConsumer } from './messaging/sqs-wager-consumer';
import { InstrumentedUnitOfWork } from './observability/instrumented-unit-of-work';
import { isTransientInfrastructureError } from './persistence/transient-errors';
import { MikroOrmUnitOfWork } from './persistence/unit-of-work';
import { SystemClock, UuidV7Generator } from './system';

export interface Services {
  uow: UnitOfWork;
  clock: Clock;
  ids: IdGenerator;
  logger: Logger;
  metrics: Metrics;
  processor: WagerProcessor;
  createWallet: CreateWallet;
  submitWagerTransaction: SubmitWagerTransaction;
  getWallet: GetWallet;
  getLedger: GetLedger;
  getTransaction: GetTransaction;
  reconcileWallet: ReconcileWallet;
  processPendingReferences: ProcessPendingReferences;
}

/** Worker de fundo de PENDING_REFERENCE (um por instância; os workers coordenam-se pelo banco). */
export function buildPendingReferenceWorker(
  services: Pick<Services, 'processPendingReferences' | 'logger'>,
  options: { idleIntervalMs?: number; batchSize?: number } = {},
): PollingWorker {
  const batchSize = options.batchSize ?? DEFAULT_PENDING_REFERENCE_OPTIONS.batchSize;
  return new PollingWorker(
    'pending reference worker',
    async () => (await services.processPendingReferences.runOnce()).selected >= batchSize,
    options.idleIntervalMs ?? 1_000,
    services.logger,
  );
}

/** Publisher da outbox: precisa de um `EventPublisher` (SQS em produção, fake/real nos testes). */
export function buildOutboxPublisher(
  services: Pick<Services, 'uow' | 'clock' | 'logger' | 'metrics'>,
  publisher: EventPublisher,
  options: Partial<PublishOutboxOptions> = {},
): PublishOutbox {
  return new PublishOutbox(services.uow, publisher, services.clock, services.logger, services.metrics, {
    ...DEFAULT_PUBLISH_OPTIONS,
    ...options,
  });
}

/** Consumer do `wager-transactions.fifo`: reutiliza o MESMO use case da API HTTP. */
export function buildWagerConsumer(
  services: Pick<Services, 'submitWagerTransaction' | 'logger' | 'metrics'>,
  client: SQSClient,
  queues: WagerQueues,
  options: { maxReceiveCount: number } & Partial<ConsumerOptions>,
): SqsWagerConsumer {
  const { maxReceiveCount, ...consumerOptions } = options;
  const handler = new WagerMessageHandler(
    services.submitWagerTransaction,
    isTransientInfrastructureError,
    { maxReceiveCount },
    services.logger,
    services.metrics,
  );
  return new SqsWagerConsumer(client, queues, handler, services.logger, services.metrics, {
    ...DEFAULT_CONSUMER_OPTIONS,
    ...consumerOptions,
  });
}

/** Composition root: liga portas às implementações. Aceita substituições (testes). */
export function buildServices(
  orm: MikroORM,
  overrides: Partial<Pick<Services, 'uow' | 'clock' | 'ids' | 'logger' | 'metrics'>> & {
    referencePolicy?: ReferencePolicy;
  } = {},
): Services {
  const referencePolicy = overrides.referencePolicy ?? DEFAULT_REFERENCE_POLICY;
  const clock = overrides.clock ?? new SystemClock();
  const ids = overrides.ids ?? new UuidV7Generator();
  const logger = overrides.logger ?? noopLogger;
  const metrics = overrides.metrics ?? noopMetrics;
  // Toda transacao SQL e medida (duracao, espera do lock da wallet, conflitos de lock).
  const uow = overrides.uow ?? new InstrumentedUnitOfWork(new MikroOrmUnitOfWork(orm), metrics);
  const processor = new WagerProcessor(clock, ids);
  return {
    uow,
    clock,
    ids,
    logger,
    metrics,
    processor,
    createWallet: new CreateWallet(uow, clock, ids),
    submitWagerTransaction: new SubmitWagerTransaction(uow, processor, clock, ids, referencePolicy, logger, metrics),
    processPendingReferences: new ProcessPendingReferences(
      uow,
      processor,
      clock,
      ids,
      logger,
      metrics,
      referencePolicy,
    ),
    getWallet: new GetWallet(uow),
    getLedger: new GetLedger(uow),
    getTransaction: new GetTransaction(uow),
    reconcileWallet: new ReconcileWallet(uow, logger, metrics),
  };
}
