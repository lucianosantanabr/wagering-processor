import 'reflect-metadata';
import { MikroORM } from '@mikro-orm/postgresql';
import { PollingWorker } from '@application/polling-worker';
import { OutboxPublisherWorker } from '@application/publish-outbox';
import { DEFAULT_REFERENCE_POLICY } from '@application/reference-policy';
import {
  buildOutboxPublisher,
  buildPendingReferenceWorker,
  buildServices,
  buildWagerConsumer,
} from '@infra/composition';
import { QueueDepthSampler } from '@infra/messaging/queue-depth-sampler';
import {
  QueueResolver,
  SqsEventPublisher,
  createSqsClient,
  pingQueue,
  sqsConfigFromEnv,
  wagerQueues,
} from '@infra/messaging/sqs';
import { PinoLogger } from '@infra/observability/pino-logger';
import { PrometheusMetrics } from '@infra/observability/prometheus-metrics';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { migrateWithLock } from '@infra/persistence/migrate';
import { createApp } from '@interface/http/create-app';

async function main(): Promise<void> {
  // Observabilidade primeiro: tudo abaixo já loga em JSON e mede.
  const logger = new PinoLogger();
  const metrics = new PrometheusMetrics(logger);
  metrics.collectProcessMetrics();

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  const orm = await MikroORM.init(
    buildOrmConfig(databaseUrl, {
      // sem cores ANSI, no logger estruturado: nenhuma linha nao-JSON no stdout
      logWriter: (message) => logger.info('orm', { message: message.replace(/\u001b\[[0-9;]*m/g, '') }),
    }),
  );
  // O schema é governado por migrations, aplicadas na subida. Com N instâncias iniciando juntas é preciso um
  // lock explícito (advisory lock): o migrator, sozinho, não serializa instâncias concorrentes.
  await migrateWithLock(orm);

  // Espera por referências fora de ordem (ver reference-policy.ts e ARCHITECTURE.md, seção 7).
  const services = buildServices(orm, {
    logger,
    metrics,
    referencePolicy: {
      baseMs: Number(process.env.PENDING_REFERENCE_BASE_MS ?? DEFAULT_REFERENCE_POLICY.baseMs),
      capMs: Number(process.env.PENDING_REFERENCE_CAP_MS ?? DEFAULT_REFERENCE_POLICY.capMs),
      maxAttempts: Number(process.env.PENDING_REFERENCE_MAX_ATTEMPTS ?? DEFAULT_REFERENCE_POLICY.maxAttempts),
    },
  });

  // SQS pode estar indisponível na subida: as filas são resolvidas sob demanda, com retry.
  const sqs = createSqsClient(sqsConfigFromEnv());
  const maxReceiveCount = Number(process.env.SQS_MAX_RECEIVE_COUNT ?? 5);
  const inbound = wagerQueues(
    sqs,
    {
      main: process.env.SQS_QUEUE_NAME ?? 'wager-transactions.fifo',
      dlq: process.env.SQS_DLQ_NAME ?? 'wager-transactions-dlq.fifo',
    },
    {
      maxReceiveCount,
      visibilityTimeoutSeconds: Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS ?? 30),
    },
  );
  const eventsQueue = new QueueResolver(sqs, process.env.SQS_EVENTS_QUEUE_NAME ?? 'wager-events.fifo');

  const consumer = buildWagerConsumer(services, sqs, inbound, { maxReceiveCount });
  const outboxWorker = new OutboxPublisherWorker(
    buildOutboxPublisher(services, new SqsEventPublisher(sqs, eventsQueue)),
    logger,
    { idleIntervalMs: Number(process.env.OUTBOX_IDLE_INTERVAL_MS ?? 500), batchSize: 50 },
  );
  const pendingReferenceWorker = buildPendingReferenceWorker(services);
  const depthSampler = new QueueDepthSampler(sqs, { main: inbound.main, dlq: inbound.dlq, events: eventsQueue }, metrics);
  const samplerWorker = new PollingWorker('queue depth sampler', async () => (await depthSampler.sample(), false), 15_000, logger);

  consumer.start();
  pendingReferenceWorker.start();
  // Permite separar replicas \"so API/consumer\" das que publicam a outbox. Desligado, os eventos seguem gravados
  // (atomicos com a mudanca financeira) e ficam pendentes ate alguma instancia com o publisher ligado.
  if (process.env.OUTBOX_PUBLISHER_ENABLED !== 'false') outboxWorker.start();
  samplerWorker.start();

  const app = await createApp({
    orm,
    services,
    metricsExposer: metrics,
    readinessChecks: [
      { name: 'sqs', check: async () => pingQueue(sqs, await inbound.main.get()) },
      { name: 'sqs-events', check: async () => pingQueue(sqs, await eventsQueue.get()) },
    ],
  });

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port, '0.0.0.0');
  logger.info('http listening', { port });

  let stopping = false;
  const shutdown = async (signal: string) => {
    if (stopping) return;
    stopping = true;
    logger.info('shutting down', { signal });
    // 1) para de consumir: conclui o que está em andamento e devolve o resto à fila
    await consumer.stop();
    // 2) para de aceitar HTTP e aguarda as requisições em andamento
    await app.close();
    // 3) conclui a rodada do worker de PENDING_REFERENCE (cada item é uma transação: nada fica pela metade)
    await pendingReferenceWorker.stop();
    // 4) esvazia o que os passos anteriores acabaram de enfileirar na outbox e para o publisher
    await outboxWorker.stop();
    await samplerWorker.stop();
    sqs.destroy();
    await orm.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((e) => {
  console.error(JSON.stringify({ level: 'fatal', msg: 'startup failed', error: String(e) }));
  process.exit(1);
});
