export type MetricType = 'counter' | 'histogram' | 'gauge';

export interface MetricDefinition {
  type: MetricType;
  help: string;
  labels: readonly string[];
  buckets?: readonly number[];
}

const LATENCY = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
const WAIT = [1, 5, 30, 60, 300, 900, 1800, 3600];

/**
 * Catálogo ÚNICO de métricas. Um teste garante que todo nome usado no código esteja aqui e que
 * nenhuma entrada fique órfã. Rótulos têm cardinalidade limitada (nunca ids de wallet/transação).
 */
export const METRIC_CATALOG: Readonly<Record<string, MetricDefinition>> = {
  // ── transações (correção e throughput) ──
  wager_transactions_total: {
    type: 'counter',
    help: 'Transações que chegaram a um desfecho, por tipo e status (PROCESSED, REJECTED, PENDING_REFERENCE).',
    labels: ['kind', 'status'],
  },
  wager_transaction_duration_seconds: {
    type: 'histogram',
    help: 'Latência do caso de uso SubmitWagerTransaction, da entrada ao commit.',
    labels: ['source', 'result'],
    buckets: LATENCY,
  },
  duplicates_detected_total: {
    type: 'counter',
    help: 'Requisições/mensagens duplicadas detectadas (replay idempotente).',
    labels: ['source'],
  },
  idempotency_conflicts_total: {
    type: 'counter',
    help: 'Mesma idempotency key com payload diferente.',
    labels: ['source'],
  },
  inbox_conflicts_total: {
    type: 'counter',
    help: 'Mensagens SQS com um messageId já registrado mas conteúdo DIFERENTE (defeito do emissor; vão para a DLQ).',
    labels: [],
  },
  idempotency_race_retries_total: {
    type: 'counter',
    help: 'Reexecuções após corrida de inserts com a mesma chave (UNIQUE do banco decidiu).',
    labels: [],
  },
  pending_reference_total: {
    type: 'counter',
    help: 'Desfechos do worker de PENDING_REFERENCE (resolved, rejected, expired, retried, skipped, failed).',
    labels: ['outcome'],
  },
  pending_reference_wait_seconds: {
    type: 'histogram',
    help: 'Tempo entre a reversão chegar e o desfecho do worker.',
    labels: [],
    buckets: WAIT,
  },
  reconciliation_runs_total: { type: 'counter', help: 'Reconciliações executadas.', labels: ['result'] },
  reconciliation_divergences_total: {
    type: 'counter',
    help: 'Divergências entre saldo materializado e ledger (deve ser sempre 0).',
    labels: [],
  },

  // ── concorrência / banco ──
  lock_conflicts_total: {
    type: 'counter',
    help: 'Conflitos de lock/concorrência vistos pelo banco (deadlock, lock_timeout, serialization).',
    labels: ['type'],
  },
  wallet_lock_wait_seconds: {
    type: 'histogram',
    help: 'Tempo esperando o lock de linha da wallet (SELECT … FOR UPDATE): sinal direto de wallet quente.',
    labels: [],
    buckets: LATENCY,
  },
  db_transaction_duration_seconds: {
    type: 'histogram',
    help: 'Duração das transações SQL.',
    labels: ['outcome'],
    buckets: LATENCY,
  },

  // ── HTTP ──
  http_request_duration_seconds: {
    type: 'histogram',
    help: 'Latência das requisições HTTP (a contagem é o total de requisições).',
    labels: ['method', 'route', 'status'],
    buckets: LATENCY,
  },
  http_errors_total: { type: 'counter', help: 'Respostas de erro por status.', labels: ['status'] },

  // ── outbox ──
  outbox_published_total: { type: 'counter', help: 'Eventos publicados pela outbox.', labels: [] },
  outbox_publish_failures_total: { type: 'counter', help: 'Falhas de publicação (retry agendado).', labels: [] },
  outbox_publish_duration_seconds: {
    type: 'histogram',
    help: 'Latência de publicação de um evento no broker.',
    labels: [],
    buckets: LATENCY,
  },
  outbox_lag_seconds: {
    type: 'gauge',
    help: 'Idade da mensagem pendente mais antiga da outbox (0 se vazia).',
    labels: [],
  },

  // ── SQS ──
  sqs_messages_total: {
    type: 'counter',
    help: 'Mensagens tratadas pelo consumer, por desfecho.',
    labels: ['result'],
  },
  sqs_retries_total: { type: 'counter', help: 'Mensagens devolvidas à fila para nova tentativa.', labels: [] },
  sqs_dlq_messages_total: {
    type: 'counter',
    help: 'Mensagens enviadas à DLQ, por classe (permanent, exhausted).',
    labels: ['class'],
  },
  sqs_disposition_failures_total: {
    type: 'counter',
    help: 'Falhas ao confirmar/devolver/mover uma mensagem (ela será reprocessada).',
    labels: ['action'],
  },
  wager_message_processing_duration_seconds: {
    type: 'histogram',
    help: 'Latência de processamento de uma mensagem SQS (handler).',
    labels: [],
    buckets: LATENCY,
  },
  sqs_queue_messages: {
    type: 'gauge',
    help: 'Profundidade das filas (visible, in_flight), amostrada periodicamente.',
    labels: ['queue', 'state'],
  },
};
