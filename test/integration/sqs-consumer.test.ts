import { setDefaultTimeout, afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { DeleteMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { Logger } from '@application/observability';
import type { UnitOfWork } from '@application/ports';
import { buildServices, buildWagerConsumer } from '@infra/composition';
import type { SqsWagerConsumer } from '@infra/messaging/sqs-wager-consumer';
import { QueueDepthSampler } from '@infra/messaging/queue-depth-sampler';
import { PinoLogger } from '@infra/observability/pino-logger';
import { PrometheusMetrics } from '@infra/observability/prometheus-metrics';
import { type TestApp, expectLedgerConsistent, newWallet, startApp } from './support/app';
import { type TestSqs, startLocalstack } from './support/localstack';
import { RecordingMetrics, sleep } from './support/outbox-fakes';
import { type QueueEnv, queueEnv, wagerMessage, waitDrained, waitFor } from './support/sqs-env';

let app: TestApp;
let sqs: TestSqs;
const running: SqsWagerConsumer[] = [];

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  [app, sqs] = await Promise.all([startApp(), startLocalstack()]);
}, 300_000);
afterEach(async () => {
  await Promise.all(running.splice(0).map((c) => c.stop()));
});
afterAll(async () => {
  await Promise.all([app?.stop(), sqs?.stop()]);
}, 60_000);

const money = (amount: string) => ({ amount, currency: 'BRL' });

function startConsumer(
  env: QueueEnv,
  opts: { uow?: UnitOfWork; client?: SQSClient; maxReceiveCount?: number; metrics?: RecordingMetrics; logger?: Logger } = {},
) {
  const metrics = opts.metrics ?? new RecordingMetrics();
  const services = buildServices(app.db.orm, {
    ...(opts.uow ? { uow: opts.uow } : {}),
    ...(opts.logger ? { logger: opts.logger } : {}),
    metrics,
  });
  const consumer = buildWagerConsumer(services, opts.client ?? sqs.client, env.queues, {
    maxReceiveCount: opts.maxReceiveCount ?? 3,
    waitTimeSeconds: 1,
    retryBaseMs: 1000,
    retryCapMs: 4000,
  });
  consumer.start();
  running.push(consumer);
  return { consumer, metrics };
}

const txStatus = async (walletId: string) =>
  (await app.db.query<{ status: string; failure_code: string | null }>(
    `SELECT status, failure_code FROM wager_transactions WHERE wallet_id = '${walletId}' AND kind <> 'OPENING' ORDER BY created_at`,
  ));

describe('processamento básico', () => {
  test('processa a mensagem, comita e só então apaga da fila (inbox marcada, eventos na outbox)', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    const msg = wagerMessage(f);
    startConsumer(env);
    await env.send(msg);
    await waitDrained(env);

    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 2]);
    const [inbox] = await app.db.query<{ processed_at: unknown; consumer_name: string }>(
      `SELECT processed_at, consumer_name FROM inbox_messages WHERE message_id = '${msg.messageId}'`,
    );
    expect(inbox?.consumer_name).toBe('wager-transactions-consumer');
    expect(inbox?.processed_at).not.toBeNull();
    expect(await env.dlqDepth()).toBe(0);
  });

  test('rejeição de NEGÓCIO (saldo insuficiente) é terminal: persiste REJECTED, dá ack e não vai à DLQ', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '10.00');
    startConsumer(env);
    await env.send(wagerMessage(f, {}, { money: money('50.00') }));
    await waitDrained(env);

    expect(await txStatus(f.walletId)).toEqual([{ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' }]);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('10.00');
    expect(await env.dlqDepth()).toBe(0);
  });

  test('referência ausente vira PENDING_REFERENCE e a mensagem é confirmada', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    startConsumer(env);
    await env.send(wagerMessage(f, {}, { kind: 'REFUND', referenceExternalTransactionId: 'bet-not-here' }));
    await waitDrained(env);
    expect((await txStatus(f.walletId))[0]?.status).toBe('PENDING_REFERENCE');
    expect(await env.dlqDepth()).toBe(0);
  });
});

describe('redelivery e duplicatas', () => {
  test('a mesma mensagem entregue 5 vezes e a mesma idempotency key sob outro messageId: um único débito', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    const msg = wagerMessage(f);
    const metrics = new RecordingMetrics();
    startConsumer(env, { metrics });

    // 5 entregas do mesmo messageId (dedupIds distintos para o FIFO não deduplicar por conta própria)
    for (let i = 0; i < 5; i++) await env.send(msg);
    // o provedor reenvia o mesmo pedido num messageId novo (mesma idempotency key)
    await env.send({ ...msg, messageId: `msg-${randomUUID()}` });
    await waitDrained(env);

    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 2]); // um único débito
    expect(metrics.counters.get('duplicates_detected_total')).toBeGreaterThanOrEqual(5);
    const [n] = await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM inbox_messages WHERE message_id = '${msg.messageId}'`);
    expect(n?.n).toBe('1'); // inbox persistente: uma linha por (consumer, messageId)
    expect(await env.dlqDepth()).toBe(0);
  });

  test('worker morto DEPOIS do commit e ANTES do ack: a mensagem reaparece e não duplica o efeito', async () => {
    const env = queueEnv(sqs.client, 3, 2);
    const f = await newWallet(app, '100.00');
    let killed = 0;
    // Simula a morte do processo no instante do ack: o DeleteMessage nunca chega ao SQS (na 1ª vez).
    const dyingClient = new Proxy(sqs.client, {
      get(target, prop, receiver) {
        if (prop === 'send') {
          return (cmd: unknown, o?: unknown) => {
            if (cmd instanceof DeleteMessageCommand && killed === 0) {
              killed += 1;
              return Promise.reject(new Error('SIGKILL antes do ack'));
            }
            return (target.send as (c: unknown, o?: unknown) => Promise<unknown>).call(target, cmd, o);
          };
        }
        return Reflect.get(target, prop, receiver);
      },
    }) as SQSClient;

    const msg = wagerMessage(f);
    startConsumer(env, { client: dyingClient });
    await env.send(msg);

    // 1ª entrega: comita (saldo já debitado) mas o ack falha
    await waitFor(async () => (await expectLedgerConsistent(app.db, f.walletId)).balance === '75.00');
    expect(killed).toBe(1);
    expect((await env.depth()).total).toBe(1); // ainda está na fila

    // após o visibility timeout reaparece, vira duplicata inofensiva e agora é confirmada
    await waitDrained(env);
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 2]);
    expect(await txStatus(f.walletId)).toHaveLength(1);
  });
});

describe('erros permanentes -> DLQ', () => {
  test('mensagens venenosas vão para a DLQ com o motivo; uma boa do mesmo grupo continua sendo processada', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    startConsumer(env);

    const good = wagerMessage(f, {}, { money: money('10.00') });
    const conflictBase = wagerMessage(f, {}, { money: money('5.00') });
    const poison: [string, unknown, boolean][] = [
      ['json inválido', '{not json', true],
      ['tipo desconhecido', wagerMessage(f, { type: 'SomethingElse' }), false],
      ['sem messageId', { ...wagerMessage(f), messageId: undefined }, false],
      ['campo ausente', wagerMessage(f, {}, { roundId: undefined }), false],
      ['valor com 3 casas', wagerMessage(f, {}, { money: money('1.001') }), false],
      ['OPENING não é submetível', wagerMessage(f, {}, { kind: 'OPENING' }), false],
      ['wallet inexistente', wagerMessage({ walletId: randomUUID(), playerId: randomUUID() }), false],
    ];
    for (const [, body, raw] of poison) await env.send(body, { raw, groupId: f.walletId });
    await env.send(conflictBase);
    // mesma idempotency key, payload diferente -> conflito (permanente)
    await env.send(
      wagerMessage(f, {}, { idempotencyKey: conflictBase.data.idempotencyKey, externalTransactionId: conflictBase.data.externalTransactionId, money: money('6.00') }),
    );
    await env.send(good);
    await waitDrained(env);

    const dlq = await env.drainDlq();
    expect(dlq).toHaveLength(poison.length + 1);
    expect(dlq.every((m) => m.attrs.failureClass === 'permanent' && !!m.attrs.failureReason)).toBe(true);
    expect(dlq.map((m) => m.attrs.failureReason).join(' | ')).toMatch(/not valid JSON/);

    // a mensagem venenosa não travou o grupo: a boa e a base do conflito foram aplicadas
    const { balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(balance).toBe('85.00'); // 100 - 5 (base do conflito) - 10 (boa)
  });

  test('messageId REUTILIZADO com conteúdo diferente: vai para a DLQ e não gera nenhum efeito financeiro', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    startConsumer(env);

    const original = wagerMessage(f, {}, { money: money('10.00') });
    await env.send(original);
    await waitDrained(env);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('90.00');

    // outro pedido (outra idempotency key, outro valor) sob o MESMO messageId: não é uma releitura
    const reused = wagerMessage(f, { messageId: original.messageId }, { money: money('60.00') });
    await env.send(reused);
    // uma releitura legítima (mesmo corpo) logo depois: continua sendo confirmada como duplicata
    await env.send(original);
    await waitDrained(env);

    const dlq = await env.drainDlq();
    expect(dlq).toHaveLength(1);
    expect(dlq[0]?.attrs.failureClass).toBe('permanent');
    expect(dlq[0]?.attrs.failureReason).toMatch(/messageId/);
    expect(JSON.parse(dlq[0]!.body).data.money.amount).toBe('60.00'); // é a mensagem adulterada que ficou para análise

    // saldo e ledger só refletem o pedido original (um débito de 10.00)
    expect(await expectLedgerConsistent(app.db, f.walletId)).toMatchObject({ balance: '90.00', entries: 2 });
    const [r] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM wager_transactions WHERE wallet_id = '${f.walletId}' AND kind <> 'OPENING'`,
    );
    expect(r?.n).toBe('1');
    const [inbox] = await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM inbox_messages WHERE message_id = '${original.messageId}'`);
    expect(inbox?.n).toBe('1');
  });
});

describe('erros transitórios', () => {
  /** UoW que falha com erro de conexão N vezes antes de funcionar. */
  const flakyUow = (failures: number): UnitOfWork & { calls: () => number } => {
    let calls = 0;
    return {
      calls: () => calls,
      run: (fn) => {
        calls += 1;
        if (calls <= failures) {
          return Promise.reject(Object.assign(new Error('connection terminated unexpectedly'), { code: '57P01' }));
        }
        return buildServices(app.db.orm).uow.run(fn);
      },
    };
  };

  test('falha transitória: retry com backoff e depois processa uma única vez', async () => {
    const env = queueEnv(sqs.client, 5, 30);
    const f = await newWallet(app, '100.00');
    const uow = flakyUow(2);
    const metrics = new RecordingMetrics();
    startConsumer(env, { uow, maxReceiveCount: 5, metrics });

    const started = Date.now();
    await env.send(wagerMessage(f));
    await waitDrained(env, 60_000);

    expect(metrics.counters.get('sqs_retries_total')).toBe(2);
    expect(Date.now() - started).toBeGreaterThanOrEqual(2900); // backoff: 1 s + 2 s
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 2]);
    expect(await env.dlqDepth()).toBe(0);
  });

  test('tentativas esgotadas (limite=3): vai para a DLQ como "exhausted" e nada é aplicado', async () => {
    const env = queueEnv(sqs.client, 3, 30);
    const f = await newWallet(app, '100.00');
    const metrics = new RecordingMetrics();
    startConsumer(env, { uow: flakyUow(Number.POSITIVE_INFINITY), maxReceiveCount: 3, metrics });

    await env.send(wagerMessage(f));
    await waitDrained(env, 60_000);

    const dlq = await env.drainDlq();
    expect(dlq).toHaveLength(1);
    expect(dlq[0]?.attrs).toMatchObject({ failureClass: 'exhausted', receiveCount: '3' });
    expect(metrics.counters.get('sqs_retries_total')).toBe(2);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });
});

describe('shutdown gracioso (SIGTERM)', () => {
  test('conclui a mensagem em andamento e devolve as não iniciadas, sem perder nem duplicar nada', async () => {
    const env = queueEnv(sqs.client, 3, 30);
    const f = await newWallet(app, '100.00');
    let inFlight = false;
    let finished = 0;
    const slow: UnitOfWork = {
      run: async (fn) => {
        inFlight = true;
        await sleep(800); // processamento lento
        const r = await buildServices(app.db.orm).uow.run(fn);
        finished += 1;
        return r;
      },
    };
    // 6 mensagens da mesma wallet (mesmo grupo FIFO): chegam juntas em UM receive e são tratadas em sequência
    const msgs = Array.from({ length: 6 }, () => wagerMessage(f, {}, { money: money('10.00') }));
    for (const m of msgs) await env.send(m);

    const { consumer } = startConsumer(env, { uow: slow });
    await waitFor(async () => inFlight);
    await consumer.stop(); // SIGTERM no meio do processamento
    running.splice(running.indexOf(consumer), 1);

    expect(finished).toBeGreaterThanOrEqual(1);
    const doneByFirst = finished;
    // o que foi concluído está comitado E confirmado; o resto foi devolvido (não ficou "em voo" por 30 s)
    const depth = await env.depth();
    expect(depth.inFlight).toBe(0);
    expect(depth.visible).toBe(6 - doneByFirst);
    expect(await txStatus(f.walletId)).toHaveLength(doneByFirst);

    // outro consumidor assume o restante
    startConsumer(env);
    await waitDrained(env, 60_000);
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['40.00', 7]); // 6 débitos de 10.00, exatamente uma vez cada
    expect(await env.dlqDepth()).toBe(0);
  });
});

describe('vários consumidores competindo', () => {
  test('3 consumidores, 6 wallets, 60 mensagens: tudo processado uma vez; saldo == ledger em todas', async () => {
    const env = queueEnv(sqs.client);
    const wallets = await Promise.all(Array.from({ length: 6 }, () => newWallet(app, '100.00')));
    for (let i = 0; i < 3; i++) startConsumer(env);
    for (let round = 0; round < 10; round++) {
      await Promise.all(wallets.map((w) => env.send(wagerMessage(w, {}, { money: money('5.00') }))));
    }
    await waitDrained(env, 90_000);
    for (const w of wallets) {
      const { balance, entries } = await expectLedgerConsistent(app.db, w.walletId);
      expect([balance, entries]).toEqual(['50.00', 11]);
    }
    expect(await env.dlqDepth()).toBe(0);
  });

  test('disputa pelo saldo via fila: 15 apostas de 10.00 sobre 100.00 -> 10 processadas, 5 rejeitadas, saldo 0', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    for (let i = 0; i < 3; i++) startConsumer(env);
    for (let i = 0; i < 15; i++) await env.send(wagerMessage(f, {}, { money: money('10.00') }));
    await waitDrained(env, 90_000);

    const st = await txStatus(f.walletId);
    expect(st.filter((s) => s.status === 'PROCESSED')).toHaveLength(10);
    expect(st.filter((s) => s.status === 'REJECTED')).toHaveLength(5);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('0.00');
  });
});

describe('observabilidade do consumer', () => {
  const capture = () => {
    const lines: string[] = [];
    const logger = new PinoLogger({ destination: { write: (s: string) => void lines.push(s.trim()) }, service: 'consumer-test' });
    return { lines, logger, json: () => lines.map((l) => JSON.parse(l) as Record<string, any>) };
  };

  test('logs estruturados carregam messageId, correlationId, transactionId, walletId e providerId, sem valores', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '1000.00');
    const { logger, json, lines } = capture();
    startConsumer(env, { logger });
    const msg = wagerMessage(f, {}, { money: money('441.37') });
    await env.send(msg);
    await waitDrained(env);

    const handled = json().find((l) => l.msg === 'wager transaction handled' && l.messageId === msg.messageId);
    expect(handled).toMatchObject({
      level: 'info',
      source: 'sqs',
      messageId: msg.messageId,
      correlationId: msg.messageId, // a mensagem é a origem da correlação
      walletId: f.walletId,
      providerId: 'provider-a',
      status: 'PROCESSED',
      service: 'consumer-test',
    });
    expect(handled!.transactionId).toBeString();
    expect(lines.join('\n')).not.toContain('441.37');
  });

  test('mensagem venenosa é logada como error com o id do transporte (SQS MessageId) e sem o corpo', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    const { logger, json, lines } = capture();
    startConsumer(env, { logger });
    await env.send('{"segredo": "valor-que-nao-deve-vazar", "money": {"amount": "7777.77"}', { raw: true, groupId: f.walletId });
    await waitDrained(env);

    const dl = json().find((l) => l.msg === 'message sent to dead-letter queue');
    expect(dl).toMatchObject({ level: 'error', failureClass: 'permanent' });
    expect(dl!.sqsMessageId).toBeString();
    expect(lines.join('\n')).not.toContain('valor-que-nao-deve-vazar');
    expect(lines.join('\n')).not.toContain('7777.77');
  });

  test('amostrador publica a profundidade das filas (inclui a DLQ) como gauge', async () => {
    const env = queueEnv(sqs.client);
    const f = await newWallet(app, '100.00');
    const metrics = new PrometheusMetrics();
    startConsumer(env);
    await env.send('not json', { raw: true, groupId: f.walletId });
    await waitDrained(env);
    await waitFor(async () => (await env.dlqDepth()) === 1);

    await new QueueDepthSampler(sqs.client, { main: env.queues.main, dlq: env.queues.dlq }, metrics).sample();
    const text = await metrics.render();
    expect(text).toContain('sqs_queue_messages{queue="dlq",state="visible"} 1');
    expect(text).toContain('sqs_queue_messages{queue="main",state="visible"} 0');
  });
});