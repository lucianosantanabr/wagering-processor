import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import { buildOutboxPublisher, buildServices } from '@infra/composition';
import { InstrumentedUnitOfWork } from '@infra/observability/instrumented-unit-of-work';
import { PinoLogger } from '@infra/observability/pino-logger';
import { PrometheusMetrics } from '@infra/observability/prometheus-metrics';
import { MikroOrmUnitOfWork } from '@infra/persistence/unit-of-work';
import { isTransientInfrastructureError } from '@infra/persistence/transient-errors';
import { createApp } from '@interface/http/create-app';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, startApp } from './support/app';
import { FakeClock, RecordingPublisher, sleep } from './support/outbox-fakes';

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

class Sink {
  lines: string[] = [];
  write(chunk: string): void {
    this.lines.push(chunk.trim());
  }
  json(): Record<string, any>[] {
    return this.lines.map((l) => JSON.parse(l));
  }
}

const sink = new Sink();
const logger = new PinoLogger({ destination: sink, service: 'wagering-test' });
const metrics = new PrometheusMetrics(logger);

let t: TestApp;
let http: NestFastifyApplication;

beforeAll(async () => {
  t = await startApp({ logger, metrics });
  http = await createApp({ orm: t.db.orm, services: t, metricsExposer: metrics });
}, 180_000);
afterAll(async () => {
  await http?.close();
  await t?.stop();
}, 60_000);

const money = (amount: string) => ({ amount, currency: 'BRL' });

async function call(method: 'GET' | 'POST', url: string, body?: unknown, headers: Record<string, string> = {}) {
  const res = await http.inject({
    method,
    url,
    headers: { ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    payload: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.statusCode, headers: res.headers, text: res.body, json: () => JSON.parse(res.body) };
}

let n = 0;
const txBody = (w: { walletId: string; playerId: string }, over: Record<string, unknown> = {}) => {
  n += 1;
  return {
    providerId: 'provider-a',
    externalTransactionId: `obs-${n}-${randomUUID().slice(0, 8)}`,
    playerId: w.playerId,
    walletId: w.walletId,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: money('25.00'),
    ...over,
  };
};
const post = (b: Record<string, unknown>, extra: Record<string, string> = {}) =>
  call('POST', '/wagering/transactions', b, { 'idempotency-key': `${b.providerId}:${b.externalTransactionId}`, ...extra });

const scrape = async () => (await call('GET', '/metrics')).text;
/** Valor de uma série; `0` se a série ainda não existe. */
const value = (text: string, series: string): number => {
  const line = text.split('\n').find((l) => l.startsWith(`${series} `));
  return line ? Number(line.split(' ').pop()) : 0;
};

describe('GET /metrics', () => {
  test('é aberto, em formato Prometheus, e reflete o tráfego real (status, duplicatas, conflitos, latência)', async () => {
    const before = await scrape();
    const w = await newWallet(t, '1000.00');

    const ok = txBody(w);
    expect((await post(ok)).status).toBe(201);
    expect((await post(ok)).status).toBe(200); // replay -> duplicata
    expect((await post({ ...ok, money: money('26.00') }, {})).status).toBe(409); // mesma key, payload diferente
    expect((await post(txBody(w, { money: money('5000.00') }))).status).toBe(422); // saldo insuficiente
    expect((await call('GET', `/wallets/${randomUUID()}`)).status).toBe(404);
    expect((await post(txBody(w, { kind: 'REFUND', referenceExternalTransactionId: 'later' }))).status).toBe(202);

    const res = await call('GET', '/metrics');
    expect(res.status).toBe(200);
    expect(String(res.headers['content-type'])).toContain('text/plain');
    const after = res.text;
    const delta = (series: string) => value(after, series) - value(before, series);

    expect(delta('wager_transactions_total{kind="BET",status="PROCESSED"}')).toBe(1);
    expect(delta('wager_transactions_total{kind="BET",status="REJECTED"}')).toBe(1);
    expect(delta('wager_transactions_total{kind="REFUND",status="PENDING_REFERENCE"}')).toBe(1);
    expect(delta('duplicates_detected_total{source="http"}')).toBe(1);
    expect(delta('idempotency_conflicts_total{source="http"}')).toBe(1);
    expect(delta('http_errors_total{status="404"}')).toBe(1);
    expect(delta('http_errors_total{status="409"}')).toBe(1);
    expect(delta('wager_transaction_duration_seconds_count{source="http",result="new"}')).toBe(3);
    expect(delta('wager_transaction_duration_seconds_count{source="http",result="replay"}')).toBe(1);
    expect(delta('http_request_duration_seconds_count{method="POST",route="/wagering/transactions",status="201"}')).toBe(1);
    expect(delta('http_request_duration_seconds_count{method="POST",route="/wagering/transactions",status="422"}')).toBe(1);
    expect(value(after, 'db_transaction_duration_seconds_count{outcome="commit"}')).toBeGreaterThan(0);
  });

  test('o rótulo "route" é o padrão da rota: nunca contém ids nem query string (cardinalidade limitada)', async () => {
    const w = await newWallet(t, '10.00');
    await call('GET', `/wallets/${w.walletId}`);
    await call('GET', `/wallets/${w.walletId}/ledger?limit=1&cursor=abc`);
    await call('GET', '/rota-que-nao-existe');
    const text = await scrape();

    expect(text).toContain('route="/wallets/:walletId"');
    expect(text).toContain('route="/wallets/:walletId/ledger"');
    expect(text).toContain('route="unmatched"');
    expect(text).not.toContain(w.walletId);
    expect(text).not.toContain('cursor=');
    const routes = new Set([...text.matchAll(/route="([^"]*)"/g)].map((m) => m[1]));
    expect([...routes].every((r) => !/[0-9a-f]{8}-[0-9a-f]{4}/.test(r!))).toBe(true);
  });

  test('wallet quente: o tempo de espera do lock aparece no histograma e nada se perde', async () => {
    const before = value(await scrape(), 'wallet_lock_wait_seconds_count');
    const w = await newWallet(t, '1000.00');
    const rs = await Promise.all(Array.from({ length: 30 }, () => post(txBody(w, { money: money('1.00') }))));
    expect(rs.every((r) => r.status === 201)).toBe(true);
    const text = await scrape();
    expect(value(text, 'wallet_lock_wait_seconds_count') - before).toBeGreaterThanOrEqual(30);
    // com 30 requisições disputando a mesma wallet, algumas esperaram mais de 5 ms pelo lock
    const total = value(text, 'wallet_lock_wait_seconds_count');
    const fast = value(text, 'wallet_lock_wait_seconds_bucket{le="0.005"}');
    expect(total - fast).toBeGreaterThan(0);
    expect((await expectLedgerConsistent(t.db, w.walletId)).balance).toBe('970.00');
  });

  test('outbox lag, publicadas e pendências de referência aparecem nas métricas', async () => {
    const clock = new FakeClock();
    const svc = buildServices(t.db.orm, { logger, metrics, clock, referencePolicy: { baseMs: 1000, capMs: 8000, maxAttempts: 4 } });
    const w = await newWallet(svc, '100.00');
    const betExt = `bet-${randomUUID().slice(0, 8)}`;

    // lag da outbox
    const pub = buildOutboxPublisher(svc, new RecordingPublisher());
    clock.advance(20_000);
    const lag = await pub.reportLag();
    expect(lag).toBeGreaterThanOrEqual(19);
    expect(value(await scrape(), 'outbox_lag_seconds')).toBeGreaterThanOrEqual(19);
    const published = value(await scrape(), 'outbox_published_total');
    while ((await pub.runOnce()).claimed > 0); // esvazia tudo (inclusive pendências de testes anteriores)
    expect(value(await scrape(), 'outbox_published_total')).toBeGreaterThan(published);
    await pub.reportLag();
    expect(value(await scrape(), 'outbox_lag_seconds')).toBe(0);

    // referência fora de ordem resolvida pelo worker
    await svc.submitWagerTransaction.execute(
      cmd(w, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: betExt }),
    );
    await svc.submitWagerTransaction.execute(cmd(w, { externalTransactionId: betExt, money: money('25.00') }));
    const resolvedBefore = value(await scrape(), 'pending_reference_total{outcome="resolved"}');
    clock.advance(1000);
    await svc.processPendingReferences.runOnce();
    expect(value(await scrape(), 'pending_reference_total{outcome="resolved"}') - resolvedBefore).toBe(1);
  });
});

describe('logs estruturados', () => {
  test('cada linha é JSON com level/time/msg; o correlationId do cliente atravessa a requisição até o log', async () => {
    sink.lines.length = 0;
    const w = await newWallet(t, '500.00');
    const body = txBody(w, { money: money('337.45') });
    const corr = `corr-${randomUUID()}`;
    const res = await post(body, { 'x-correlation-id': corr });
    expect(res.status).toBe(201);
    expect(res.headers['x-correlation-id']).toBe(corr);
    const created = res.json();

    const logs = sink.json();
    expect(logs.every((l) => typeof l.level === 'string' && typeof l.time === 'string' && typeof l.msg === 'string')).toBe(true);

    const handled = logs.find((l) => l.msg === 'wager transaction handled' && l.transactionId === created.transactionId);
    expect(handled).toMatchObject({
      level: 'info',
      correlationId: corr,
      transactionId: created.transactionId,
      walletId: w.walletId,
      providerId: 'provider-a',
      kind: 'BET',
      status: 'PROCESSED',
      idempotentReplay: false,
      source: 'http',
      service: 'wagering-test',
    });

    const req = logs.find((l) => l.msg === 'http request' && l.correlationId === corr);
    expect(req).toMatchObject({ method: 'POST', route: '/wagering/transactions', status: 201 });
    expect(typeof req!.durationMs).toBe('number');
  });

  test('NENHUMA linha contém valores monetários, o corpo da requisição, nem a idempotency key completa do payload', async () => {
    sink.lines.length = 0;
    const w = await newWallet(t, '812.33');
    const body = txBody(w, { money: money('337.45') });
    await post(body);
    await post({ ...body, money: money('99.99') }); // conflito
    await post(txBody(w, { money: money('99999.99') })); // rejeitada por saldo
    await call('POST', '/wallets/' + w.walletId + '/reconciliation');
    await call('GET', `/wallets/${w.walletId}/ledger?limit=2`);

    // Valores com 3+ digitos inteiros: um timestamp ISO (":41.371Z") nunca contem um valor assim por acaso.
    const all = sink.lines.join('\n');
    expect(sink.lines.length).toBeGreaterThan(4);
    for (const secret of ['337.45', '99.99', '99999.99', '812.33', '"amount"', 'initialBalance', 'gameId', 'fortune-chimp']) {
      expect(all).not.toContain(secret);
    }
    expect(all).not.toContain('limit=2'); // sem query string
  });

  test('erros 5xx são logados com correlationId e SEM vazar a mensagem do cliente nem o payload', async () => {
    sink.lines.length = 0;
    const flaky = buildServices(t.db.orm, {
      logger,
      metrics,
      uow: { run: () => Promise.reject(Object.assign(new Error('violates check constraint; Failing row contains (a, b, BRL, 123.45, 3)'), { code: 'XX000' })) },
    });
    const app2 = await createApp({ orm: t.db.orm, services: flaky });
    const w = { walletId: randomUUID(), playerId: randomUUID() };
    const b = txBody(w, { money: money('123.45') });
    const res = await app2.inject({
      method: 'POST',
      url: '/wagering/transactions',
      headers: { 'content-type': 'application/json', 'idempotency-key': 'k-500', 'x-correlation-id': 'corr-500' },
      payload: JSON.stringify(b),
    });
    expect(res.statusCode).toBe(500);
    expect(JSON.parse(res.body)).toMatchObject({ code: 'INTERNAL_ERROR', message: 'unexpected error', correlationId: 'corr-500' });
    expect(res.body).not.toContain('Failing row');

    const failure = sink.json().find((l) => l.msg === 'request failed');
    expect(failure).toMatchObject({ level: 'error', correlationId: 'corr-500', status: 500, method: 'POST', url: '/wagering/transactions' });
    expect(failure!.error).toContain('Failing row contains [REDACTED]'); // diagnostico preservado, valores nao
    expect(sink.lines.join('\n')).not.toContain('123.45');
    await app2.close();
  });
});

describe('conflitos de lock (deadlock real)', () => {
  test('dois processos travando wallets em ordem oposta: o PostgreSQL aborta um, a métrica registra e o erro é transitório', async () => {
    const localMetrics = new PrometheusMetrics();
    const uow = new InstrumentedUnitOfWork(new MikroOrmUnitOfWork(t.db.orm), localMetrics);
    const a = await newWallet(t, '10.00');
    const b = await newWallet(t, '10.00');
    const lockBoth = (first: string, second: string) =>
      uow.run(async (r) => {
        await r.wallets.findByIdForUpdate(first);
        await sleep(300); // garante que cada um já segura a sua antes de pedir a outra
        await r.wallets.findByIdForUpdate(second);
      });

    const results = await Promise.allSettled([lockBoth(a.walletId, b.walletId), lockBoth(b.walletId, a.walletId)]);
    const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
    expect(failed).toHaveLength(1);
    expect(isTransientInfrastructureError(failed[0]!.reason)).toBe(true); // a API responderia 503, o consumer faria retry

    const text = await localMetrics.render();
    expect(text).toContain('lock_conflicts_total{type="deadlock"} 1');
    expect(text).toContain('db_transaction_duration_seconds_count{outcome="rollback"} 1');
    expect(text).toContain('db_transaction_duration_seconds_count{outcome="commit"} 1');
  });
});
