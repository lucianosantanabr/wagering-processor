import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { Logger } from '@application/observability';
import type { UnitOfWork } from '@application/ports';
import { buildServices } from '@infra/composition';
import { createApp } from '@interface/http/create-app';
import { type TestApp, expectLedgerConsistent, startApp } from './support/app';

let t: TestApp;
let http: NestFastifyApplication;
const logged: { message: string; context?: Record<string, unknown> }[] = [];
const logger: Logger = {
  info() {},
  warn() {},
  error: (message, context) => void logged.push({ message, context }),
};

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  t = await startApp({ logger });
  http = await createApp({ orm: t.db.orm, services: t });
}, 180_000);
afterAll(async () => {
  await http?.close();
  await t?.stop();
}, 60_000);

type Res = { status: number; body: any; headers: Record<string, unknown> };
async function call(method: 'GET' | 'POST', url: string, opts: { body?: unknown; headers?: Record<string, string>; raw?: string } = {}): Promise<Res> {
  const res = await http.inject({
    method,
    url,
    headers: { ...(opts.raw !== undefined || opts.body !== undefined ? { 'content-type': 'application/json' } : {}), ...opts.headers },
    payload: opts.raw ?? (opts.body === undefined ? undefined : JSON.stringify(opts.body)),
  });
  let body: any;
  try {
    body = res.body ? JSON.parse(res.body) : undefined;
  } catch {
    body = res.body;
  }
  return { status: res.statusCode, body, headers: res.headers };
}

const money = (amount: string) => ({ amount, currency: 'BRL' });

async function createWallet(balance = '1000.00') {
  const playerId = randomUUID();
  const r = await call('POST', '/wallets', { body: { playerId, initialBalance: money(balance) } });
  expect(r.status).toBe(201);
  return { walletId: r.body.id as string, playerId };
}

let n = 0;
function txBody(w: { walletId: string; playerId: string }, over: Record<string, unknown> = {}) {
  n += 1;
  return {
    providerId: 'provider-a',
    externalTransactionId: `http-${n}-${randomUUID().slice(0, 8)}`,
    playerId: w.playerId,
    walletId: w.walletId,
    roundId: 'round-987',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: money('25.00'),
    ...over,
  };
}
const post = (body: Record<string, unknown>, key?: string) =>
  call('POST', '/wagering/transactions', {
    body,
    headers: key === undefined ? {} : { 'idempotency-key': key },
  });
const keyOf = (b: Record<string, unknown>) => `${b.providerId}:${b.externalTransactionId}`;

describe('health', () => {
  test('live e ready (sem autenticação)', async () => {
    expect(await call('GET', '/health/live')).toMatchObject({ status: 200, body: { status: 'ok' } });
    expect(await call('GET', '/health/ready')).toMatchObject({ status: 200, body: { status: 'ok', checks: { postgres: 'up' } } });
  });

  test('ready responde 503 quando uma dependência cai', async () => {
    const app2 = await createApp({
      orm: t.db.orm,
      services: t,
      readinessChecks: [{ name: 'sqs', check: async () => Promise.reject(new Error('unreachable')) }],
    });
    const res = await app2.inject({ method: 'GET', url: '/health/ready' });
    expect(res.statusCode).toBe(503);
    expect(JSON.parse(res.body)).toEqual({ status: 'unavailable', checks: { postgres: 'up', sqs: 'down' } });
    await app2.close();
  });
});

describe('POST /wallets', () => {
  test('201 com o contrato do enunciado e OPENING no ledger', async () => {
    const playerId = randomUUID();
    const r = await call('POST', '/wallets', { body: { playerId, initialBalance: money('1000.00') } });
    expect(r.status).toBe(201);
    expect(r.body).toEqual({ id: expect.any(String), playerId, balance: money('1000.00'), version: 1 });
  });

  test('duplicada para o mesmo player+moeda -> 409', async () => {
    const w = await createWallet();
    const r = await call('POST', '/wallets', { body: { playerId: w.playerId, initialBalance: money('1.00') } });
    expect(r).toMatchObject({ status: 409, body: { code: 'WALLET_ALREADY_EXISTS' } });
  });

  test.each([
    ['sem body', undefined],
    ['playerId inválido', { playerId: 'x', initialBalance: money('1.00') }],
    ['saldo negativo', { playerId: randomUUID(), initialBalance: money('-1.00') }],
    ['amount numérico', { playerId: randomUUID(), initialBalance: { amount: 10, currency: 'BRL' } }],
    ['sem initialBalance', { playerId: randomUUID() }],
  ])('%s -> 400', async (_n, body) => {
    const r = await call('POST', '/wallets', body === undefined ? { raw: '' } : { body });
    expect(r.status).toBe(400);
    expect(r.body.code).toMatch(/VALIDATION_ERROR|BAD_REQUEST/);
  });

  test('JSON malformado -> 400', async () => {
    expect((await call('POST', '/wallets', { raw: '{oops' })).status).toBe(400);
  });
});

describe('GET /wallets/:id', () => {
  test('200, 404 e 400 (id não-UUID)', async () => {
    const w = await createWallet('10.00');
    expect(await call('GET', `/wallets/${w.walletId}`)).toMatchObject({
      status: 200,
      body: { id: w.walletId, playerId: w.playerId, balance: money('10.00'), version: 1 },
    });
    expect((await call('GET', `/wallets/${randomUUID()}`)).status).toBe(404);
    expect((await call('GET', '/wallets/not-a-uuid')).status).toBe(400);
  });
});

describe('POST /wagering/transactions', () => {
  test('201 PROCESSED com o contrato do enunciado', async () => {
    const w = await createWallet('1000.00');
    const body = txBody(w);
    const r = await post(body, keyOf(body));
    expect(r.status).toBe(201);
    expect(r.body).toEqual({
      transactionId: expect.any(String),
      status: 'PROCESSED',
      balance: money('975.00'),
      idempotentReplay: false,
    });
  });

  test('replay -> 200 com a mesma resposta e idempotentReplay=true', async () => {
    const w = await createWallet();
    const body = txBody(w);
    const first = await post(body, keyOf(body));
    const replay = await post(body, keyOf(body));
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
  });

  test('o header Idempotency-Key é obrigatório -> 400', async () => {
    const w = await createWallet();
    const r = await post(txBody(w));
    expect(r).toMatchObject({ status: 400, body: { code: 'VALIDATION_ERROR' } });
  });

  test('mesma key com payload diferente -> 409 (conflito, não replay)', async () => {
    const w = await createWallet();
    const body = txBody(w);
    await post(body, keyOf(body));
    const r = await post({ ...body, money: money('26.00') }, keyOf(body));
    expect(r).toMatchObject({ status: 409, body: { code: 'IDEMPOTENCY_CONFLICT' } });
  });

  test('rejeição de negócio -> 422 com failureCode estável e saldo', async () => {
    const w = await createWallet('10.00');
    const body = txBody(w, { money: money('50.00') });
    const r = await post(body, keyOf(body));
    expect(r.status).toBe(422);
    expect(r.body).toMatchObject({
      code: 'TRANSACTION_REJECTED',
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_FUNDS',
      balance: money('10.00'),
      idempotentReplay: false,
    });
    // replay da rejeição: mesma resposta
    expect((await post(body, keyOf(body))).status).toBe(422);
  });

  test('referência ausente -> 202 PENDING_REFERENCE', async () => {
    const w = await createWallet();
    const body = txBody(w, { kind: 'REFUND', referenceExternalTransactionId: 'not-yet' });
    const r = await post(body, keyOf(body));
    expect(r.status).toBe(202);
    expect(r.body).toMatchObject({ status: 'PENDING_REFERENCE', balance: money('1000.00') });
  });

  test('wallet inexistente -> 404', async () => {
    const body = txBody({ walletId: randomUUID(), playerId: randomUUID() });
    expect((await post(body, keyOf(body))).status).toBe(404);
  });

  test.each([
    ['OPENING', { kind: 'OPENING' }],
    ['valor com 3 casas', { money: money('1.001') }],
    ['campo ausente', { roundId: undefined }],
    ['REFUND sem referência', { kind: 'REFUND' }],
  ])('payload inválido (%s) -> 400', async (_n, over) => {
    const w = await createWallet();
    const body = txBody(w, over);
    const r = await post(body, keyOf(body));
    expect(r).toMatchObject({ status: 400, body: { code: 'VALIDATION_ERROR' } });
  });

  test('50 requisições HTTP idênticas em paralelo -> um 201 e 49 replays 200; um único débito', async () => {
    const w = await createWallet('100.00');
    const body = txBody(w);
    const rs = await Promise.all(Array.from({ length: 50 }, () => post(body, keyOf(body))));
    expect(rs.filter((r) => r.status === 201)).toHaveLength(1);
    expect(rs.filter((r) => r.status === 200)).toHaveLength(49);
    const { balance } = await expectLedgerConsistent(t.db, w.walletId);
    expect(balance).toBe('75.00');
  });

  test('falha transitória de infraestrutura -> 503 com Retry-After (seguro reenviar)', async () => {
    const flaky: UnitOfWork = {
      run: () => Promise.reject(Object.assign(new Error('terminating connection due to administrator command'), { code: '57P01' })),
    };
    const app2 = await createApp({ orm: t.db.orm, services: buildServices(t.db.orm, { uow: flaky, logger }) });
    const w = { walletId: randomUUID(), playerId: randomUUID() };
    const body = txBody(w);
    const res = await app2.inject({
      method: 'POST',
      url: '/wagering/transactions',
      headers: { 'content-type': 'application/json', 'idempotency-key': keyOf(body) },
      payload: JSON.stringify(body),
    });
    expect(res.statusCode).toBe(503);
    expect(res.headers['retry-after']).toBe('1');
    expect(JSON.parse(res.body).code).toBe('SERVICE_UNAVAILABLE');
    await app2.close();
  });
});

describe('consultas de transação', () => {
  test('por id interno e por (provider, id externo)', async () => {
    const w = await createWallet();
    const body = txBody(w);
    const created = await post(body, keyOf(body));
    const byId = await call('GET', `/wagering/transactions/${created.body.transactionId}`);
    const byExt = await call('GET', `/providers/provider-a/wagering/transactions/${body.externalTransactionId}`);
    expect(byId.status).toBe(200);
    expect(byId.body).toMatchObject({
      transactionId: created.body.transactionId,
      status: 'PROCESSED',
      kind: 'BET',
      money: money('25.00'),
      balance: money('975.00'),
    });
    expect(byExt.body).toEqual(byId.body);
  });

  test('GET de transação REJECTED é 200 (é um recurso); inexistente é 404; id inválido é 400', async () => {
    const w = await createWallet('1.00');
    const body = txBody(w);
    const rejected = await post(body, keyOf(body));
    const got = await call('GET', `/wagering/transactions/${rejected.body.transactionId}`);
    expect(got).toMatchObject({ status: 200, body: { status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' } });
    expect((await call('GET', `/wagering/transactions/${randomUUID()}`)).status).toBe(404);
    expect((await call('GET', '/wagering/transactions/xyz')).status).toBe(400);
    expect((await call('GET', '/providers/provider-a/wagering/transactions/none')).status).toBe(404);
  });
});

describe('GET /wallets/:id/ledger', () => {
  test('paginação por cursor opaco: sem repetição, sem lacuna, mais recentes primeiro', async () => {
    const w = await createWallet('1000.00');
    for (let i = 0; i < 6; i++) {
      const b = txBody(w, { money: money(`${i + 1}.00`) });
      expect((await post(b, keyOf(b))).status).toBe(201);
    }
    // 7 lançamentos: abertura + 6 débitos
    const seen: any[] = [];
    let cursor: string | null | undefined;
    let pages = 0;
    do {
      const url: string = `/wallets/${w.walletId}/ledger?limit=3${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
      const r = await call('GET', url);
      expect(r.status).toBe(200);
      seen.push(...r.body.items);
      cursor = r.body.nextCursor;
      pages += 1;
    } while (cursor);

    expect(pages).toBe(3); // 3 + 3 + 1
    expect(seen).toHaveLength(7);
    expect(new Set(seen.map((e) => e.id)).size).toBe(7);
    expect(seen[0]).toMatchObject({ direction: 'DEBIT', money: money('6.00') }); // mais recente primeiro
    expect(seen[6]).toMatchObject({ direction: 'CREDIT', money: money('1000.00'), balanceBefore: money('0.00') });
    expect(seen[0]).toHaveProperty('balanceAfter');
  });

  test('parâmetros inválidos -> 400; wallet inexistente -> 404', async () => {
    const w = await createWallet();
    for (const q of ['limit=0', 'limit=201', 'limit=abc', 'cursor=@@@', `cursor=${Buffer.from('{"s":"x"}').toString('base64url')}`]) {
      expect((await call('GET', `/wallets/${w.walletId}/ledger?${q}`)).status).toBe(400);
    }
    expect((await call('GET', `/wallets/${randomUUID()}/ledger`)).status).toBe(404);
  });

  test('o cursor continua válido quando entram lançamentos novos', async () => {
    const w = await createWallet('1000.00');
    const b1 = txBody(w);
    await post(b1, keyOf(b1));
    const first = await call('GET', `/wallets/${w.walletId}/ledger?limit=1`);
    const b2 = txBody(w);
    await post(b2, keyOf(b2)); // chega depois de emitido o cursor
    const next = await call('GET', `/wallets/${w.walletId}/ledger?limit=5&cursor=${first.body.nextCursor}`);
    expect(next.body.items).toHaveLength(1); // só a abertura; o novo lançamento não entra na página antiga
    expect(next.body.items[0].direction).toBe('CREDIT');
  });
});

describe('POST /wallets/:id/reconciliation', () => {
  test('wallet consistente', async () => {
    const w = await createWallet('1000.00');
    const b = txBody(w);
    await post(b, keyOf(b));
    const r = await call('POST', `/wallets/${w.walletId}/reconciliation`);
    expect(r).toMatchObject({
      status: 200,
      body: {
        walletId: w.walletId,
        storedBalance: money('975.00'),
        calculatedBalance: money('975.00'),
        difference: money('0.00'),
        consistent: true,
        checkedEntries: 2,
      },
    });
  });

  test('divergência é sinalizada e logada, mas NÃO corrigida', async () => {
    const w = await createWallet('100.00');
    // Simula corrupção fora do caminho da aplicação (desliga os triggers só nesta transação de teste).
    await t.db.query(
      `ALTER TABLE wallets DISABLE TRIGGER USER;
       UPDATE wallets SET balance = balance + 7.50 WHERE id = '${w.walletId}';
       ALTER TABLE wallets ENABLE TRIGGER USER`,
    );
    logged.length = 0;
    const r = await call('POST', `/wallets/${w.walletId}/reconciliation`);
    expect(r.status).toBe(200);
    expect(r.body).toMatchObject({
      storedBalance: money('107.50'),
      calculatedBalance: money('100.00'),
      difference: money('7.50'),
      consistent: false,
    });
    expect(logged.some((l) => l.message.includes('divergence') && l.context?.walletId === w.walletId)).toBe(true);
    // nada foi "consertado" silenciosamente
    expect((await call('GET', `/wallets/${w.walletId}`)).body.balance).toEqual(money('107.50'));
  });

  test('404 e 400', async () => {
    expect((await call('POST', `/wallets/${randomUUID()}/reconciliation`)).status).toBe(404);
    expect((await call('POST', '/wallets/zzz/reconciliation')).status).toBe(400);
  });
});

describe('transversais', () => {
  test('X-Correlation-Id é propagado/gerado e devolvido', async () => {
    const mine = await call('GET', '/health/live', { headers: { 'x-correlation-id': 'abc-123' } });
    expect(mine.headers['x-correlation-id']).toBe('abc-123');
    const w = await createWallet();
    const err = await call('GET', `/wallets/${randomUUID()}`);
    expect(err.status).toBe(404);
    expect(err.body.correlationId).toBe(err.headers['x-correlation-id']);
    expect(w.walletId).toBeDefined();
  });

  test('rota inexistente -> 404 no formato padrão', async () => {
    expect(await call('GET', '/nope')).toMatchObject({ status: 404, body: { code: 'NOT_FOUND' } });
  });
});
