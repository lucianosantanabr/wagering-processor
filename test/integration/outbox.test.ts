import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { OutboxPublisherWorker } from '@application/publish-outbox';
import { buildOutboxPublisher, buildServices } from '@infra/composition';
import { type TestApp, cmd, newWallet, startApp } from './support/app';
import { FakeClock, RecordingMetrics, RecordingPublisher, sleep } from './support/outbox-fakes';

let app: TestApp;
// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  app = await startApp();
}, 180_000);
afterAll(async () => {
  await app?.stop();
}, 60_000);

const pendingCount = async () =>
  Number((await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL`))[0]?.n);

/** Isola os testes: encerra qualquer pendencia (inclusive as com retry agendado) deixada pelos anteriores. */
const reset = () => app.db.query(`UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL`);

describe('PublishOutbox', () => {
  test('publica o que foi commitado e marca published_at; a rodada seguinte não republica', async () => {
    await reset();
    const f = await newWallet(app, '100.00');
    await app.submitWagerTransaction.execute(cmd(f));
    expect(await pendingCount()).toBe(4); // abertura (2 eventos) + BET (2 eventos)

    const broker = new RecordingPublisher();
    const pub = buildOutboxPublisher(app, broker);
    const round = await pub.runOnce();
    expect(round).toEqual({ claimed: 4, published: 4, failed: 0 });
    expect(new Set(broker.published).size).toBe(4);
    expect(await pendingCount()).toBe(0);

    expect((await pub.runOnce()).claimed).toBe(0);
    expect(broker.published).toHaveLength(4);
  });

  test('nada é publicado de uma transação que sofreu rollback', async () => {
    await reset();
    const failing = {
      run: <T>(fn: Parameters<typeof app.uow.run<T>>[0]) =>
        app.uow.run(async (repos) => {
          const r = await fn(repos);
          throw new Error('crash antes do commit');
          return r;
        }),
    };
    const broken = buildServices(app.db.orm, { uow: failing });
    const f = await newWallet(app, '100.00');
    await reset(); // esvazia os eventos da abertura
    await expect(broken.submitWagerTransaction.execute(cmd(f))).rejects.toThrow('crash antes do commit');

    const broker = new RecordingPublisher();
    expect((await buildOutboxPublisher(app, broker).runOnce()).claimed).toBe(0);
    expect(broker.published).toHaveLength(0);
  });

  test('falha no broker agenda retry com backoff exponencial e respeita o nextAttemptAt', async () => {
    await reset();
    const clock = new FakeClock();
    const metrics = new RecordingMetrics();
    const f = await newWallet(app, '100.00');
    const broker = new RecordingPublisher();
    broker.failAll = true;
    const pub = buildOutboxPublisher({ ...app, clock, metrics }, broker);

    // 1ª tentativa: falha -> attempts=1, próxima em +1s
    expect(await pub.runOnce()).toMatchObject({ claimed: 2, published: 0, failed: 2 });
    const rows = () =>
      app.db.query<{ attempts: number; next_attempt_at: string; published_at: unknown }>(
        `SELECT attempts, next_attempt_at, published_at FROM outbox_messages
          WHERE aggregate_id = '${f.walletId}' OR payload->'data'->>'walletId' = '${f.walletId}'`,
      );
    let r = await rows();
    expect(r.every((x) => x.attempts === 1 && x.published_at === null)).toBe(true);
    const next1 = new Date(r[0]!.next_attempt_at).getTime();
    expect(next1 - clock.now().getTime()).toBe(1000);

    // ainda não venceu: nenhuma reserva
    clock.advance(999);
    expect((await pub.runOnce()).claimed).toBe(0);

    // venceu, broker segue fora: 2ª falha -> backoff dobra (+2s)
    clock.advance(1);
    expect(await pub.runOnce()).toMatchObject({ claimed: 2, failed: 2 });
    r = await rows();
    expect(r.every((x) => x.attempts === 2)).toBe(true);
    expect(new Date(r[0]!.next_attempt_at).getTime() - clock.now().getTime()).toBe(2000);

    // broker volta: publica
    broker.failAll = false;
    clock.advance(2000);
    expect(await pub.runOnce()).toMatchObject({ claimed: 2, published: 2, failed: 0 });
    expect(await pendingCount()).toBe(0);
    expect(metrics.counters.get('outbox_publish_failures_total')).toBe(4);
    expect(metrics.counters.get('outbox_published_total')).toBe(2);
  });

  test('uma mensagem com falha não impede as demais da rodada', async () => {
    await reset();
    await newWallet(app, '10.00');
    const broker = new RecordingPublisher();
    const [first] = await app.db.query<{ id: string }>(`SELECT id FROM outbox_messages WHERE published_at IS NULL ORDER BY seq LIMIT 1`);
    broker.failIds.add(first!.id);
    expect(await buildOutboxPublisher(app, broker).runOnce()).toMatchObject({ claimed: 2, published: 1, failed: 1 });
    expect(await pendingCount()).toBe(1);
  });

  test('outbox lag: gauge com a idade da mensagem pendente mais antiga (0 se vazia)', async () => {
    await reset();
    const metrics = new RecordingMetrics();
    const clock = new FakeClock();
    const pub = buildOutboxPublisher({ ...app, clock, metrics }, new RecordingPublisher());
    expect(await pub.reportLag()).toBe(0);

    await newWallet(app, '10.00');
    clock.advance(30_000);
    const lag = await pub.reportLag();
    expect(lag).toBeGreaterThan(29); // o relogio falso nasce alguns ms antes da wallet
    expect(lag).toBeLessThan(31);
    expect(metrics.gauges.get('outbox_lag_seconds')).toBe(lag);
  });
});

describe('OutboxPublisherWorker', () => {
  test('publica em segundo plano e para de forma graciosa', async () => {
    await reset();
    const broker = new RecordingPublisher();
    const worker = new OutboxPublisherWorker(buildOutboxPublisher(app, broker), app.logger, { idleIntervalMs: 20, batchSize: 50 });
    worker.start();
    await newWallet(app, '10.00');
    for (let i = 0; i < 100 && (await pendingCount()) > 0; i++) await sleep(20);
    expect(await pendingCount()).toBe(0);
    expect(broker.published).toHaveLength(2);

    await worker.stop(); // não pendura e não deixa rodada pela metade
    const before = broker.published.length;
    await newWallet(app, '10.00');
    await sleep(100);
    expect(broker.published.length).toBe(before); // parado: não publica mais
    expect(await pendingCount()).toBe(2);
  });

  test('sobrevive a uma rodada que falha (ex.: banco indisponível) e continua', async () => {
    await reset();
    let calls = 0;
    const flaky = {
      runOnce: async () => {
        calls += 1;
        if (calls === 1) throw new Error('db down');
        return { claimed: 0, published: 0, failed: 0 };
      },
      reportLag: async () => 0,
    };
    const worker = new OutboxPublisherWorker(flaky as never, app.logger, { idleIntervalMs: 10, batchSize: 50 });
    worker.start();
    for (let i = 0; i < 50 && calls < 3; i++) await sleep(10);
    await worker.stop();
    expect(calls).toBeGreaterThanOrEqual(3);
  });
});
