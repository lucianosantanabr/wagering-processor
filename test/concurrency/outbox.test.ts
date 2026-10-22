import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import type { EventPublisher, UnitOfWork } from '@application/ports';
import type { PublishOutbox } from '@application/publish-outbox';
import { buildOutboxPublisher, buildServices } from '@infra/composition';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { type TestApp, startApp } from '../integration/support/app';
import { RecordingPublisher } from '../integration/support/outbox-fakes';

let app: TestApp;
let extra: MikroORM[] = [];
// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  app = await startApp();
  extra = await Promise.all([1, 2].map(() => MikroORM.init(buildOrmConfig(app.db.url))));
}, 180_000);
afterAll(async () => {
  await Promise.all(extra.map((o) => o.close(true)));
  await app?.stop();
}, 60_000);

const seed = (n: number) =>
  app.db.query(
    `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at)
     SELECT gen_random_uuid(), gen_random_uuid(), 'TestEvent', '{"data":{}}'::jsonb, now()
       FROM generate_series(1, ${n})`,
  );
const clear = () => app.db.query(`UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL`);
const pending = async () =>
  Number((await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL`))[0]?.n);

/** 3 "instâncias": cada uma com o seu pool de conexões. */
const instances = (broker: EventPublisher, uow?: (orm: MikroORM) => UnitOfWork): PublishOutbox[] =>
  [app.db.orm, ...extra].map((orm) =>
    buildOutboxPublisher(buildServices(orm, uow ? { uow: uow(orm) } : {}), broker, { batchSize: 10, concurrency: 5 }),
  );

async function drainConcurrently(pubs: PublishOutbox[]) {
  const claimedBy = pubs.map(() => 0);
  await Promise.all(
    pubs.map(async (p, i) => {
      for (let k = 0; k < 200; k++) {
        const r = await p.runOnce();
        claimedBy[i]! += r.claimed;
        if (r.claimed === 0) return;
      }
    }),
  );
  return claimedBy;
}

describe('múltiplos publishers sobre a mesma outbox', () => {
  test('3 publishers concorrentes: cada evento é publicado exatamente uma vez e todos são publicados', async () => {
    await clear();
    await seed(300);
    const broker = new RecordingPublisher();
    broker.delayMs = 3; // aumenta a sobreposição entre os publishers
    const claimedBy = await drainConcurrently(instances(broker));

    expect(broker.published).toHaveLength(300);
    expect(new Set(broker.published).size).toBe(300); // nenhuma duplicata
    expect(claimedBy.reduce((a, b) => a + b, 0)).toBe(300);
    expect(claimedBy.filter((c) => c > 0).length).toBeGreaterThan(1); // o trabalho foi realmente dividido
    expect(await pending()).toBe(0);
  });

  test('SKIP LOCKED: enquanto A segura um lote em publicação, B pega OUTRO lote sem esperar', async () => {
    await clear();
    await seed(20);
    const slow = new RecordingPublisher();
    slow.delayMs = 400;
    const [a, b] = instances(slow);
    const started = Date.now();
    const [ra, rb] = await Promise.all([a!.runOnce(), b!.runOnce()]);
    expect(ra.claimed).toBe(10);
    expect(rb.claimed).toBe(10); // lotes disjuntos
    expect(Date.now() - started).toBeLessThan(1500); // em paralelo, não em série
    expect(new Set(slow.published).size).toBe(20);
  });
});

describe('recuperação após crash', () => {
  test('commit feito, processo morre antes de publicar: outra instância assume e publica', async () => {
    await clear();
    await seed(5);
    // instância A "morre" logo ao tentar publicar (nada chega ao broker, nada é marcado)
    const dying: EventPublisher = {
      publish: async () => {
        throw new Error('process killed');
      },
    };
    const crashing = buildOutboxPublisher(buildServices(app.db.orm), {
      publish: async () => {
        throw Object.assign(new Error('SIGKILL'), { fatal: true });
      },
    });
    // simula morte: a rodada inteira é abortada antes do commit
    const dead: UnitOfWork = {
      run: (fn) =>
        buildServices(app.db.orm).uow.run(async (repos) => {
          await fn(repos);
          throw new Error('SIGKILL antes do commit');
        }),
    };
    const killed = buildOutboxPublisher({ ...buildServices(app.db.orm), uow: dead }, dying);
    await expect(killed.runOnce()).rejects.toThrow('SIGKILL');
    void crashing;
    expect(await pending()).toBe(5); // nada foi perdido: continuam pendentes

    const broker = new RecordingPublisher();
    const survivor = instances(broker)[1]!;
    expect(await survivor.runOnce()).toMatchObject({ claimed: 5, published: 5 });
    expect(await pending()).toBe(0);
  });

  test('morte DEPOIS de publicar e ANTES do commit: republicação duplicada, e o consumidor idempotente aplica uma vez', async () => {
    await clear();
    await seed(5);
    const broker = new RecordingPublisher();

    // instância A publica no broker e morre antes de gravar published_at
    const deadUow: UnitOfWork = {
      run: (fn) =>
        buildServices(app.db.orm).uow.run(async (repos) => {
          await fn(repos);
          throw new Error('SIGKILL entre publicar e commitar');
        }),
    };
    const a = buildOutboxPublisher({ ...buildServices(app.db.orm), uow: deadUow }, broker);
    await expect(a.runOnce()).rejects.toThrow('SIGKILL');
    expect(broker.published).toHaveLength(5); // o broker já recebeu
    expect(await pending()).toBe(5); // mas a outbox ainda acha que está pendente

    // instância B assume e publica de novo
    await instances(broker)[2]!.runOnce();
    expect(broker.published).toHaveLength(10);
    expect(await pending()).toBe(0);

    // At-least-once: há duplicatas no broker, mas o consumidor deduplica por eventId (id da mensagem)
    const applied = new Map<string, number>();
    const seen = new Set<string>();
    for (const id of broker.published) {
      if (seen.has(id)) continue; // inbox do consumidor
      seen.add(id);
      applied.set(id, (applied.get(id) ?? 0) + 1);
    }
    expect(applied.size).toBe(5);
    expect([...applied.values()].every((n) => n === 1)).toBe(true);
    expect(new Set(broker.published).size).toBe(5);
  });

  test('broker fora do ar durante a rodada: nenhuma mensagem é perdida e tudo sai quando volta', async () => {
    await clear();
    await seed(30);
    const broker = new RecordingPublisher();
    broker.failAll = true;
    const [p] = instances(broker);
    const r = await p!.runOnce();
    expect(r).toMatchObject({ published: 0, failed: 10 });
    expect(await pending()).toBe(30);

    broker.failAll = false;
    await app.db.query(`UPDATE outbox_messages SET next_attempt_at = NULL`); // pula a espera do backoff
    await drainConcurrently(instances(broker));
    expect(new Set(broker.published).size).toBe(30);
    expect(await pending()).toBe(0);
  });
});
