import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { type Infra, type Instance, startInfra, startInstance } from './support/cluster';

setDefaultTimeout(240_000);

let infra: Infra;
const instances: Instance[] = [];
beforeAll(async () => {
  infra = await startInfra();
}, 300_000);
afterAll(async () => {
  await Promise.all(instances.map((i) => i.kill().catch(() => undefined)));
  await infra?.stop();
}, 120_000);

describe('subida simultânea', () => {
  test('3 processos iniciando AO MESMO TEMPO num banco vazio: todos sobem e o schema é aplicado uma única vez', async () => {
    // waitReady:false => dispara os 3 juntos; só depois esperamos cada um (o que expõe a corrida das migrations)
    const started = await Promise.allSettled(
      ['p1', 'p2', 'p3'].map((id) => startInstance(infra, id, {}, { waitReady: true })),
    );
    for (const s of started) if (s.status === 'fulfilled') instances.push(s.value);

    const failures = started.filter((s): s is PromiseRejectedResult => s.status === 'rejected');
    expect(failures.map((f) => String(f.reason).slice(0, 400))).toEqual([]);

    const applied = await infra.query<{ name: string }>(`SELECT name FROM mikro_orm_migrations`);
    expect(applied).toHaveLength(1);
    for (const i of instances) expect(i.isAlive()).toBe(true);
  });
});
