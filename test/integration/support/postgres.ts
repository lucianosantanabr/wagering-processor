import { MikroORM } from '@mikro-orm/postgresql';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { removeContainer, runContainer } from './container';

export interface TestDb {
  orm: MikroORM;
  url: string;
  /** Executa SQL cru e devolve as linhas. Sem parâmetros, vários statements rodam como uma só transação. */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>;
  stop(): Promise<void>;
}

/** Sobe um PostgreSQL real em container (podman ou docker) e aplica as migrations (up). */
export async function startTestDb(): Promise<TestDb> {
  const c = await runContainer({
    image: 'postgres:16-alpine',
    port: 5432,
    env: { POSTGRES_USER: 'wagering', POSTGRES_PASSWORD: 'wagering', POSTGRES_DB: 'wagering' },
    readyCommand: ['pg_isready', '-U', 'wagering', '-d', 'wagering'],
  });
  const url = `postgresql://wagering:wagering@127.0.0.1:${c.hostPort}/wagering`;
  try {
    const orm = await initWithRetry(url);
    await orm.migrator.up();
    return {
      orm,
      url,
      query: (sql, params) => orm.em.getConnection().execute(sql, params) as Promise<never>,
      stop: async () => {
        await orm.close(true);
        await removeContainer(c.id);
      },
    };
  } catch (e) {
    await removeContainer(c.id);
    throw e;
  }
}

async function initWithRetry(url: string, attempts = 30): Promise<MikroORM> {
  let last: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      const orm = await MikroORM.init(buildOrmConfig(url));
      await orm.em.getConnection().execute('select 1');
      return orm;
    } catch (e) {
      last = e;
      await new Promise((r) => setTimeout(r, 500));
    }
  }
  throw last;
}
