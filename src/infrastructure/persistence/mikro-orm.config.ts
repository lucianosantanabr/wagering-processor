import { DefaultLogger } from '@mikro-orm/core';
import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { ALL_SCHEMAS } from './entities';
import { Migration20260801000000_init } from './migrations/Migration20260801000000_init';

export interface OrmConfigOptions {
  /**
   * Destino das mensagens do proprio MikroORM (ex.: "[migrator] Processing ..."). Por padrao ele escreve
   * texto cru no console, o que quebra coletores de logs JSON; o main.ts roteia para o logger estruturado.
   */
  logWriter?: (message: string) => void;
}

export function buildOrmConfig(clientUrl: string = process.env.DATABASE_URL ?? '', options: OrmConfigOptions = {}) {
  return defineConfig({
    clientUrl,
    ...(options.logWriter ? { loggerFactory: (o) => new DefaultLogger({ ...o, writer: options.logWriter! }) } : {}),
    entities: ALL_SCHEMAS,
    extensions: [Migrator],
    migrations: {
      migrationsList: [Migration20260801000000_init],
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
    // O schema é governado exclusivamente pelas migrations.
    allowGlobalContext: false,
  });
}
