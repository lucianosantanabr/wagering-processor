import type { MikroORM } from '@mikro-orm/postgresql';
import type { Repositories, UnitOfWork } from '@application/ports';
import { buildRepositories } from './repositories';

/**
 * Uma transação SQL por chamada, em um EntityManager isolado (fork): sem estado compartilhado
 * entre requisições/mensagens concorrentes. Isolamento READ COMMITTED + lock de linha por wallet.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(private readonly orm: MikroORM) {}

  run<T>(fn: (repos: Repositories) => Promise<T>): Promise<T> {
    return this.orm.em.fork().transactional((em) => fn(buildRepositories(em)));
  }
}
