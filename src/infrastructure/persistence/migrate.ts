import type { MikroORM } from '@mikro-orm/postgresql';

/** Chave arbitrária e fixa do advisory lock de migrations (a mesma em todas as instâncias). */
const MIGRATION_LOCK_KEY = 7_270_001;

/**
 * Aplica as migrations pendentes de forma segura com N instâncias subindo ao mesmo tempo.
 *
 * O migrator do MikroORM NÃO serializa instâncias concorrentes: com 3 processos iniciando juntos num banco
 * vazio, dois falharam com `duplicate key … pg_class_relname_nsp_index` (corrida no CREATE TABLE; provado em
 * test/e2e). Aqui, uma transação externa toma `pg_advisory_xact_lock` e a mantém aberta enquanto as migrations
 * rodam; as demais instâncias esperam o lock, e quando entram já encontram tudo aplicado (no-op).
 * O lock é liberado no commit/rollback, inclusive se o processo morrer (a conexão cai).
 */
export async function migrateWithLock(orm: MikroORM): Promise<void> {
  await orm.em.fork().transactional(async (em) => {
    await em.execute('select pg_advisory_xact_lock(?)', [MIGRATION_LOCK_KEY]);
    await orm.migrator.up();
  });
}
