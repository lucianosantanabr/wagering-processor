import { DeadlockException, LockWaitTimeoutException } from '@mikro-orm/core';
import type { Metrics } from '@application/observability';
import type { Repositories, UnitOfWork } from '@application/ports';

/** Tipo de conflito de concorrência que o PostgreSQL abortou (null se o erro é de outra natureza). */
export function lockConflictType(e: unknown): 'deadlock' | 'lock_timeout' | 'serialization' | null {
  if (e instanceof DeadlockException) return 'deadlock';
  if (e instanceof LockWaitTimeoutException) return 'lock_timeout';
  const code = (e as { code?: unknown; cause?: { code?: unknown } } | null)?.code ?? (e as { cause?: { code?: unknown } } | null)?.cause?.code;
  if (code === '40P01') return 'deadlock';
  if (code === '55P03') return 'lock_timeout';
  if (code === '40001') return 'serialization';
  return null;
}

/**
 * Decorator de `UnitOfWork` que mede, sem alterar o comportamento:
 *  - `db_transaction_duration_seconds{outcome}`   duração de cada transação SQL (commit/rollback);
 *  - `wallet_lock_wait_seconds`                   tempo esperando o `SELECT … FOR UPDATE` da wallet (wallet quente);
 *  - `lock_conflicts_total{type}`                 deadlock / lock_timeout / serialization abortados pelo banco.
 */
export class InstrumentedUnitOfWork implements UnitOfWork {
  constructor(
    private readonly inner: UnitOfWork,
    private readonly metrics: Metrics,
  ) {}

  async run<T>(fn: (repos: Repositories) => Promise<T>): Promise<T> {
    const started = performance.now();
    let outcome: 'commit' | 'rollback' = 'commit';
    try {
      return await this.inner.run((repos) => fn(this.instrument(repos)));
    } catch (e) {
      outcome = 'rollback';
      const conflict = lockConflictType(e);
      if (conflict) this.metrics.increment('lock_conflicts_total', { type: conflict });
      throw e;
    } finally {
      this.metrics.observe('db_transaction_duration_seconds', (performance.now() - started) / 1000, { outcome });
    }
  }

  private instrument(repos: Repositories): Repositories {
    const metrics = this.metrics;
    // Object.create preserva os métodos (protótipo) e só sobrescreve o que medimos.
    const wallets = Object.assign(Object.create(repos.wallets), {
      findByIdForUpdate: async (id: string) => {
        const started = performance.now();
        try {
          return await repos.wallets.findByIdForUpdate(id);
        } finally {
          metrics.observe('wallet_lock_wait_seconds', (performance.now() - started) / 1000);
        }
      },
    });
    return { ...repos, wallets };
  }
}
