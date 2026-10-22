import { WagerTransactionStatus } from '@domain/wager-transaction';
import { OutboxMessage } from '@domain/outbox-message';
import { type Logger, type Metrics, noopLogger, noopMetrics } from './observability';
import type { Clock, IdGenerator, UnitOfWork } from './ports';
import { DEFAULT_REFERENCE_POLICY, type ReferencePolicy, nextReferenceAttemptAt } from './reference-policy';
import type { WagerProcessor } from './wager-processor';

export interface PendingReferenceOptions {
  /** Transações vencidas selecionadas por rodada. */
  batchSize: number;
  /** Itens processados em paralelo (cada um em sua própria transação). */
  concurrency: number;
}

export const DEFAULT_PENDING_REFERENCE_OPTIONS: PendingReferenceOptions = { batchSize: 50, concurrency: 4 };

export type PendingOutcome = 'resolved' | 'rejected' | 'expired' | 'retried' | 'skipped' | 'failed';

export interface PendingReferenceRound {
  selected: number;
  resolved: number; // reversão aplicada (PROCESSED)
  rejected: number; // referência apareceu, mas a regra de negócio rejeitou
  expired: number; // limite esgotado -> REFERENCE_NOT_FOUND
  retried: number; // referência ainda ausente; reagendada com backoff
  skipped: number; // outro worker foi mais rápido
  failed: number; // erro inesperado (a transação do item foi desfeita; tenta de novo depois)
}

/**
 * Worker de PENDING_REFERENCE (item 7.1). Cada item roda em UMA transação própria:
 *
 *   1. lê a transação (sem lock) só para descobrir a wallet;
 *   2. trava a WALLET (`FOR UPDATE`);
 *   3. trava a transação pendente (`FOR UPDATE SKIP LOCKED`) e confirma que ainda está pendente;
 *   4. reaplica as MESMAS regras do caminho normal (`WagerProcessor`);
 *   5. grava o novo estado, o lançamento (se houver) e os eventos na outbox; commit.
 *
 * Ordem de locks: sempre wallet -> transação, igual ao caminho de submissão; e um único lock de wallet por
 * transação. Assim não há ciclo possível, mesmo com N workers em N instâncias. Vários workers podem
 * selecionar o mesmo id: o `SKIP LOCKED` e a rechecagem de status garantem que só um aplica.
 */
export class ProcessPendingReferences {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly processor: WagerProcessor,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
    private readonly policy: ReferencePolicy = DEFAULT_REFERENCE_POLICY,
    private readonly options: PendingReferenceOptions = DEFAULT_PENDING_REFERENCE_OPTIONS,
  ) {}

  async runOnce(): Promise<PendingReferenceRound> {
    const due = await this.uow.run((r) => r.transactions.findDuePendingReference(this.clock.now(), this.options.batchSize));
    const round: PendingReferenceRound = { selected: due.length, resolved: 0, rejected: 0, expired: 0, retried: 0, skipped: 0, failed: 0 };

    for (let i = 0; i < due.length; i += this.options.concurrency) {
      const chunk = due.slice(i, i + this.options.concurrency);
      const outcomes = await Promise.all(chunk.map((id) => this.processOne(id)));
      for (const o of outcomes) round[o] += 1;
    }
    return round;
  }

  private async processOne(id: string): Promise<PendingOutcome> {
    try {
      return await this.uow.run((repos) => this.handle(repos, id));
    } catch (e) {
      // O rollback desfez só este item; ele continua pendente e vencido, então a próxima rodada tenta de novo.
      this.logger.error('pending reference processing failed', {
        transactionId: id,
        error: e instanceof Error ? e.message : String(e),
      });
      this.metrics.increment('pending_reference_total', { outcome: 'failed' });
      return 'failed';
    }
  }

  private async handle(repos: Parameters<Parameters<UnitOfWork['run']>[0]>[0], id: string): Promise<PendingOutcome> {
    const peek = await repos.transactions.findById(id);
    if (!peek || peek.tx.status !== WagerTransactionStatus.PendingReference) return this.done('skipped');

    const wallet = await repos.wallets.findByIdForUpdate(peek.tx.walletId); // 1º: wallet
    if (!wallet) return this.done('skipped');
    const locked = await repos.transactions.lockPendingReference(id); // 2º: transação (SKIP LOCKED)
    if (!locked) return this.done('skipped'); // outro worker a tem, ou já foi resolvida enquanto esperávamos a wallet

    const { tx, balanceSnapshot, referenceAttempts } = locked;
    const now = this.clock.now();
    const attempts = referenceAttempts + 1;
    const previousVersion = wallet.version;
    const ctx = { correlationId: this.ids.next(), causationId: tx.idempotencyKey };

    let result = await this.processor.apply(tx, wallet, repos, ctx);
    let outcome: PendingOutcome;

    if (tx.status === WagerTransactionStatus.PendingReference) {
      // A referência continua ausente (ou ainda em trânsito).
      if (attempts >= this.policy.maxAttempts) {
        result = this.processor.expire(tx, ctx);
        outcome = 'expired';
        await repos.transactions.updateState(tx, wallet.balance, { attempts, nextAttemptAt: null });
      } else {
        outcome = 'retried';
        // O saldo "original" da transação não muda: só o agendamento.
        await repos.transactions.updateState(tx, balanceSnapshot, {
          attempts,
          nextAttemptAt: nextReferenceAttemptAt(now, attempts, this.policy),
        });
      }
    } else {
      outcome = tx.status === WagerTransactionStatus.Processed ? 'resolved' : 'rejected';
      await repos.transactions.updateState(tx, wallet.balance, { attempts, nextAttemptAt: null });
      if (result.entry) {
        await repos.wallets.saveBalance(wallet, previousVersion);
        await repos.ledger.insert(result.entry);
      }
    }

    for (const event of result.events) await repos.outbox.enqueue(OutboxMessage.enqueue(event));

    if (outcome !== 'retried') {
      this.metrics.increment('wager_transactions_total', { kind: tx.kind, status: tx.status });
      this.metrics.observe('pending_reference_wait_seconds', Math.max(0, (now.getTime() - tx.createdAt.getTime()) / 1000));
      const log = outcome === 'expired' ? this.logger.warn.bind(this.logger) : this.logger.info.bind(this.logger);
      log('pending reference concluded', {
        outcome,
        transactionId: tx.id,
        walletId: tx.walletId,
        providerId: tx.providerId,
        attempts,
        failureCode: tx.failureCode,
      });
    }
    return this.done(outcome);
  }

  private done(outcome: PendingOutcome): PendingOutcome {
    this.metrics.increment('pending_reference_total', { outcome });
    return outcome;
  }
}
