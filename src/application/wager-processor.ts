import { InsufficientFundsError } from '@domain/errors';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '@domain/events';
import { FailureCode } from '@domain/failure-code';
import type { IntegrationEvent } from '@domain/integration-event';
import { WagerTransaction, WagerTransactionKind as K, WagerTransactionStatus as S } from '@domain/wager-transaction';
import type { Wallet } from '@domain/wallet';
import { LedgerDirection, type WalletLedgerEntry } from '@domain/wallet-ledger-entry';
import type { Clock, IdGenerator, Repositories } from './ports';

export interface ProcessingContext {
  correlationId: string;
  causationId?: string;
}

export interface ProcessingResult {
  /** Lançamento gerado (apenas quando o saldo mudou). */
  entry?: WalletLedgerEntry;
  events: IntegrationEvent<unknown>[];
}

/**
 * Regras de negócio de BET/WIN/LOSS/REFUND/ROLLBACK. Muta `tx` e `wallet` em memória e faz
 * leituras para resolver referências; quem chama (use case) persiste tudo na mesma transação SQL,
 * já com a wallet travada (`FOR UPDATE`). Reutilizável pelo worker de PENDING_REFERENCE.
 */
export class WagerProcessor {
  constructor(
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async apply(
    tx: WagerTransaction,
    wallet: Wallet,
    repos: Repositories,
    ctx: ProcessingContext,
  ): Promise<ProcessingResult> {
    const now = this.clock.now();
    const events: IntegrationEvent<unknown>[] = [];
    const evCtx = () => ({ eventId: this.ids.next(), occurredAt: now, ...ctx });

    const reject = (code: FailureCode): ProcessingResult => {
      tx.reject(code);
      events.push(WagerTransactionRejected.from(tx, evCtx()));
      return { events };
    };

    // ── identidade da operação x wallet ──
    if (wallet.playerId !== tx.playerId) return reject(FailureCode.WalletMismatch);
    if (wallet.currency !== tx.money.currency) return reject(FailureCode.CurrencyMismatch);

    switch (tx.kind) {
      case K.Bet: {
        return this.move(tx, wallet, events, evCtx, now, undefined, FailureCode.InsufficientFunds);
      }
      case K.Win: {
        // A referência de um WIN (BET da rodada) é informativa: não bloqueia o crédito.
        return this.move(tx, wallet, events, evCtx, now, undefined, FailureCode.InsufficientFunds);
      }
      case K.Loss: {
        tx.markProcessed(undefined, now);
        events.push(WagerTransactionProcessed.from(tx, evCtx()));
        return { events };
      }
      case K.Refund:
      case K.Rollback:
        return this.reverse(tx, wallet, repos, events, evCtx, now, reject);
      default:
        // OPENING e valores desconhecidos nunca chegam aqui (create() os barra).
        return reject(FailureCode.ProcessingError);
    }
  }

  /**
   * Limite de espera esgotado: a referência nunca chegou. Rejeita com REFERENCE_NOT_FOUND (terminal) e
   * emite WagerTransactionRejected, para o provedor reenviar a original e a reversão.
   */
  expire(tx: WagerTransaction, ctx: ProcessingContext): ProcessingResult {
    tx.reject(FailureCode.ReferenceNotFound);
    return {
      events: [
        WagerTransactionRejected.from(tx, {
          eventId: this.ids.next(),
          occurredAt: this.clock.now(),
          ...ctx,
        }),
      ],
    };
  }

  private async reverse(
    tx: WagerTransaction,
    wallet: Wallet,
    repos: Repositories,
    events: IntegrationEvent<unknown>[],
    evCtx: () => { eventId: string; occurredAt: Date; correlationId: string; causationId?: string },
    now: Date,
    reject: (code: FailureCode) => ProcessingResult,
  ): Promise<ProcessingResult> {
    const stored = await repos.transactions.findByProviderExternalId(
      tx.providerId,
      tx.referenceExternalTransactionId as string,
    );

    if (!stored) return this.awaitReference(tx, events, evCtx);
    const ref = stored.tx;

    // mesma wallet/player/moeda/rodada (provider é igual por construção da busca)
    if (
      ref.walletId !== tx.walletId ||
      ref.playerId !== tx.playerId ||
      ref.money.currency !== tx.money.currency ||
      ref.roundId !== tx.roundId
    ) {
      return reject(FailureCode.ReferenceMismatch);
    }

    const validKinds = tx.kind === K.Refund ? [K.Bet] : [K.Bet, K.Win, K.Refund];
    if (!validKinds.includes(ref.kind)) return reject(FailureCode.InvalidReferenceKind);

    if (ref.status === S.Rejected || ref.status === S.Failed) return reject(FailureCode.ReferenceNotProcessed);
    if (ref.status !== S.Processed) return this.awaitReference(tx, events, evCtx); // ainda em trânsito

    if (!ref.money.equals(tx.money)) return reject(FailureCode.AmountMismatch);

    if (await repos.transactions.findProcessedReversalOf(ref.id)) return reject(FailureCode.AlreadyReversed);

    return this.move(tx, wallet, events, evCtx, now, ref, FailureCode.ReversalWouldOverdraw);
  }

  /** Referência ausente: PENDING (primeira vez) fica PENDING_REFERENCE; no reprocesso o estado é mantido. */
  private awaitReference(
    tx: WagerTransaction,
    events: IntegrationEvent<unknown>[],
    evCtx: () => { eventId: string; occurredAt: Date; correlationId: string; causationId?: string },
  ): ProcessingResult {
    if (tx.status === S.Pending) {
      tx.markPendingReference();
      events.push(WagerTransactionPendingReference.from(tx, evCtx()));
    }
    return { events };
  }

  /** Aplica débito/crédito conforme a direção da transação; saldo insuficiente vira rejeição com o código dado. */
  private move(
    tx: WagerTransaction,
    wallet: Wallet,
    events: IntegrationEvent<unknown>[],
    evCtx: () => { eventId: string; occurredAt: Date; correlationId: string; causationId?: string },
    now: Date,
    reference: WagerTransaction | undefined,
    insufficientCode: FailureCode,
  ): ProcessingResult {
    const direction = tx.ledgerDirectionFor(reference);
    const movement = { entryId: this.ids.next(), transactionId: tx.id, money: tx.money, at: now };
    let entry: WalletLedgerEntry;
    try {
      entry = direction === LedgerDirection.Debit ? wallet.debit(movement) : wallet.credit(movement);
    } catch (e) {
      if (e instanceof InsufficientFundsError) {
        tx.reject(insufficientCode);
        events.push(WagerTransactionRejected.from(tx, evCtx()));
        return { events };
      }
      throw e;
    }
    tx.markProcessed(reference?.id, now);
    events.push(WagerTransactionProcessed.from(tx, evCtx()));
    events.push(WalletBalanceChanged.from(wallet, entry, evCtx()));
    return { entry, events };
  }
}
