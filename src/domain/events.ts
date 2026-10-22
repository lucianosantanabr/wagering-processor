import { type EventContext, IntegrationEvent } from './integration-event';
import type { MoneyProps } from './money';
import type { WagerTransaction } from './wager-transaction';
import type { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';
import type { Wallet } from './wallet';

interface WagerTransactionEventData {
  transactionId: string;
  walletId: string;
  playerId: string;
  providerId: string;
  externalTransactionId: string;
  roundId: string;
  kind: string;
  money: MoneyProps;
}

function txData(tx: WagerTransaction): WagerTransactionEventData {
  return {
    transactionId: tx.id,
    walletId: tx.walletId,
    playerId: tx.playerId,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    roundId: tx.roundId,
    kind: tx.kind,
    money: tx.money.toJSON(),
  };
}

// ---- WagerTransactionProcessed

export interface WagerTransactionProcessedData extends WagerTransactionEventData {
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed({
      ...ctx,
      aggregateId: tx.id,
      data: { ...txData(tx), processedAt: (tx.processedAt ?? ctx.occurredAt).toISOString() },
    });
  }
}

// ---- WagerTransactionRejected

export interface WagerTransactionRejectedData extends WagerTransactionEventData {
  failureCode: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected({
      ...ctx,
      aggregateId: tx.id,
      data: { ...txData(tx), failureCode: tx.failureCode ?? 'UNKNOWN' },
    });
  }
}

// ---- WagerTransactionPendingReference

export interface WagerTransactionPendingReferenceData extends WagerTransactionEventData {
  referenceExternalTransactionId: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({
      ...ctx,
      aggregateId: tx.id,
      data: {
        ...txData(tx),
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? '',
      },
    });
  }
}

// ---- WalletBalanceChanged

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...ctx,
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
