import { EntitySchema } from '@mikro-orm/core';

/**
 * Modelos de persistência (linhas). Ficam separados do domínio: o domínio não conhece o ORM.
 * Dinheiro persiste em duas colunas — `numeric(20,2)` (lido como string, nunca como number)
 * e `currency` — e é reidratado como `Money` pelos mappers.
 * Os nomes de coluna seguem a UnderscoreNamingStrategy (playerId -> player_id).
 */

export class WalletRow {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export const WalletSchema = new EntitySchema<WalletRow>({
  class: WalletRow,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string', columnType: 'char(3)' },
    balance: { type: 'string', columnType: 'numeric(20,2)' },
    version: { type: 'integer' },
    createdAt: { type: 'Date', columnType: 'timestamptz' },
    updatedAt: { type: 'Date', columnType: 'timestamptz' },
  },
});

export class WagerTransactionRow {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  balanceSnapshot!: string;
  referenceAttempts!: number;
  nextAttemptAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRow>({
  class: WagerTransactionRow,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string' },
    externalTransactionId: { type: 'string' },
    idempotencyKey: { type: 'string' },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    walletId: { type: 'uuid' },
    playerId: { type: 'uuid' },
    roundId: { type: 'string' },
    gameId: { type: 'string' },
    kind: { type: 'string' },
    amount: { type: 'string', columnType: 'numeric(20,2)' },
    currency: { type: 'string', columnType: 'char(3)' },
    referenceExternalTransactionId: { type: 'string', nullable: true },
    referenceTransactionId: { type: 'uuid', nullable: true },
    status: { type: 'string' },
    failureCode: { type: 'string', nullable: true },
    balanceSnapshot: { type: 'string', columnType: 'numeric(20,2)' },
    referenceAttempts: { type: 'integer' },
    nextAttemptAt: { type: 'Date', columnType: 'timestamptz', nullable: true },
    createdAt: { type: 'Date', columnType: 'timestamptz' },
    processedAt: { type: 'Date', columnType: 'timestamptz', nullable: true },
  },
});

export class LedgerEntryRow {
  id!: string;
  walletId!: string;
  transactionId!: string;
  direction!: string;
  amount!: string;
  currency!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  createdAt!: Date;
}

// `seq` (identity) é gerado pelo banco e lido apenas via SQL na paginação.
export const LedgerEntrySchema = new EntitySchema<LedgerEntryRow>({
  class: LedgerEntryRow,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid' },
    transactionId: { type: 'uuid' },
    direction: { type: 'string' },
    amount: { type: 'string', columnType: 'numeric(20,2)' },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceBefore: { type: 'string', columnType: 'numeric(20,2)' },
    balanceAfter: { type: 'string', columnType: 'numeric(20,2)' },
    createdAt: { type: 'Date', columnType: 'timestamptz' },
  },
});

export class InboxMessageRow {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export const InboxMessageSchema = new EntitySchema<InboxMessageRow>({
  class: InboxMessageRow,
  tableName: 'inbox_messages',
  properties: {
    consumerName: { type: 'string', primary: true },
    messageId: { type: 'string', primary: true },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    receivedAt: { type: 'Date', columnType: 'timestamptz' },
    processedAt: { type: 'Date', columnType: 'timestamptz', nullable: true },
  },
});

export class OutboxMessageRow {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt?: Date | null;
  publishedAt?: Date | null;
}

export const OutboxMessageSchema = new EntitySchema<OutboxMessageRow>({
  class: OutboxMessageRow,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'uuid' },
    eventType: { type: 'string' },
    payload: { type: 'json' },
    occurredAt: { type: 'Date', columnType: 'timestamptz' },
    attempts: { type: 'integer' },
    nextAttemptAt: { type: 'Date', columnType: 'timestamptz', nullable: true },
    publishedAt: { type: 'Date', columnType: 'timestamptz', nullable: true },
  },
});

export const ALL_SCHEMAS = [
  WalletSchema,
  WagerTransactionSchema,
  LedgerEntrySchema,
  InboxMessageSchema,
  OutboxMessageSchema,
];
