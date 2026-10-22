import type { InboxMessage } from '@domain/inbox-message';
import type { Money } from '@domain/money';
import type { OutboxMessage } from '@domain/outbox-message';
import type { WagerTransaction } from '@domain/wager-transaction';
import type { Wallet } from '@domain/wallet';
import type { WalletLedgerEntry } from '@domain/wallet-ledger-entry';

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  next(): string;
}

/** Transação + saldo da wallet observado quando ela chegou ao estado atual (base do replay). */
export interface StoredTransaction {
  tx: WagerTransaction;
  balanceSnapshot: Money;
  /** Reprocessamentos já feitos enquanto aguardava a referência. */
  referenceAttempts: number;
}

/** Agendamento do próximo reprocessamento de uma transação em PENDING_REFERENCE. */
export interface ReferenceRetry {
  attempts: number;
  nextAttemptAt: Date | null;
}

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  /** `SELECT … FOR UPDATE` da linha da wallet: serializa todas as operações dessa wallet. */
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  insert(wallet: Wallet): Promise<void>;
  /** UPDATE condicionado à versão anterior; falha se 0 linhas forem afetadas. */
  saveBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<StoredTransaction | null>;
  findByIdempotencyKey(key: string): Promise<StoredTransaction | null>;
  findByProviderExternalId(providerId: string, externalTransactionId: string): Promise<StoredTransaction | null>;
  /** Transação PROCESSED (REFUND/ROLLBACK) que já reverteu `referenceTransactionId`, se houver. */
  findProcessedReversalOf(referenceTransactionId: string): Promise<WagerTransaction | null>;
  insert(tx: WagerTransaction, balanceSnapshot: Money, retry?: ReferenceRetry): Promise<void>;
  /**
   * Persiste a transição de estado de uma transação já existente (ex.: PENDING_REFERENCE -> PROCESSED).
   * `retry` atualiza o agendamento (omitido = mantém; `nextAttemptAt: null` = sem nova tentativa).
   */
  updateState(tx: WagerTransaction, balanceSnapshot: Money, retry?: ReferenceRetry): Promise<void>;
  /** Ids em PENDING_REFERENCE cuja próxima tentativa já venceu (leitura sem lock, mais antigas primeiro). */
  findDuePendingReference(now: Date, limit: number): Promise<string[]>;
  /**
   * Trava a transação pendente com `FOR UPDATE SKIP LOCKED` (devolve `null` se outro worker a tem
   * ou se ela já saiu de PENDING_REFERENCE). Deve ser chamada DEPOIS do lock da wallet.
   */
  lockPendingReference(id: string): Promise<StoredTransaction | null>;
}

export interface LedgerRepository {
  insert(entry: WalletLedgerEntry): Promise<void>;
  /** Mais recentes primeiro. `beforeSeq` é o cursor (exclusivo); devolve até `limit` itens. */
  listByWallet(walletId: string, limit: number, beforeSeq?: string): Promise<{ seq: string; entry: WalletLedgerEntry }[]>;
}

export interface ReconciliationSnapshot {
  storedBalance: Money;
  credits: Money;
  debits: Money;
  entries: number;
}

export interface ReconciliationRepository {
  /** Saldo armazenado e somas do ledger lidos em UM statement (mesmo snapshot do banco). */
  snapshot(walletId: string): Promise<ReconciliationSnapshot | null>;
}

export interface OutboxRepository {
  enqueue(message: OutboxMessage): Promise<void>;
  /**
   * Reserva até `limit` mensagens pendentes e vencidas com `FOR UPDATE SKIP LOCKED`: publishers
   * concorrentes nunca disputam a mesma linha. O lock vale até o fim da transação corrente.
   */
  lockDue(now: Date, limit: number): Promise<OutboxMessage[]>;
  /** Persiste `attempts` / `nextAttemptAt` / `publishedAt` de uma mensagem já existente. */
  save(message: OutboxMessage): Promise<void>;
  /** `occurredAt` da mensagem pendente mais antiga (base do outbox lag). */
  oldestPendingOccurredAt(): Promise<Date | null>;
}

/** Porta de saída dos eventos de integração (SQS na implementação real). */
export interface EventPublisher {
  /** Entrega ao menos uma vez; o consumidor deduplica por `message.id` (= eventId). */
  publish(message: OutboxMessage): Promise<void>;
}

/** Resultado de registrar uma mensagem na inbox. */
export interface InboxReceipt {
  /** `true`: primeira vez que (consumer, messageId) é visto (a linha acabou de ser criada). */
  isNew: boolean;
  /** Quando `isNew` é `false`: o que JÁ estava registrado (nunca é sobrescrito). */
  existing?: { payloadHash: string; processedAt?: Date };
}

export interface InboxRepository {
  /**
   * Registra (consumer, messageId) com `INSERT … ON CONFLICT DO NOTHING`. Se a linha já existia, devolve o
   * registro original para o chamador comparar o conteúdo (releitura × messageId reutilizado).
   */
  tryReceive(message: InboxMessage): Promise<InboxReceipt>;
  markProcessed(consumerName: string, messageId: string, at: Date): Promise<void>;
}

export interface Repositories {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  reconciliation: ReconciliationRepository;
  outbox: OutboxRepository;
  inbox: InboxRepository;
}

export interface UnitOfWork {
  /** Executa `fn` em UMA transação SQL; commit ao retornar, rollback ao lançar. */
  run<T>(fn: (repos: Repositories) => Promise<T>): Promise<T>;
}
