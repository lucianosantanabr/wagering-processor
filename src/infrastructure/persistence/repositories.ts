import { LockMode, UniqueConstraintViolationException } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import { DuplicateKeyError } from '@application/errors';
import type {
  InboxReceipt,
  InboxRepository,
  LedgerRepository,
  OutboxRepository,
  ReconciliationRepository,
  ReconciliationSnapshot,
  ReferenceRetry,
  Repositories,
  StoredTransaction,
  WagerTransactionRepository,
  WalletRepository,
} from '@application/ports';
import type { InboxMessage } from '@domain/inbox-message';
import { Money } from '@domain/money';
import { OutboxMessage } from '@domain/outbox-message';
import {
  WagerTransaction,
  type WagerTransactionKind,
  type WagerTransactionStatus,
} from '@domain/wager-transaction';
import { Wallet } from '@domain/wallet';
import { type LedgerDirection, WalletLedgerEntry } from '@domain/wallet-ledger-entry';
import type { FailureCode } from '@domain/failure-code';
import {
  InboxMessageRow,
  LedgerEntryRow,
  OutboxMessageRow,
  WagerTransactionRow,
  WalletRow,
} from './entities';

/** Converte violação de unicidade do driver em erro de aplicação, preservando o nome da constraint. */
async function guardUnique<T>(op: () => Promise<T>): Promise<T> {
  try {
    return await op();
  } catch (e) {
    if (e instanceof UniqueConstraintViolationException) {
      const constraint = (e as { constraint?: string }).constraint ?? e.message;
      throw new DuplicateKeyError(constraint, e.message);
    }
    throw e;
  }
}

// ───────────────────────────── mappers ─────────────────────────────

const money = (amount: string, currency: string) => Money.from({ amount, currency });

function walletToDomain(r: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency,
    balance: money(r.balance, r.currency),
    version: r.version,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  });
}

function txToStored(r: WagerTransactionRow): StoredTransaction {
  return {
    tx: WagerTransaction.rehydrate({
      id: r.id,
      providerId: r.providerId,
      externalTransactionId: r.externalTransactionId,
      idempotencyKey: r.idempotencyKey,
      payloadHash: r.payloadHash,
      walletId: r.walletId,
      playerId: r.playerId,
      roundId: r.roundId,
      gameId: r.gameId,
      kind: r.kind as WagerTransactionKind,
      money: money(r.amount, r.currency),
      referenceExternalTransactionId: r.referenceExternalTransactionId ?? undefined,
      createdAt: r.createdAt,
      status: r.status as WagerTransactionStatus,
      referenceTransactionId: r.referenceTransactionId ?? undefined,
      failureCode: (r.failureCode ?? undefined) as FailureCode | undefined,
      processedAt: r.processedAt ?? undefined,
    }),
    balanceSnapshot: money(r.balanceSnapshot, r.currency),
    referenceAttempts: r.referenceAttempts,
  };
}

// ─────────────────────────── repositórios ──────────────────────────

class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string) {
    const row = await this.em.findOne(WalletRow, { id });
    return row ? walletToDomain(row) : null;
  }

  async findByIdForUpdate(id: string) {
    // SELECT … FOR UPDATE: lock de linha por wallet (nunca global).
    const row = await this.em.findOne(WalletRow, { id }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    return row ? walletToDomain(row) : null;
  }

  async findByPlayerAndCurrency(playerId: string, currency: string) {
    const row = await this.em.findOne(WalletRow, { playerId, currency });
    return row ? walletToDomain(row) : null;
  }

  async insert(wallet: Wallet) {
    await guardUnique(() =>
      this.em.insert(WalletRow, {
        id: wallet.id,
        playerId: wallet.playerId,
        currency: wallet.currency,
        balance: wallet.balance.toString(),
        version: wallet.version,
        createdAt: wallet.createdAt,
        updatedAt: wallet.updatedAt,
      }),
    );
  }

  async saveBalance(wallet: Wallet, expectedVersion: number) {
    // Defesa em profundidade além do lock: UPDATE condicionado à versão lida.
    const affected = await this.em.nativeUpdate(
      WalletRow,
      { id: wallet.id, version: expectedVersion },
      { balance: wallet.balance.toString(), version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) {
      throw new Error(`lost update detected on wallet ${wallet.id} (expected version ${expectedVersion})`);
    }
  }
}

class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  private async one(where: Record<string, unknown>) {
    const row = await this.em.findOne(WagerTransactionRow, where);
    return row ? txToStored(row) : null;
  }

  findById(id: string) {
    return this.one({ id });
  }

  findByIdempotencyKey(idempotencyKey: string) {
    return this.one({ idempotencyKey });
  }

  findByProviderExternalId(providerId: string, externalTransactionId: string) {
    return this.one({ providerId, externalTransactionId });
  }

  async findProcessedReversalOf(referenceTransactionId: string) {
    const row = await this.em.findOne(WagerTransactionRow, {
      referenceTransactionId,
      status: 'PROCESSED',
      kind: { $in: ['REFUND', 'ROLLBACK'] },
    });
    return row ? txToStored(row).tx : null;
  }

  async insert(tx: WagerTransaction, balanceSnapshot: Money, retry?: ReferenceRetry) {
    await guardUnique(() =>
      this.em.insert(WagerTransactionRow, {
        id: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        idempotencyKey: tx.idempotencyKey,
        payloadHash: tx.payloadHash,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        amount: tx.money.toString(),
        currency: tx.money.currency,
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
        referenceTransactionId: tx.referenceTransactionId ?? null,
        status: tx.status,
        failureCode: tx.failureCode ?? null,
        balanceSnapshot: balanceSnapshot.toString(),
        referenceAttempts: retry?.attempts ?? 0,
        nextAttemptAt: retry?.nextAttemptAt ?? null,
        createdAt: tx.createdAt,
        processedAt: tx.processedAt ?? null,
      }),
    );
  }

  async updateState(tx: WagerTransaction, balanceSnapshot: Money, retry?: ReferenceRetry) {
    await guardUnique(() =>
      this.em.nativeUpdate(
        WagerTransactionRow,
        { id: tx.id },
        {
          status: tx.status,
          failureCode: tx.failureCode ?? null,
          referenceTransactionId: tx.referenceTransactionId ?? null,
          balanceSnapshot: balanceSnapshot.toString(),
          processedAt: tx.processedAt ?? null,
          ...(retry ? { referenceAttempts: retry.attempts, nextAttemptAt: retry.nextAttemptAt } : {}),
        },
      ),
    );
  }

  async findDuePendingReference(now: Date, limit: number) {
    const rows = (await this.em.execute(
      `SELECT id FROM wager_transactions
        WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= ?
        ORDER BY next_attempt_at
        LIMIT ?`,
      [now, limit],
    )) as { id: string }[];
    return rows.map((r) => r.id);
  }

  async lockPendingReference(id: string) {
    // FOR UPDATE SKIP LOCKED: se outro worker já a tem, devolve null em vez de esperar.
    const row = await this.em.findOne(
      WagerTransactionRow,
      { id, status: 'PENDING_REFERENCE' },
      { lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },
    );
    return row ? txToStored(row) : null;
  }
}

class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly em: EntityManager) {}

  async insert(e: WalletLedgerEntry) {
    await guardUnique(() =>
      this.em.insert(LedgerEntryRow, {
        id: e.id,
        walletId: e.walletId,
        transactionId: e.transactionId,
        direction: e.direction,
        amount: e.money.toString(),
        currency: e.money.currency,
        balanceBefore: e.balanceBefore.toString(),
        balanceAfter: e.balanceAfter.toString(),
        createdAt: e.createdAt,
      }),
    );
  }

  async listByWallet(walletId: string, limit: number, beforeSeq?: string) {
    const rows = (await this.em.execute(
      `SELECT id, seq::text AS seq, wallet_id, transaction_id, direction, amount::text AS amount, currency,
              balance_before::text AS balance_before, balance_after::text AS balance_after, created_at
         FROM wallet_ledger_entries
        WHERE wallet_id = ? ${beforeSeq ? 'AND seq < ?::bigint' : ''}
        ORDER BY seq DESC LIMIT ?`,
      beforeSeq ? [walletId, beforeSeq, limit] : [walletId, limit],
    )) as {
      id: string; seq: string; wallet_id: string; transaction_id: string; direction: string; amount: string;
      currency: string; balance_before: string; balance_after: string; created_at: string | Date;
    }[];
    return rows.map((r) => ({
      seq: r.seq,
      entry: WalletLedgerEntry.rehydrate({
        id: r.id,
        walletId: r.wallet_id,
        transactionId: r.transaction_id,
        direction: r.direction as LedgerDirection,
        money: money(r.amount, r.currency),
        balanceBefore: money(r.balance_before, r.currency),
        balanceAfter: money(r.balance_after, r.currency),
        // SQL cru não passa pelo mapeamento de tipos do ORM: a data chega como string/Date.
        createdAt: new Date(r.created_at),
      }),
    }));
  }
}

class SqlReconciliationRepository implements ReconciliationRepository {
  constructor(private readonly em: EntityManager) {}

  async snapshot(walletId: string): Promise<ReconciliationSnapshot | null> {
    // Um único statement => um único snapshot: saldo e somas nunca divergem por corrida.
    const rows = (await this.em.execute(
      `SELECT w.balance::text AS balance, w.currency,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'CREDIT'), 0)::text AS credits,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'DEBIT'), 0)::text AS debits,
              COUNT(l.id)::text AS entries
         FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
        WHERE w.id = ? GROUP BY w.id`,
      [walletId],
    )) as { balance: string; currency: string; credits: string; debits: string; entries: string }[];
    const r = rows[0];
    if (!r) return null;
    return {
      storedBalance: money(r.balance, r.currency),
      credits: money(r.credits, r.currency),
      debits: money(r.debits, r.currency),
      entries: Number(r.entries),
    };
  }
}

class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  async enqueue(m: OutboxMessage) {
    await this.em.insert(OutboxMessageRow, {
      id: m.id,
      aggregateId: m.aggregateId,
      eventType: m.eventType,
      payload: m.payload as Record<string, unknown>,
      occurredAt: m.occurredAt,
      attempts: m.attempts,
      nextAttemptAt: m.nextAttemptAt ?? null,
      publishedAt: m.publishedAt ?? null,
    });
  }

  async lockDue(now: Date, limit: number) {
    // SKIP LOCKED: cada publisher reserva um conjunto disjunto; ninguém espera por ninguém.
    const rows = (await this.em.execute(
      `SELECT id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at
         FROM outbox_messages
        WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY seq
        LIMIT ?
        FOR UPDATE SKIP LOCKED`,
      [now, limit],
    )) as {
      id: string; aggregate_id: string; event_type: string; payload: Record<string, unknown>;
      occurred_at: string | Date; attempts: number; next_attempt_at: string | Date | null; published_at: string | Date | null;
    }[];
    return rows.map((r) =>
      OutboxMessage.rehydrate({
        id: r.id,
        aggregateId: r.aggregate_id,
        eventType: r.event_type,
        payload: r.payload,
        occurredAt: new Date(r.occurred_at),
        attempts: r.attempts,
        nextAttemptAt: r.next_attempt_at ? new Date(r.next_attempt_at) : undefined,
        publishedAt: r.published_at ? new Date(r.published_at) : undefined,
      }),
    );
  }

  async save(m: OutboxMessage) {
    await this.em.nativeUpdate(
      OutboxMessageRow,
      { id: m.id },
      { attempts: m.attempts, nextAttemptAt: m.nextAttemptAt ?? null, publishedAt: m.publishedAt ?? null },
    );
  }

  async oldestPendingOccurredAt() {
    const rows = (await this.em.execute(
      `SELECT occurred_at FROM outbox_messages WHERE published_at IS NULL ORDER BY seq LIMIT 1`,
    )) as { occurred_at: string | Date }[];
    return rows[0] ? new Date(rows[0].occurred_at) : null;
  }
}

class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async tryReceive(m: InboxMessage): Promise<InboxReceipt> {
    // Com outra transação inserindo a mesma chave, este INSERT ESPERA o desfecho dela: se comitar, não insere
    // (conflito); se der rollback, insere. Por isso a linha lida abaixo é sempre a comitada.
    const inserted = await this.em.execute(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
       VALUES (?, ?, ?, ?) ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING message_id`,
      [m.consumerName, m.messageId, m.payloadHash, m.receivedAt],
    );
    if ((inserted as unknown[]).length === 1) return { isNew: true };

    const rows = (await this.em.execute(
      `SELECT payload_hash, processed_at FROM inbox_messages WHERE consumer_name = ? AND message_id = ?`,
      [m.consumerName, m.messageId],
    )) as { payload_hash: string; processed_at: string | Date | null }[];
    const row = rows[0];
    if (!row) throw new Error(`inbox row ${m.consumerName}/${m.messageId} disappeared after conflict`); // linhas nunca são apagadas
    return {
      isNew: false,
      existing: { payloadHash: row.payload_hash, processedAt: row.processed_at ? new Date(row.processed_at) : undefined },
    };
  }

  async markProcessed(consumerName: string, messageId: string, at: Date) {
    await this.em.nativeUpdate(InboxMessageRow, { consumerName, messageId }, { processedAt: at });
  }
}

export function buildRepositories(em: EntityManager): Repositories {
  return {
    wallets: new MikroOrmWalletRepository(em),
    transactions: new MikroOrmWagerTransactionRepository(em),
    ledger: new MikroOrmLedgerRepository(em),
    reconciliation: new SqlReconciliationRepository(em),
    outbox: new MikroOrmOutboxRepository(em),
    inbox: new MikroOrmInboxRepository(em),
  };
}
