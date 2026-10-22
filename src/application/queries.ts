import type { MoneyProps } from '@domain/money';
import type { WalletLedgerEntry } from '@domain/wallet-ledger-entry';
import type { WagerTransaction } from '@domain/wager-transaction';
import { TransactionNotFoundError, ValidationError, WalletNotFoundError } from './errors';
import { type Logger, type Metrics, noopLogger, noopMetrics } from './observability';
import type { StoredTransaction, UnitOfWork } from './ports';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function assertUuid(value: string, field: string): void {
  if (!UUID.test(value ?? '')) throw new ValidationError(`${field} must be a UUID`);
}

// ───────────────────────────── wallet ─────────────────────────────

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export class GetWallet {
  constructor(private readonly uow: UnitOfWork) {}

  async execute(walletId: string): Promise<WalletView> {
    assertUuid(walletId, 'walletId');
    const wallet = await this.uow.run((r) => r.wallets.findById(walletId));
    if (!wallet) throw new WalletNotFoundError(`wallet ${walletId} not found`);
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
      createdAt: wallet.createdAt.toISOString(),
      updatedAt: wallet.updatedAt.toISOString(),
    };
  }
}

// ───────────────────────────── ledger ─────────────────────────────

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerPage {
  items: LedgerEntryView[];
  /** Cursor opaco e estável (baseado na `seq` do ledger); `null` na última página. */
  nextCursor: string | null;
}

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

export const encodeCursor = (seq: string): string => Buffer.from(JSON.stringify({ s: seq })).toString('base64url');

export function decodeCursor(cursor: string): string {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { s?: unknown };
    if (typeof parsed.s === 'string' && /^\d{1,19}$/.test(parsed.s)) return parsed.s;
  } catch {
    /* cai no erro abaixo */
  }
  throw new ValidationError('invalid cursor');
}

function toView(e: WalletLedgerEntry): LedgerEntryView {
  return {
    id: e.id,
    transactionId: e.transactionId,
    direction: e.direction,
    money: e.money.toJSON(),
    balanceBefore: e.balanceBefore.toJSON(),
    balanceAfter: e.balanceAfter.toJSON(),
    createdAt: e.createdAt.toISOString(),
  };
}

export class GetLedger {
  constructor(private readonly uow: UnitOfWork) {}

  async execute(input: { walletId: string; cursor?: string; limit?: string | number }): Promise<LedgerPage> {
    assertUuid(input.walletId, 'walletId');
    const limit = input.limit === undefined || input.limit === '' ? DEFAULT_LIMIT : Number(input.limit);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
      throw new ValidationError(`limit must be an integer between 1 and ${MAX_LIMIT}`);
    }
    const before = input.cursor ? decodeCursor(input.cursor) : undefined;

    return this.uow.run(async (r) => {
      if (!(await r.wallets.findById(input.walletId))) {
        throw new WalletNotFoundError(`wallet ${input.walletId} not found`);
      }
      const rows = await r.ledger.listByWallet(input.walletId, limit + 1, before);
      const page = rows.slice(0, limit);
      const last = page[page.length - 1];
      return {
        items: page.map((row) => toView(row.entry)),
        nextCursor: rows.length > limit && last ? encodeCursor(last.seq) : null,
      };
    });
  }
}

// ─────────────────────────── transações ───────────────────────────

export interface TransactionView {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  status: string;
  failureCode?: string;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  /** Saldo da wallet observado quando a transação chegou ao estado atual. */
  balance: MoneyProps;
  createdAt: string;
  processedAt?: string;
}

function txView({ tx, balanceSnapshot }: StoredTransaction): TransactionView {
  const t: WagerTransaction = tx;
  return {
    transactionId: t.id,
    providerId: t.providerId,
    externalTransactionId: t.externalTransactionId,
    walletId: t.walletId,
    playerId: t.playerId,
    roundId: t.roundId,
    gameId: t.gameId,
    kind: t.kind,
    money: t.money.toJSON(),
    status: t.status,
    ...(t.failureCode ? { failureCode: t.failureCode } : {}),
    ...(t.referenceExternalTransactionId ? { referenceExternalTransactionId: t.referenceExternalTransactionId } : {}),
    ...(t.referenceTransactionId ? { referenceTransactionId: t.referenceTransactionId } : {}),
    balance: balanceSnapshot.toJSON(),
    createdAt: t.createdAt.toISOString(),
    ...(t.processedAt ? { processedAt: t.processedAt.toISOString() } : {}),
  };
}

export class GetTransaction {
  constructor(private readonly uow: UnitOfWork) {}

  async byId(transactionId: string): Promise<TransactionView> {
    assertUuid(transactionId, 'transactionId');
    const found = await this.uow.run((r) => r.transactions.findById(transactionId));
    if (!found) throw new TransactionNotFoundError(`transaction ${transactionId} not found`);
    return txView(found);
  }

  async byProviderExternalId(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    const found = await this.uow.run((r) => r.transactions.findByProviderExternalId(providerId, externalTransactionId));
    if (!found) {
      throw new TransactionNotFoundError(`transaction ${providerId}/${externalTransactionId} not found`);
    }
    return txView(found);
  }
}

// ───────────────────────────── reconciliação ─────────────────────────────

export interface ReconciliationReport {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

/**
 * Compara o saldo materializado com o saldo reconstruído pelo ledger. Divergências NÃO são
 * corrigidas: são logadas, contadas em métrica e sinalizadas na resposta.
 */
export class ReconcileWallet {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    assertUuid(walletId, 'walletId');
    const snap = await this.uow.run((r) => r.reconciliation.snapshot(walletId));
    if (!snap) throw new WalletNotFoundError(`wallet ${walletId} not found`);

    const calculated = snap.credits.subtract(snap.debits);
    const difference = snap.storedBalance.subtract(calculated);
    const consistent = difference.isZero();

    this.metrics.increment('reconciliation_runs_total', { result: consistent ? 'consistent' : 'divergent' });
    if (!consistent) {
      this.metrics.increment('reconciliation_divergences_total');
      this.logger.error('wallet reconciliation divergence', {
        walletId,
        difference: difference.toString(),
        checkedEntries: snap.entries,
      });
    }

    return {
      walletId,
      storedBalance: snap.storedBalance.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: snap.entries,
    };
  }
}
