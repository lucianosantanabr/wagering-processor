import { FailureCode } from './failure-code';
import { InvalidTransactionStateError, InvalidWagerTransactionError } from './errors';
import { Money } from './money';
import { LedgerDirection } from './wallet-ledger-entry';

export enum WagerTransactionKind {
  Opening = 'OPENING', // interno: crédito de abertura da wallet
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED', // terminal
  Rejected = 'REJECTED', // terminal
  Failed = 'FAILED', // terminal
}

/**
 * Transições válidas:
 *   PENDING           -> PROCESSED | PENDING_REFERENCE | REJECTED | FAILED
 *   PENDING_REFERENCE -> PROCESSED | REJECTED | FAILED
 *   PROCESSED, REJECTED, FAILED: terminais (qualquer transição lança InvalidTransactionStateError)
 */

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<WagerTransactionKind, WagerTransactionKind.Opening>;
  money: Money;
  referenceExternalTransactionId?: string;
  now: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

const TERMINAL: ReadonlySet<WagerTransactionStatus> = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor — não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {}

  /** Nasce em PENDING. OPENING não pode ser criado por aqui (ver {@link opening}). */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if ((props.kind as WagerTransactionKind) === WagerTransactionKind.Opening) {
      throw new InvalidWagerTransactionError('OPENING is internal and cannot be submitted');
    }
    const requiresReference =
      props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback;
    if (requiresReference && !props.referenceExternalTransactionId) {
      throw new InvalidWagerTransactionError(`${props.kind} requires referenceExternalTransactionId`);
    }
    if (props.kind === WagerTransactionKind.Bet && props.referenceExternalTransactionId) {
      throw new InvalidWagerTransactionError('BET must not carry a reference');
    }
    if (props.money.isNegative()) {
      throw new InvalidWagerTransactionError('amount must not be negative');
    }
    if (props.kind !== WagerTransactionKind.Loss && !props.money.isPositive()) {
      throw new InvalidWagerTransactionError(`${props.kind} amount must be positive`);
    }
    return WagerTransaction.fromProps(props, props.kind);
  }

  /** Transação interna de abertura de wallet. Chave de idempotência derivada do walletId. */
  static opening(props: {
    id: string;
    payloadHash: string;
    walletId: string;
    playerId: string;
    money: Money;
    now: Date;
  }): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvalidWagerTransactionError('OPENING amount must be positive');
    }
    return WagerTransaction.fromProps(
      {
        id: props.id,
        providerId: 'internal',
        externalTransactionId: `opening:${props.walletId}`,
        idempotencyKey: `internal:opening:${props.walletId}`,
        payloadHash: props.payloadHash,
        walletId: props.walletId,
        playerId: props.playerId,
        roundId: 'opening',
        gameId: 'opening',
        money: props.money,
        now: props.now,
      },
      WagerTransactionKind.Opening,
    );
  }

  /** Reconstrução a partir da persistência — não revalida regras de transição. */
  static rehydrate(s: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      s.id,
      s.providerId,
      s.externalTransactionId,
      s.idempotencyKey,
      s.payloadHash,
      s.walletId,
      s.playerId,
      s.roundId,
      s.gameId,
      s.kind,
      s.money,
      s.referenceExternalTransactionId,
      s.createdAt,
      s.status,
      s.referenceTransactionId,
      s.failureCode,
      s.processedAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  // ---- transições (lançam InvalidTransactionStateError se o estado atual for terminal)

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertNotTerminal(WagerTransactionStatus.Processed);
    if (this.requiresReference() && !referenceTransactionId) {
      throw new InvalidWagerTransactionError(`${this.kind} cannot be processed without a resolved reference`);
    }
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  /** Só a partir de PENDING: o reprocessamento de quem já está em PENDING_REFERENCE não muda o estado. */
  markPendingReference(): void {
    this.assertNotTerminal(WagerTransactionStatus.PendingReference);
    if (!this.requiresReference()) {
      throw new InvalidWagerTransactionError(`${this.kind} does not require a reference`);
    }
    if (this._status !== WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(
        `cannot move ${this._status} -> ${WagerTransactionStatus.PendingReference}`,
      );
    }
    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    this.assertNotTerminal(WagerTransactionStatus.Rejected);
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  fail(code: FailureCode): void {
    this.assertNotTerminal(WagerTransactionStatus.Failed);
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  // ---- consultas de domínio

  isTerminal(): boolean {
    return TERMINAL.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /**
   * Direção do lançamento que esta transação produz. ROLLBACK é o inverso do
   * lançamento da referência (que deve ser BET, WIN ou REFUND).
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Loss:
        throw new InvalidWagerTransactionError('LOSS does not affect balance');
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidWagerTransactionError('ROLLBACK requires its reference to resolve direction');
        }
        if (!reference.affectsBalance() || reference.kind === WagerTransactionKind.Rollback) {
          throw new InvalidWagerTransactionError(`ROLLBACK cannot reference ${reference.kind}`);
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      }
    }
  }

  private assertNotTerminal(target: WagerTransactionStatus): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`cannot move ${this._status} -> ${target}: status is terminal`);
    }
  }

  private static fromProps(
    p: Omit<CreateWagerTransactionProps, 'kind'>,
    kind: WagerTransactionKind,
  ): WagerTransaction {
    return new WagerTransaction(
      p.id,
      p.providerId,
      p.externalTransactionId,
      p.idempotencyKey,
      p.payloadHash,
      p.walletId,
      p.playerId,
      p.roundId,
      p.gameId,
      kind,
      p.money,
      p.referenceExternalTransactionId,
      p.now,
      WagerTransactionStatus.Pending,
    );
  }
}
