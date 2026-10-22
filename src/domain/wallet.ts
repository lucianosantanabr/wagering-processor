import { CurrencyMismatchError, InsufficientFundsError, InvalidWalletError } from './errors';
import { Money } from './money';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovement {
  /** id do lançamento de ledger que será produzido */
  entryId: string;
  transactionId: string;
  money: Money;
  at: Date;
}

/**
 * Aggregate root. Saldo e ledger andam juntos: toda mutação de saldo devolve o
 * lançamento correspondente. `version` só incrementa quando o saldo muda.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * O saldo inicial já nasce na wallet (version 1); o lançamento de abertura
   * correspondente é obtido com {@link openingEntry} e gravado na mesma transação SQL.
   */
  static open(props: { id: string; playerId: string; initialBalance: Money; now: Date }): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new InvalidWalletError('initial balance must not be negative');
    }
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      props.now,
      props.now,
    );
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  /** Lançamento CREDIT de abertura. Só vale para uma wallet recém-aberta com saldo > 0. */
  openingEntry(props: { entryId: string; transactionId: string }): WalletLedgerEntry {
    if (this._version !== 1 || !this._balance.isPositive()) {
      throw new InvalidWalletError('opening entry requires a freshly opened wallet with positive balance');
    }
    return WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction: LedgerDirection.Credit,
      money: this._balance,
      balanceBefore: Money.zero(this.currency),
      balanceAfter: this._balance,
      createdAt: this.createdAt,
    });
  }

  debit(movement: WalletMovement): WalletLedgerEntry {
    this.assertMovement(movement.money);
    const after = this._balance.subtract(movement.money);
    if (after.isNegative()) {
      throw new InsufficientFundsError(
        `insufficient funds: balance ${this._balance}, debit ${movement.money}`,
      );
    }
    return this.apply(LedgerDirection.Debit, movement, after);
  }

  credit(movement: WalletMovement): WalletLedgerEntry {
    this.assertMovement(movement.money);
    return this.apply(LedgerDirection.Credit, movement, this._balance.add(movement.money));
  }

  private apply(direction: LedgerDirection, m: WalletMovement, after: Money): WalletLedgerEntry {
    const entry = WalletLedgerEntry.create({
      id: m.entryId,
      walletId: this.id,
      transactionId: m.transactionId,
      direction,
      money: m.money,
      balanceBefore: this._balance,
      balanceAfter: after,
      createdAt: m.at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = m.at;
    return entry;
  }

  private assertMovement(money: Money): void {
    this.assertSameCurrency(money);
    if (!money.isPositive()) {
      throw new InvalidWalletError('movement amount must be positive');
    }
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(
        `wallet currency ${this.currency} differs from operation currency ${money.currency}`,
      );
    }
  }
}
