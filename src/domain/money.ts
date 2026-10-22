import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './errors';

export interface MoneyProps {
  /** decimal string, ex.: "25.00" */
  amount: string;
  /** ISO-4217 */
  currency: string;
}

const SCALE = 2;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;
// Sem sinal, sem notação científica, no máximo 2 casas decimais.
const INPUT_AMOUNT_PATTERN = /^\d+(\.\d{1,2})?$/;

/** Imutável: toda operação retorna uma nova instância. */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  /**
   * Contrato de entrada: rejeita negativos, NaN, Infinity, notação científica,
   * string vazia e mais de 2 casas decimais. Valores com menos casas são
   * normalizados para escala 2 ("25" -> "25.00").
   */
  static from(props: MoneyProps): Money {
    Money.assertCurrency(props.currency);
    if (typeof props.amount !== 'string' || !INPUT_AMOUNT_PATTERN.test(props.amount)) {
      throw new InvalidMoneyError(`invalid amount: "${String(props.amount)}"`);
    }
    return new Money(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(new Decimal(0), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  /** Pode resultar em valor negativo; quem chama decide se isso é aceitável. */
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.isNegative() && !this.value.isZero();
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    return this.value.toFixed(SCALE);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(
        `currency mismatch: ${this.currency} vs ${other.currency}`,
      );
    }
  }

  private static assertCurrency(currency: string): void {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`invalid currency: "${String(currency)}"`);
    }
  }
}
