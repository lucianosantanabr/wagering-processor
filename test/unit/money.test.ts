import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '@domain/errors';
import { Money } from '@domain/money';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money', () => {
  test('serializa sempre com escala 2', () => {
    expect(brl('25').toString()).toBe('25.00');
    expect(brl('25.5').toString()).toBe('25.50');
    expect(brl('25.00').toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('aritmética decimal exata (sem erro de ponto flutuante)', () => {
    expect(brl('0.10').add(brl('0.20')).toString()).toBe('0.30');
    expect(brl('100.00').subtract(brl('80.00')).toString()).toBe('20.00');
  });

  test('imutável: operações retornam novas instâncias', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.toString()).toBe('10.00');
    expect(b.toString()).toBe('15.00');
  });

  test('subtract e negate podem produzir valores negativos internamente', () => {
    const r = brl('10.00').subtract(brl('25.00'));
    expect(r.isNegative()).toBe(true);
    expect(r.toString()).toBe('-15.00');
    expect(brl('5.00').negate().isNegative()).toBe(true);
    expect(Money.zero('BRL').negate().isNegative()).toBe(false);
  });

  test('predicados', () => {
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('1.00').isLessThan(brl('2.00'))).toBe(true);
    expect(brl('2.00').isLessThan(brl('2.00'))).toBe(false);
    expect(brl('2.0').equals(brl('2.00'))).toBe(true);
  });

  test.each(['', ' ', 'NaN', 'Infinity', '-Infinity', '1e3', '1E3', '0x10', '-1.00', '+1.00', '1,00', '.5', '5.', '1.234', '1.005', 'abc'])(
    'rejeita amount inválido: "%s"',
    (amount) => {
      expect(() => brl(amount)).toThrow(InvalidMoneyError);
    },
  );

  test('rejeita amount que não é string (ex.: number)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each(['', 'brl', 'BR', 'BRLL', '123'])('rejeita currency inválida: "%s"', (currency) => {
    expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
    expect(() => Money.zero(currency)).toThrow(InvalidMoneyError);
  });

  test('conflito de moeda lança erro de domínio em todas as operações binárias', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    const real = brl('1.00');
    expect(() => real.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => real.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => real.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(real.equals(usd)).toBe(false);
  });
});
