import { describe, expect, test } from 'bun:test';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidLedgerEntryError,
  InvalidWalletError,
} from '@domain/errors';
import { Money } from '@domain/money';
import { LedgerDirection, WalletLedgerEntry } from '@domain/wallet-ledger-entry';
import { Wallet } from '@domain/wallet';
import { NOW, brl, openWallet } from './helpers';

const move = (amount: string, n = 1) => ({
  entryId: `e-${n}`,
  transactionId: `t-${n}`,
  money: brl(amount),
  at: NOW,
});

describe('Wallet', () => {
  test('abre com version 1 e saldo inicial', () => {
    const w = openWallet('1000.00');
    expect(w.version).toBe(1);
    expect(w.balance.toString()).toBe('1000.00');
  });

  test('não abre com saldo negativo', () => {
    expect(() =>
      Wallet.open({
        id: 'w',
        playerId: 'p',
        initialBalance: Money.zero('BRL').subtract(brl('1.00')),
        now: NOW,
      }),
    ).toThrow(InvalidWalletError);
  });

  test('debit reduz saldo, incrementa version e devolve lançamento DEBIT balanceado', () => {
    const w = openWallet('100.00');
    const e = w.debit(move('30.00'));
    expect(w.balance.toString()).toBe('70.00');
    expect(w.version).toBe(2);
    expect(e.direction).toBe(LedgerDirection.Debit);
    expect(e.balanceBefore.toString()).toBe('100.00');
    expect(e.balanceAfter.toString()).toBe('70.00');
    expect(e.isBalanced()).toBe(true);
  });

  test('credit aumenta saldo e devolve lançamento CREDIT', () => {
    const w = openWallet('10.00');
    const e = w.credit(move('5.50'));
    expect(w.balance.toString()).toBe('15.50');
    expect(e.direction).toBe(LedgerDirection.Credit);
  });

  test('saldo nunca fica negativo: débito acima do saldo falha e nada muda', () => {
    const w = openWallet('100.00');
    expect(() => w.debit(move('100.01'))).toThrow(InsufficientFundsError);
    expect(w.balance.toString()).toBe('100.00');
    expect(w.version).toBe(1);
  });

  test('debitar exatamente o saldo é permitido', () => {
    const w = openWallet('100.00');
    w.debit(move('100.00'));
    expect(w.balance.isZero()).toBe(true);
  });

  test('moeda diferente é rejeitada', () => {
    const w = openWallet('100.00', 'BRL');
    const usd = { ...move('1.00'), money: Money.from({ amount: '1.00', currency: 'USD' }) };
    expect(() => w.debit(usd)).toThrow(CurrencyMismatchError);
    expect(() => w.credit(usd)).toThrow(CurrencyMismatchError);
    expect(w.version).toBe(1);
  });

  test('valor zero não move saldo nem incrementa version', () => {
    const w = openWallet('100.00');
    expect(() => w.credit(move('0.00'))).toThrow(InvalidWalletError);
    expect(w.version).toBe(1);
  });

  test('openingEntry gera CREDIT de abertura a partir de zero', () => {
    const w = openWallet('1000.00');
    const e = w.openingEntry({ entryId: 'e0', transactionId: 't0' });
    expect(e.direction).toBe(LedgerDirection.Credit);
    expect(e.balanceBefore.isZero()).toBe(true);
    expect(e.balanceAfter.toString()).toBe('1000.00');
  });

  test('openingEntry não vale para wallet movimentada ou com saldo zero', () => {
    const moved = openWallet('100.00');
    moved.debit(move('1.00'));
    expect(() => moved.openingEntry({ entryId: 'e', transactionId: 't' })).toThrow(InvalidWalletError);
    expect(() => openWallet('0.00').openingEntry({ entryId: 'e', transactionId: 't' })).toThrow(
      InvalidWalletError,
    );
  });

  test('rehydrate reconstrói o estado sem revalidar', () => {
    const w = Wallet.rehydrate({
      id: 'w',
      playerId: 'p',
      currency: 'BRL',
      balance: brl('42.00'),
      version: 7,
      createdAt: NOW,
      updatedAt: NOW,
    });
    expect(w.version).toBe(7);
    expect(w.balance.toString()).toBe('42.00');
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    id: 'e',
    walletId: 'w',
    transactionId: 't',
    createdAt: NOW,
  };

  test('create valida a aritmética', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('100.00'),
        balanceAfter: brl('80.00'),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  test('rejeita valor não positivo e saldos negativos', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Credit,
        money: brl('0.00'),
        balanceBefore: brl('1.00'),
        balanceAfter: brl('1.00'),
      }),
    ).toThrow(InvalidLedgerEntryError);
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('5.00'),
        balanceAfter: brl('5.00').subtract(brl('5.00')).subtract(brl('5.00')),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  test('é estruturalmente imutável: sem métodos de transição', () => {
    const e = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      money: brl('10.00'),
      balanceBefore: brl('0.00'),
      balanceAfter: brl('10.00'),
    });
    const proto = Object.getOwnPropertyNames(Object.getPrototypeOf(e));
    expect(proto.sort()).toEqual(['constructor', 'isBalanced']);
  });
});
