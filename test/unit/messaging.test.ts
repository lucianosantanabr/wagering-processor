import { describe, expect, test } from 'bun:test';
import { exponentialBackoffMs } from '@domain/backoff';
import { InvalidInboxStateError, InvalidOutboxStateError } from '@domain/errors';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '@domain/events';
import { InboxMessage } from '@domain/inbox-message';
import { OutboxMessage } from '@domain/outbox-message';
import { FailureCode } from '@domain/failure-code';
import { WagerTransactionKind as K } from '@domain/wager-transaction';
import { NOW, brl, makeTx, openWallet } from './helpers';

const ctx = { eventId: 'ev-1', correlationId: 'corr-1', causationId: 'cause-1', occurredAt: NOW };

describe('InboxMessage', () => {
  test('receive -> markProcessed uma única vez', () => {
    const m = InboxMessage.receive({ messageId: 'm1', consumerName: 'c', payloadHash: 'h', now: NOW });
    expect(m.isProcessed()).toBe(false);
    m.markProcessed(NOW);
    expect(m.isProcessed()).toBe(true);
    expect(() => m.markProcessed(NOW)).toThrow(InvalidInboxStateError);
  });
});

describe('backoff', () => {
  test('cresce exponencialmente e respeita o teto', () => {
    expect(exponentialBackoffMs(0, 1000, 60_000)).toBe(1000);
    expect(exponentialBackoffMs(3, 1000, 60_000)).toBe(8000);
    expect(exponentialBackoffMs(10, 1000, 60_000)).toBe(60_000);
    expect(exponentialBackoffMs(10_000, 1000, 60_000)).toBe(60_000);
  });
});

describe('Integration events + OutboxMessage', () => {
  test('WalletBalanceChanged serializa MoneyProps (strings), nunca Money', () => {
    const w = openWallet('100.00');
    const entry = w.debit({ entryId: 'e1', transactionId: 't1', money: brl('25.00'), at: NOW });
    const ev = WalletBalanceChanged.from(w, entry, ctx);
    const json = ev.toJSON();
    expect(json.eventType).toBe('WalletBalanceChanged');
    expect(json.version).toBe(1);
    expect(json.occurredAt).toBe(NOW.toISOString());
    expect(json.causationId).toBe('cause-1');
    expect(json.data).toMatchObject({
      walletId: 'wallet-1',
      direction: 'DEBIT',
      money: { amount: '25.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '75.00', currency: 'BRL' },
      walletVersion: 2,
    });
    expect(JSON.parse(JSON.stringify(ev))).toEqual(json);
  });

  test('eventos de transação carregam os dados esperados', () => {
    const tx = makeTx({ kind: K.Refund, referenceExternalTransactionId: 'ref-1' });
    tx.markPendingReference();
    expect(WagerTransactionPendingReference.from(tx, ctx).toJSON().data.referenceExternalTransactionId).toBe('ref-1');

    tx.reject(FailureCode.ReferenceNotFound);
    const rejected = WagerTransactionRejected.from(tx, ctx).toJSON();
    expect(rejected.eventType).toBe('WagerTransactionRejected');
    expect(rejected.data.failureCode).toBe('REFERENCE_NOT_FOUND');

    const ok = makeTx({ kind: K.Loss });
    ok.markProcessed(undefined, NOW);
    expect(WagerTransactionProcessed.from(ok, ctx).toJSON().data.kind).toBe('LOSS');
  });

  test('data do evento é congelado', () => {
    const ok = makeTx();
    ok.markProcessed(undefined, NOW);
    const ev = WagerTransactionProcessed.from(ok, ctx);
    expect(Object.isFrozen(ev.data)).toBe(true);
  });

  test('enqueue usa eventId como id e guarda o envelope', () => {
    const ok = makeTx();
    ok.markProcessed(undefined, NOW);
    const ev = WagerTransactionProcessed.from(ok, ctx);
    const m = OutboxMessage.enqueue(ev);
    expect(m.id).toBe('ev-1');
    expect(m.eventType).toBe('WagerTransactionProcessed');
    expect(m.payload).toEqual(ev.toJSON() as never);
    expect(m.attempts).toBe(0);
    expect(m.isPending()).toBe(true);
    expect(m.isDue(NOW)).toBe(true);
  });

  test('scheduleRetry incrementa attempts e adia a próxima tentativa', () => {
    const ok = makeTx();
    ok.markProcessed(undefined, NOW);
    const m = OutboxMessage.enqueue(WagerTransactionProcessed.from(ok, ctx));
    m.scheduleRetry(NOW);
    expect(m.attempts).toBe(1);
    expect(m.nextAttemptAt!.getTime()).toBe(NOW.getTime() + 1000);
    expect(m.isDue(NOW)).toBe(false);
    expect(m.isDue(new Date(NOW.getTime() + 1000))).toBe(true);
    m.scheduleRetry(NOW);
    expect(m.nextAttemptAt!.getTime()).toBe(NOW.getTime() + 2000);
  });

  test('markPublished é terminal', () => {
    const ok = makeTx();
    ok.markProcessed(undefined, NOW);
    const m = OutboxMessage.enqueue(WagerTransactionProcessed.from(ok, ctx));
    m.markPublished(NOW);
    expect(m.isPending()).toBe(false);
    expect(m.isDue(NOW)).toBe(false);
    expect(() => m.markPublished(NOW)).toThrow(InvalidOutboxStateError);
    expect(() => m.scheduleRetry(NOW)).toThrow(InvalidOutboxStateError);
  });
});
