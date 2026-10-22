import { describe, expect, test } from 'bun:test';
import { InvalidTransactionStateError, InvalidWagerTransactionError } from '@domain/errors';
import { FailureCode } from '@domain/failure-code';
import { computePayloadHash } from '@domain/payload-hash';
import { WagerTransaction, WagerTransactionKind as K, WagerTransactionStatus as S } from '@domain/wager-transaction';
import { LedgerDirection } from '@domain/wallet-ledger-entry';
import { NOW, brl, makeTx } from './helpers';

describe('WagerTransaction.create', () => {
  test('nasce PENDING', () => {
    expect(makeTx().status).toBe(S.Pending);
  });

  test('OPENING não pode ser submetido', () => {
    expect(() => makeTx({ kind: K.Opening as never })).toThrow(InvalidWagerTransactionError);
  });

  test.each([K.Refund, K.Rollback] as const)('%s exige referência', (kind) => {
    expect(() => makeTx({ kind })).toThrow(InvalidWagerTransactionError);
    expect(makeTx({ kind, referenceExternalTransactionId: 'ext-ref' }).requiresReference()).toBe(true);
  });

  test('BET não aceita referência; WIN/LOSS aceitam opcionalmente', () => {
    expect(() => makeTx({ kind: K.Bet, referenceExternalTransactionId: 'x' })).toThrow(
      InvalidWagerTransactionError,
    );
    expect(() => makeTx({ kind: K.Win, referenceExternalTransactionId: 'x' })).not.toThrow();
    expect(() => makeTx({ kind: K.Win })).not.toThrow();
  });

  test('valor deve ser positivo, exceto LOSS que aceita zero', () => {
    expect(() => makeTx({ kind: K.Bet, money: brl('0.00') })).toThrow(InvalidWagerTransactionError);
    expect(() => makeTx({ kind: K.Loss, money: brl('0.00') })).not.toThrow();
  });

  test('opening() é interno, nasce PENDING e tem chave derivada do wallet', () => {
    const tx = WagerTransaction.opening({
      id: 'o1',
      payloadHash: 'h',
      walletId: 'w1',
      playerId: 'p1',
      money: brl('1000.00'),
      now: NOW,
    });
    expect(tx.kind).toBe(K.Opening);
    expect(tx.status).toBe(S.Pending);
    expect(tx.idempotencyKey).toBe('internal:opening:w1');
  });
});

describe('WagerTransaction transições', () => {
  test('PENDING -> PROCESSED', () => {
    const tx = makeTx();
    tx.markProcessed(undefined, NOW);
    expect(tx.status).toBe(S.Processed);
    expect(tx.processedAt).toEqual(NOW);
    expect(tx.isTerminal()).toBe(true);
  });

  test('PENDING -> PENDING_REFERENCE -> PROCESSED (com referência resolvida)', () => {
    const tx = makeTx({ kind: K.Refund, referenceExternalTransactionId: 'r' });
    tx.markPendingReference();
    expect(tx.status).toBe(S.PendingReference);
    expect(tx.isTerminal()).toBe(false);
    tx.markProcessed('internal-ref-id', NOW);
    expect(tx.referenceTransactionId).toBe('internal-ref-id');
  });

  test('REFUND/ROLLBACK não podem ser processados sem referência resolvida', () => {
    const tx = makeTx({ kind: K.Rollback, referenceExternalTransactionId: 'r' });
    expect(() => tx.markProcessed(undefined, NOW)).toThrow(InvalidWagerTransactionError);
  });

  test('markPendingReference só vale para kinds com referência e a partir de PENDING', () => {
    expect(() => makeTx({ kind: K.Bet }).markPendingReference()).toThrow(InvalidWagerTransactionError);
    const tx = makeTx({ kind: K.Refund, referenceExternalTransactionId: 'r' });
    tx.markPendingReference();
    expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
  });

  test('reject e fail registram failureCode', () => {
    const a = makeTx();
    a.reject(FailureCode.InsufficientFunds);
    expect(a.status).toBe(S.Rejected);
    expect(a.failureCode).toBe(FailureCode.InsufficientFunds);

    const b = makeTx();
    b.fail(FailureCode.ProcessingError);
    expect(b.status).toBe(S.Failed);
  });

  test('PENDING_REFERENCE -> REJECTED (referência nunca chegou)', () => {
    const tx = makeTx({ kind: K.Refund, referenceExternalTransactionId: 'r' });
    tx.markPendingReference();
    tx.reject(FailureCode.ReferenceNotFound);
    expect(tx.failureCode).toBe(FailureCode.ReferenceNotFound);
  });

  test.each([
    ['PROCESSED', (t: WagerTransaction) => t.markProcessed(undefined, NOW)],
    ['REJECTED', (t: WagerTransaction) => t.reject(FailureCode.InsufficientFunds)],
    ['FAILED', (t: WagerTransaction) => t.fail(FailureCode.ProcessingError)],
  ])('estado terminal %s não transiciona mais', (_name, toTerminal) => {
    const tx = makeTx();
    toTerminal(tx);
    expect(() => tx.markProcessed(undefined, NOW)).toThrow(InvalidTransactionStateError);
    expect(() => tx.reject(FailureCode.InsufficientFunds)).toThrow(InvalidTransactionStateError);
    expect(() => tx.fail(FailureCode.ProcessingError)).toThrow(InvalidTransactionStateError);
    expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
  });

  test('rehydrate não revalida transições', () => {
    const tx = WagerTransaction.rehydrate({
      id: 'x', providerId: 'p', externalTransactionId: 'e', idempotencyKey: 'k', payloadHash: 'h',
      walletId: 'w', playerId: 'pl', roundId: 'r', gameId: 'g', kind: K.Bet, money: brl('1.00'),
      createdAt: NOW, status: S.Processed, processedAt: NOW,
    });
    expect(tx.isTerminal()).toBe(true);
  });
});

describe('WagerTransaction consultas', () => {
  test('affectsBalance é false só para LOSS', () => {
    expect(makeTx({ kind: K.Loss }).affectsBalance()).toBe(false);
    expect(makeTx({ kind: K.Bet }).affectsBalance()).toBe(true);
    expect(makeTx({ kind: K.Win }).affectsBalance()).toBe(true);
  });

  test('matchesPayload compara o hash', () => {
    const tx = makeTx({ payloadHash: 'abc' });
    expect(tx.matchesPayload('abc')).toBe(true);
    expect(tx.matchesPayload('abd')).toBe(false);
  });

  test('direção do lançamento por kind', () => {
    expect(makeTx({ kind: K.Bet }).ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(makeTx({ kind: K.Win }).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(makeTx({ kind: K.Refund, referenceExternalTransactionId: 'r' }).ledgerDirectionFor()).toBe(
      LedgerDirection.Credit,
    );
    expect(() => makeTx({ kind: K.Loss }).ledgerDirectionFor()).toThrow(InvalidWagerTransactionError);
  });

  test('ROLLBACK inverte a direção da referência', () => {
    const rb = makeTx({ kind: K.Rollback, referenceExternalTransactionId: 'r' });
    expect(rb.ledgerDirectionFor(makeTx({ kind: K.Bet }))).toBe(LedgerDirection.Credit);
    expect(rb.ledgerDirectionFor(makeTx({ kind: K.Win }))).toBe(LedgerDirection.Debit);
    expect(
      rb.ledgerDirectionFor(makeTx({ kind: K.Refund, referenceExternalTransactionId: 'x' })),
    ).toBe(LedgerDirection.Debit);
  });

  test('ROLLBACK exige referência válida (não LOSS, não ROLLBACK)', () => {
    const rb = makeTx({ kind: K.Rollback, referenceExternalTransactionId: 'r' });
    expect(() => rb.ledgerDirectionFor()).toThrow(InvalidWagerTransactionError);
    expect(() => rb.ledgerDirectionFor(makeTx({ kind: K.Loss }))).toThrow(InvalidWagerTransactionError);
    expect(() =>
      rb.ledgerDirectionFor(makeTx({ kind: K.Rollback, referenceExternalTransactionId: 'y' })),
    ).toThrow(InvalidWagerTransactionError);
  });
});

describe('payloadHash', () => {
  const fields = {
    providerId: 'provider-a',
    externalTransactionId: 't1',
    playerId: 'p',
    walletId: 'w',
    roundId: 'r',
    gameId: 'g',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
  };

  test('é determinístico, independe da ordem das chaves e normaliza o valor', () => {
    const reordered = {
      money: { currency: 'BRL', amount: '25' },
      kind: 'BET',
      gameId: 'g',
      roundId: 'r',
      walletId: 'w',
      playerId: 'p',
      externalTransactionId: 't1',
      providerId: 'provider-a',
    };
    expect(computePayloadHash(reordered)).toBe(computePayloadHash(fields));
    expect(computePayloadHash(fields)).toMatch(/^[0-9a-f]{64}$/);
  });

  test('qualquer campo de negócio diferente muda o hash', () => {
    const base = computePayloadHash(fields);
    expect(computePayloadHash({ ...fields, money: { amount: '25.01', currency: 'BRL' } })).not.toBe(base);
    expect(computePayloadHash({ ...fields, roundId: 'r2' })).not.toBe(base);
    expect(computePayloadHash({ ...fields, referenceExternalTransactionId: 'x' })).not.toBe(base);
  });

  test('mesma key com payload diferente é detectável via matchesPayload', () => {
    const tx = makeTx({ payloadHash: computePayloadHash(fields) });
    expect(tx.matchesPayload(computePayloadHash({ ...fields, money: { amount: '30.00', currency: 'BRL' } }))).toBe(false);
  });
});
