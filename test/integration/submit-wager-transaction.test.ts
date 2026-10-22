import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  IdempotencyConflictError,
  ValidationError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '@application/errors';
import type { Repositories, UnitOfWork } from '@application/ports';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, outboxTypes, startApp } from './support/app';

let app: TestApp;
// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  app = await startApp();
}, 180_000);
afterAll(async () => {
  await app?.stop();
}, 60_000);

const submit = (c: ReturnType<typeof cmd>) => app.submitWagerTransaction.execute(c);

describe('CreateWallet', () => {
  test('saldo inicial gera OPENING + CREDIT no ledger e eventos na outbox, na mesma transação', async () => {
    const f = await newWallet(app, '1000.00');
    const { entries, balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(1);
    expect(balance).toBe('1000.00');
    const [t] = await app.db.query<{ kind: string; status: string }>(
      `SELECT kind, status FROM wager_transactions WHERE wallet_id = '${f.walletId}'`,
    );
    expect(t).toEqual({ kind: 'OPENING', status: 'PROCESSED' });
    expect(await outboxTypes(app.db, f.walletId)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  });

  test('saldo inicial zero não gera OPENING nem lançamento', async () => {
    const f = await newWallet(app, '0.00');
    const [r] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM wager_transactions WHERE wallet_id = '${f.walletId}'`,
    );
    expect(r?.n).toBe('0');
  });

  test('wallet duplicada para (player, moeda) é conflito; outra moeda é permitida', async () => {
    const f = await newWallet(app, '10.00');
    await expect(
      app.createWallet.execute({ playerId: f.playerId, initialBalance: { amount: '5.00', currency: 'BRL' } }),
    ).rejects.toBeInstanceOf(WalletAlreadyExistsError);
    await app.createWallet.execute({ playerId: f.playerId, initialBalance: { amount: '5.00', currency: 'USD' } });
  });

  test('criações simultâneas para o mesmo player: só uma vence', async () => {
    const playerId = randomUUID();
    const results = await Promise.allSettled(
      Array.from({ length: 10 }, () =>
        app.createWallet.execute({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }),
      ),
    );
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    for (const r of results.filter((r): r is PromiseRejectedResult => r.status === 'rejected')) {
      expect(r.reason).toBeInstanceOf(WalletAlreadyExistsError);
    }
  });

  test('entradas inválidas', async () => {
    await expect(
      app.createWallet.execute({ playerId: 'x', initialBalance: { amount: '1.00', currency: 'BRL' } }),
    ).rejects.toBeInstanceOf(ValidationError);
    await expect(
      app.createWallet.execute({ playerId: randomUUID(), initialBalance: { amount: '-1.00', currency: 'BRL' } }),
    ).rejects.toBeInstanceOf(ValidationError);
  });
});

describe('BET / WIN / LOSS', () => {
  test('BET debita, gera 1 lançamento DEBIT e os eventos Processed + BalanceChanged', async () => {
    const f = await newWallet(app, '1000.00');
    const r = await submit(cmd(f, { money: { amount: '25.00', currency: 'BRL' } }));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '975.00', currency: 'BRL' }, idempotentReplay: false });
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(2);
    expect((await outboxTypes(app.db, f.walletId)).slice(2)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  });

  test('BET sem saldo é REJECTED (INSUFFICIENT_FUNDS): sem lançamento e só o evento Rejected', async () => {
    const f = await newWallet(app, '10.00');
    const r = await submit(cmd(f, { money: { amount: '10.01', currency: 'BRL' } }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '10.00' } });
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(1);
    expect((await outboxTypes(app.db, f.walletId)).slice(2)).toEqual(['WagerTransactionRejected']);
  });

  test('WIN credita', async () => {
    const f = await newWallet(app, '100.00');
    const r = await submit(cmd(f, { kind: 'WIN', money: { amount: '40.00', currency: 'BRL' } }));
    expect(r.balance.amount).toBe('140.00');
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('LOSS não move saldo, não gera lançamento nem WalletBalanceChanged', async () => {
    const f = await newWallet(app, '100.00');
    const r = await submit(cmd(f, { kind: 'LOSS' }));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
    // version não muda e há só o lançamento de abertura
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(1);
    expect((await outboxTypes(app.db, f.walletId)).slice(2)).toEqual(['WagerTransactionProcessed']);
  });

  test('moeda diferente da wallet e player diferente são rejeitados e auditáveis', async () => {
    const f = await newWallet(app, '100.00');
    const usd = await submit(cmd(f, { money: { amount: '5.00', currency: 'USD' } }));
    expect(usd).toMatchObject({ status: 'REJECTED', failureCode: 'CURRENCY_MISMATCH' });
    const other = await submit(cmd({ ...f, playerId: randomUUID() }));
    expect(other).toMatchObject({ status: 'REJECTED', failureCode: 'WALLET_MISMATCH' });
    await expectLedgerConsistent(app.db, f.walletId);
  });
});

describe('contrato de entrada', () => {
  test('wallet inexistente não persiste nada', async () => {
    const f = { walletId: randomUUID(), playerId: randomUUID() };
    await expect(submit(cmd(f))).rejects.toBeInstanceOf(WalletNotFoundError);
  });

  test.each([
    ['OPENING não pode ser submetido', { kind: 'OPENING' }],
    ['kind desconhecido', { kind: 'DEPOSIT' }],
    ['valor negativo', { money: { amount: '-5.00', currency: 'BRL' } }],
    ['mais de 2 casas', { money: { amount: '5.001', currency: 'BRL' } }],
    ['notação científica', { money: { amount: '1e2', currency: 'BRL' } }],
    ['walletId inválido', { walletId: 'nope' }],
    ['REFUND sem referência', { kind: 'REFUND' }],
    ['BET com referência', { referenceExternalTransactionId: 'x' }],
    ['provider reservado', { providerId: 'internal' }],
  ])('%s -> ValidationError', async (_name, over) => {
    const f = await newWallet(app, '100.00');
    await expect(submit(cmd(f, over as never))).rejects.toBeInstanceOf(ValidationError);
  });
});

describe('idempotência', () => {
  test('replay devolve o resultado ORIGINAL (inclusive o saldo da época), sem novo efeito', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f, { money: { amount: '30.00', currency: 'BRL' } });
    const first = await submit(c);
    await submit(cmd(f, { money: { amount: '20.00', currency: 'BRL' } })); // saldo muda depois
    const replay = await submit(c);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(replay.balance.amount).toBe('70.00'); // não 50.00
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(3);
  });

  test('replay de uma rejeição também devolve o resultado original', async () => {
    const f = await newWallet(app, '5.00');
    const c = cmd(f, { money: { amount: '50.00', currency: 'BRL' } });
    const first = await submit(c);
    await submit(cmd(f, { kind: 'WIN', money: { amount: '100.00', currency: 'BRL' } })); // agora haveria saldo
    expect(await submit(c)).toEqual({ ...first, idempotentReplay: true });
  });

  test('mesma key com payload diferente é CONFLITO, não replay', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f);
    await submit(c);
    await expect(submit({ ...c, money: { amount: '26.00', currency: 'BRL' } })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('"25" e "25.00" têm o mesmo hash: é replay', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f, { money: { amount: '25.00', currency: 'BRL' } });
    await submit(c);
    const r = await submit({ ...c, money: { amount: '25', currency: 'BRL' } });
    expect(r.idempotentReplay).toBe(true);
  });

  test('corrida entre as duas buscas (por key e por id externo): é replay, nunca um falso 409', async () => {
    // Reproduz o intervalo em que a transação vencedora comita ENTRE a busca por idempotency key (que ainda
    // não a via) e a busca por (provider, id externo) (que já a vê). Antes da correção isto virava 409.
    const f = await newWallet(app, '100.00');
    const c = cmd(f);
    const first = await submit(c);

    const blindToKey: UnitOfWork = {
      run: (fn) =>
        app.uow.run((repos: Repositories) =>
          fn({
            ...repos,
            transactions: Object.assign(Object.create(repos.transactions), {
              findByIdempotencyKey: async () => null, // "ainda não comitado" na 1ª busca
            }),
          }),
        ),
    };
    const racing = (await import('@infra/composition')).buildServices(app.db.orm, { uow: blindToKey });

    const replay = await racing.submitWagerTransaction.execute(c);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    // e a MESMA key com payload diferente continua sendo conflito de verdade
    await expect(
      racing.submitWagerTransaction.execute({ ...c, money: { amount: '26.00', currency: 'BRL' } }),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect((await expectLedgerConsistent(app.db, f.walletId)).entries).toBe(2);
  });

  test('mesmo id externo sob outra idempotency key é conflito', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f);
    await submit(c);
    await expect(submit({ ...c, idempotencyKey: 'another-key' })).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  test('inbox: a mesma messageId entregue duas vezes não duplica efeitos', async () => {
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    const c = cmd(f, { inbox: { consumerName: 'wager-consumer', messageId, payloadHash: 'h'.repeat(64) } });
    const a = await submit(c);
    const b = await submit(c);
    expect(a.idempotentReplay).toBe(false);
    expect(b).toEqual({ ...a, idempotentReplay: true });
    const [row] = await app.db.query<{ processed_at: unknown }>(
      `SELECT processed_at FROM inbox_messages WHERE message_id = '${messageId}'`,
    );
    expect(row?.processed_at).not.toBeNull();
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(2);
  });
});

describe('REFUND / ROLLBACK', () => {
  const m = (amount: string) => ({ amount, currency: 'BRL' });

  async function betOf(f: Awaited<ReturnType<typeof newWallet>>, amount = '25.00', kind = 'BET') {
    const c = cmd(f, { kind, money: m(amount) });
    const r = await submit(c);
    return { c, r };
  }

  test('REFUND reverte uma BET processada (crédito)', async () => {
    const f = await newWallet(app, '100.00');
    const bet = await betOf(f);
    const r = await submit(cmd(f, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: bet.c.externalTransactionId }));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('ROLLBACK de WIN debita (inverso da referência) e de BET credita', async () => {
    const f = await newWallet(app, '100.00');
    const win = await betOf(f, '40.00', 'WIN');
    const rbWin = await submit(cmd(f, { kind: 'ROLLBACK', money: m('40.00'), referenceExternalTransactionId: win.c.externalTransactionId }));
    expect(rbWin.balance.amount).toBe('100.00');
    const bet = await betOf(f, '10.00');
    const rbBet = await submit(cmd(f, { kind: 'ROLLBACK', money: m('10.00'), referenceExternalTransactionId: bet.c.externalTransactionId }));
    expect(rbBet.balance.amount).toBe('100.00');
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('ROLLBACK de REFUND é permitido (debita de volta)', async () => {
    const f = await newWallet(app, '100.00');
    const bet = await betOf(f);
    const refund = cmd(f, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: bet.c.externalTransactionId });
    await submit(refund);
    const rb = await submit(cmd(f, { kind: 'ROLLBACK', money: m('25.00'), referenceExternalTransactionId: refund.externalTransactionId }));
    expect(rb).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00' } });
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('uma referência é revertida uma única vez (ALREADY_REVERSED), inclusive entre REFUND e ROLLBACK', async () => {
    const f = await newWallet(app, '100.00');
    const bet = await betOf(f);
    const ref = bet.c.externalTransactionId;
    await submit(cmd(f, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: ref }));
    const again = await submit(cmd(f, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: ref }));
    const rollback = await submit(cmd(f, { kind: 'ROLLBACK', money: m('25.00'), referenceExternalTransactionId: ref }));
    expect(again).toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED' });
    expect(rollback).toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED' });
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });

  interface Case {
    kind: string;
    amount: string;
    refKind?: string;
    refAmount?: string;
    round?: string;
  }
  test.each<[string, Case, string]>([
    ['valor diferente', { kind: 'REFUND', amount: '24.00' }, 'AMOUNT_MISMATCH'],
    ['REFUND de WIN', { kind: 'REFUND', amount: '25.00', refKind: 'WIN' }, 'INVALID_REFERENCE_KIND'],
    // LOSS aceita 0.00; a reversão exige > 0, e o tipo da referência é barrado antes do valor
    ['ROLLBACK de LOSS', { kind: 'ROLLBACK', amount: '0.01', refKind: 'LOSS', refAmount: '0.00' }, 'INVALID_REFERENCE_KIND'],
    ['rodada diferente', { kind: 'REFUND', amount: '25.00', round: 'round-2' }, 'REFERENCE_MISMATCH'],
  ])('%s -> REJECTED %s', async (_n, o, code) => {
    const f = await newWallet(app, '100.00');
    const ref = await betOf(f, o.refAmount ?? '25.00', o.refKind ?? 'BET');
    const r = await submit(
      cmd(f, {
        kind: o.kind,
        money: m(o.amount),
        roundId: o.round ?? 'round-1',
        referenceExternalTransactionId: ref.c.externalTransactionId,
      }),
    );
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: code });
    await expectLedgerConsistent(app.db, f.walletId);
  });

  test('referência de outro player/wallet é REFERENCE_MISMATCH', async () => {
    const a = await newWallet(app, '100.00');
    const b = await newWallet(app, '100.00');
    const bet = await betOf(a);
    const r = await submit(cmd(b, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: bet.c.externalTransactionId }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_MISMATCH' });
  });

  test('referência rejeitada -> REFERENCE_NOT_PROCESSED', async () => {
    const f = await newWallet(app, '10.00');
    const bet = await betOf(f, '50.00'); // rejeitada por saldo
    expect(String(bet.r.status)).toBe('REJECTED');
    const r = await submit(cmd(f, { kind: 'REFUND', money: m('50.00'), referenceExternalTransactionId: bet.c.externalTransactionId }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_NOT_PROCESSED' });
  });

  test('reversão que deixaria saldo negativo -> REVERSAL_WOULD_OVERDRAW (código distinto de INSUFFICIENT_FUNDS)', async () => {
    const f = await newWallet(app, '0.00');
    const win = await betOf(f, '50.00', 'WIN'); // saldo 50
    // gasta o prêmio
    const spend = await submit(cmd(f, { money: m('50.00') }));
    expect(String(spend.status)).toBe('PROCESSED');
    const r = await submit(cmd(f, { kind: 'ROLLBACK', money: m('50.00'), referenceExternalTransactionId: win.c.externalTransactionId }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: 'REVERSAL_WOULD_OVERDRAW' });
    expect(r.failureCode).not.toBe('INSUFFICIENT_FUNDS');
    expect(r.balance.amount).toBe('0.00');
  });

  test('referência ausente -> PENDING_REFERENCE, sem efeito no saldo, com evento', async () => {
    const f = await newWallet(app, '100.00');
    const r = await submit(cmd(f, { kind: 'REFUND', money: m('25.00'), referenceExternalTransactionId: 'does-not-exist-yet' }));
    expect(r).toMatchObject({ status: 'PENDING_REFERENCE', balance: { amount: '100.00' } });
    expect((await outboxTypes(app.db, f.walletId)).slice(2)).toEqual(['WagerTransactionPendingReference']);
    await expectLedgerConsistent(app.db, f.walletId);
  });
});

describe('atomicidade', () => {
  test('falha ao gravar a outbox desfaz transação, saldo e ledger (tudo ou nada)', async () => {
    const failing: UnitOfWork = {
      run: (fn) =>
        app.uow.run((repos: Repositories) =>
          fn({
            ...repos,
            // Object.create preserva os métodos do protótipo; só `enqueue` falha.
            outbox: Object.assign(Object.create(repos.outbox), {
              enqueue: async () => {
                throw new Error('outbox down');
              },
            }),
          }),
        ),
    };
    const broken = (await import('@infra/composition')).buildServices(app.db.orm, { uow: failing });
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    const c = cmd(f, { inbox: { consumerName: 'wager-consumer', messageId, payloadHash: 'h'.repeat(64) } });
    await expect(broken.submitWagerTransaction.execute(c)).rejects.toThrow('outbox down');

    const [t] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM wager_transactions WHERE idempotency_key = '${c.idempotencyKey}'`,
    );
    expect(t?.n).toBe('0');
    // a inbox faz parte da MESMA transação: a mensagem não pode ficar "recebida" sem efeito
    const [inbox] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM inbox_messages WHERE message_id = '${messageId}'`,
    );
    expect(inbox?.n).toBe('0');
    const { entries, balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(1);
    expect(balance).toBe('100.00');
    // e o retry correto depois da falha processa normalmente (sem duplicar)
    const ok = await submit(c);
    expect(String(ok.status)).toBe('PROCESSED');
  });
});
