import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { PollingWorker } from '@application/polling-worker';
import type { Repositories, UnitOfWork } from '@application/ports';
import type { ReferencePolicy } from '@application/reference-policy';
import { buildPendingReferenceWorker, buildServices } from '@infra/composition';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, outboxTypes, startApp } from './support/app';
import { FakeClock, RecordingMetrics, sleep } from './support/outbox-fakes';
import { waitFor } from './support/sqs-env';

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

let app: TestApp;
beforeAll(async () => {
  app = await startApp();
}, 180_000);
afterAll(async () => {
  await app?.stop();
}, 60_000);

// 4 tentativas: espera 1 s, depois 2 s, 4 s, 8 s (teto) e na 4ª o limite se esgota.
const POLICY: ReferencePolicy = { baseMs: 1_000, capMs: 8_000, maxAttempts: 4 };
const money = (amount: string) => ({ amount, currency: 'BRL' });

let clock: FakeClock;
let metrics: RecordingMetrics;
let world: ReturnType<typeof buildServices>;

beforeEach(async () => {
  // Neutraliza pendências deixadas por testes anteriores (NULL = nunca vence) para contar só as deste teste.
  await app.db.query(`UPDATE wager_transactions SET next_attempt_at = NULL WHERE status = 'PENDING_REFERENCE'`);
  clock = new FakeClock();
  metrics = new RecordingMetrics();
  world = buildServices(app.db.orm, { clock, metrics, referencePolicy: POLICY });
});

const submit = (c: ReturnType<typeof cmd>) => world.submitWagerTransaction.execute(c);
const run = () => world.processPendingReferences.runOnce();
const wallet = (balance = '100.00') => newWallet(world, balance);

const row = async (transactionId: string) =>
  (
    await app.db.query<{
      status: string;
      failure_code: string | null;
      reference_attempts: number;
      next_attempt_at: string | null;
      reference_transaction_id: string | null;
      balance_snapshot: string;
    }>(
      `SELECT status, failure_code, reference_attempts, next_attempt_at, reference_transaction_id,
              balance_snapshot::text AS balance_snapshot
         FROM wager_transactions WHERE id = '${transactionId}'`,
    )
  )[0]!;

describe('referência que chega depois', () => {
  test('REFUND antes da BET: fica PENDING_REFERENCE e o worker o aplica quando a BET aparece', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });

    const pending = await submit(refund);
    expect(String(pending.status)).toBe('PENDING_REFERENCE');
    const r0 = await row(pending.transactionId);
    expect(r0.reference_attempts).toBe(0);
    expect(new Date(r0.next_attempt_at!).getTime() - clock.now().getTime()).toBe(1000); // 1ª nova tentativa agendada

    // ainda não venceu: o worker não toca
    expect(await run()).toMatchObject({ selected: 0 });

    // a BET chega; o REFUND só é aplicado quando vencer a espera
    await submit(bet);
    clock.advance(1000);
    expect(await run()).toMatchObject({ selected: 1, resolved: 1 });

    const after = await row(pending.transactionId);
    expect(after.status).toBe('PROCESSED');
    expect(after.reference_transaction_id).not.toBeNull();
    expect(after.next_attempt_at).toBeNull(); // não é mais candidato
    expect(after.balance_snapshot).toBe('100.00');

    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['100.00', 3]); // abertura, BET (débito), REFUND (crédito)
    const events = await outboxTypes(app.db, f.walletId);
    expect(events).toContain('WagerTransactionPendingReference');
    expect(events.filter((e) => e === 'WagerTransactionProcessed')).toHaveLength(3); // abertura, BET, REFUND
    expect(events.filter((e) => e === 'WalletBalanceChanged')).toHaveLength(3);
    expect(metrics.counters.get('pending_reference_total')).toBeGreaterThanOrEqual(1);
  });

  test('ROLLBACK antes do WIN: debita ao resolver', async () => {
    const f = await wallet('100.00');
    const win = cmd(f, { kind: 'WIN', money: money('40.00') });
    const rollback = cmd(f, { kind: 'ROLLBACK', money: money('40.00'), referenceExternalTransactionId: win.externalTransactionId });
    const pending = await submit(rollback);
    await submit(win); // saldo 140
    clock.advance(1000);
    expect(await run()).toMatchObject({ resolved: 1 });
    expect((await row(pending.transactionId)).status).toBe('PROCESSED');
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });

  test('replay do pedido original, depois de resolvido, devolve o estado atual (PROCESSED)', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    await submit(refund);
    await submit(bet);
    clock.advance(1000);
    await run();

    const replay = await submit(refund);
    expect(replay).toMatchObject({ status: 'PROCESSED', idempotentReplay: true, balance: money('100.00') });
    expect((await expectLedgerConsistent(app.db, f.walletId)).entries).toBe(3); // nada duplicado
  });
});

describe('backoff e limite de espera', () => {
  test('referência ausente: reagenda com backoff exponencial e, no limite, REJECTED/REFERENCE_NOT_FOUND + evento', async () => {
    const f = await wallet('100.00');
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: 'never-arrives' });
    const { transactionId } = await submit(refund);

    const step = async (advanceMs: number, expectAttempts: number, expectDelayMs: number | null) => {
      clock.advance(advanceMs);
      const round = await run();
      const r = await row(transactionId);
      expect(r.reference_attempts).toBe(expectAttempts);
      if (expectDelayMs === null) {
        expect(r.next_attempt_at).toBeNull();
      } else {
        expect(round).toMatchObject({ selected: 1, retried: 1 });
        expect(new Date(r.next_attempt_at!).getTime() - clock.now().getTime()).toBe(expectDelayMs);
      }
      return r;
    };

    await step(1000, 1, 2000); // 1ª tentativa -> próxima em 2 s
    // 1 ms antes de vencer: nada acontece
    clock.advance(1999);
    expect(await run()).toMatchObject({ selected: 0 });
    clock.advance(1);
    await step(0, 2, 4000);
    await step(4000, 3, 8000);

    // 4ª tentativa = limite: rejeita
    clock.advance(8000);
    expect(await run()).toMatchObject({ selected: 1, expired: 1 });
    const final = await row(transactionId);
    expect(final).toMatchObject({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_FOUND', reference_attempts: 4, next_attempt_at: null });

    // evento WagerTransactionRejected com o failureCode, e o saldo intocado
    const [ev] = await app.db.query<{ payload: { data: { failureCode: string; transactionId: string } } }>(
      `SELECT payload FROM outbox_messages WHERE event_type = 'WagerTransactionRejected' AND payload->'data'->>'transactionId' = '${transactionId}'`,
    );
    expect(ev?.payload.data.failureCode).toBe('REFERENCE_NOT_FOUND');
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['100.00', 1]);

    // terminal: nunca mais é selecionada
    clock.advance(60_000);
    expect(await run()).toMatchObject({ selected: 0 });
  });

  test('referência que chega DEPOIS de expirar não ressuscita a reversão; o provedor pode reenviar uma nova', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const lost = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const { transactionId } = await submit(lost);
    for (const ms of [1000, 2000, 4000, 8000]) {
      clock.advance(ms);
      await run();
    }
    expect((await row(transactionId)).status).toBe('REJECTED');

    await submit(bet); // chega tarde
    clock.advance(60_000);
    expect(await run()).toMatchObject({ selected: 0 });
    expect((await row(transactionId)).status).toBe('REJECTED');
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('75.00');

    // nova reversão (novo id externo): agora a referência existe e é aplicada na hora
    const resend = await submit(cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId }));
    expect(String(resend.status)).toBe('PROCESSED');
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });
});

describe('regras de negócio aplicadas na resolução', () => {
  test('valor diferente da referência -> REJECTED AMOUNT_MISMATCH, sem lançamento', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('30.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const { transactionId } = await submit(refund);
    await submit(bet);
    clock.advance(1000);
    expect(await run()).toMatchObject({ rejected: 1 });
    expect(await row(transactionId)).toMatchObject({ status: 'REJECTED', failure_code: 'AMOUNT_MISMATCH' });
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('75.00');
    expect((await outboxTypes(app.db, f.walletId)).filter((t) => t === 'WagerTransactionRejected')).toHaveLength(1);
  });

  test('referência que foi rejeitada -> REFERENCE_NOT_PROCESSED', async () => {
    const f = await wallet('10.00');
    const bet = cmd(f, { money: money('50.00') }); // será rejeitada por saldo
    const refund = cmd(f, { kind: 'REFUND', money: money('50.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const { transactionId } = await submit(refund);
    expect(String((await submit(bet)).status)).toBe('REJECTED');
    clock.advance(1000);
    await run();
    expect(await row(transactionId)).toMatchObject({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_PROCESSED' });
  });

  test('reversão em cadeia (ROLLBACK de um REFUND que também estava pendente) converge em rodadas sucessivas', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const rollback = cmd(f, { kind: 'ROLLBACK', money: money('25.00'), referenceExternalTransactionId: refund.externalTransactionId });

    const ids = {
      rollback: (await submit(rollback)).transactionId, // depende do REFUND, que ainda não existe
      refund: (await submit(refund)).transactionId, // depende da BET, que ainda não existe
    };
    await submit(bet);

    for (let i = 0; i < 6; i++) {
      clock.advance(8000);
      if ((await run()).selected === 0) break;
    }
    expect((await row(ids.refund)).status).toBe('PROCESSED');
    expect((await row(ids.rollback)).status).toBe('PROCESSED');
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 4]); // abertura, BET -25, REFUND +25, ROLLBACK -25
  });

  test('reversão pendente (worker) e reversão nova (HTTP/fila) disputando a mesma BET: só uma é aplicada', async () => {
    const f = await wallet('100.00');
    const bet = cmd(f, { money: money('25.00') });
    const pendingRefund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const pending = await submit(pendingRefund);
    await submit(bet);
    clock.advance(1000);

    const fresh = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const [, freshResult] = await Promise.all([run(), submit(fresh)]);

    const a = await row(pending.transactionId);
    const results = [a.status, String(freshResult.status)].sort();
    expect(results).toEqual(['PROCESSED', 'REJECTED']);
    const rejectedCode = a.status === 'REJECTED' ? a.failure_code : freshResult.failureCode;
    expect(String(rejectedCode)).toBe('ALREADY_REVERSED');
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00'); // devolvido uma única vez
  });
});

describe('atomicidade e resiliência', () => {
  test('falha no meio da resolução desfaz tudo daquele item (continua pendente); a rodada seguinte conclui', async () => {
    let breakOutbox = false;
    const flaky: UnitOfWork = {
      run: (fn) =>
        app.uow.run((repos: Repositories) =>
          fn({
            ...repos,
            outbox: Object.assign(Object.create(repos.outbox), {
              enqueue: async (m: unknown) => {
                if (breakOutbox) throw new Error('outbox down');
                return (repos.outbox.enqueue as (x: unknown) => Promise<void>)(m);
              },
            }),
          }),
        ),
    };
    const w = buildServices(app.db.orm, { clock, uow: flaky, referencePolicy: POLICY });
    const f = await newWallet(w, '100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const { transactionId } = await w.submitWagerTransaction.execute(refund);
    await w.submitWagerTransaction.execute(bet);
    clock.advance(1000);

    breakOutbox = true;
    expect(await w.processPendingReferences.runOnce()).toMatchObject({ selected: 1, failed: 1, resolved: 0 });
    // rollback completo: transação ainda pendente e sem tentativas contabilizadas, saldo e ledger intactos
    expect(await row(transactionId)).toMatchObject({ status: 'PENDING_REFERENCE', reference_attempts: 0 });
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('75.00');

    breakOutbox = false;
    expect(await w.processPendingReferences.runOnce()).toMatchObject({ resolved: 1, failed: 0 });
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });

  test('worker de fundo: resolve sozinho (relógio real) e para de forma graciosa', async () => {
    const fast = buildServices(app.db.orm, { referencePolicy: { baseMs: 50, capMs: 200, maxAttempts: 100 } });
    const f = await newWallet(fast, '100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    const { transactionId } = await fast.submitWagerTransaction.execute(refund);

    const worker = buildPendingReferenceWorker(fast, { idleIntervalMs: 20 });
    worker.start();
    await sleep(120);
    expect((await row(transactionId)).status).toBe('PENDING_REFERENCE'); // referência ainda ausente
    await fast.submitWagerTransaction.execute(bet);
    await waitFor(async () => (await row(transactionId)).status === 'PROCESSED', 10_000, 50);
    await worker.stop();
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });

  test('PollingWorker sobrevive a uma rodada que lança e continua', async () => {
    let calls = 0;
    const w = new PollingWorker('x', async () => {
      calls += 1;
      if (calls === 1) throw new Error('boom');
      return false;
    }, 5);
    w.start();
    await waitFor(async () => calls >= 3, 5000, 10);
    await w.stop();
    expect(calls).toBeGreaterThanOrEqual(3);
  });
});
