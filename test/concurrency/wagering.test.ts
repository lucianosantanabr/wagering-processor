import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import { IdempotencyConflictError } from '@application/errors';
import type { SubmitWagerTransactionResult } from '@application/submit-wager-transaction';
import { type Services, buildServices } from '@infra/composition';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, startApp } from '../integration/support/app';

let app: TestApp;
let extraOrms: MikroORM[] = [];
let instances: Services[] = []; // "instâncias" da aplicação: cada uma com o seu próprio pool de conexões

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  app = await startApp();
  extraOrms = await Promise.all([1, 2].map(() => MikroORM.init(buildOrmConfig(app.db.url))));
  instances = [app, ...extraOrms.map((orm) => buildServices(orm))];
}, 180_000);
afterAll(async () => {
  await Promise.all(extraOrms.map((o) => o.close(true)));
  await app?.stop();
}, 60_000);

const m = (amount: string) => ({ amount, currency: 'BRL' });
const statuses = (rs: SubmitWagerTransactionResult[]) => rs.map((r) => String(r.status));
const count = (xs: string[], v: string) => xs.filter((x) => x === v).length;

describe('mesma operação em paralelo', () => {
  test('a mesma aposta enviada 50 vezes em paralelo gera um único débito', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f, { money: m('25.00') });
    const results = await Promise.all(Array.from({ length: 50 }, () => app.submitWagerTransaction.execute(c)));

    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(results.filter((r) => r.idempotentReplay)).toHaveLength(49);
    expect(new Set(results.map((r) => r.balance.amount))).toEqual(new Set(['75.00']));

    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(balance).toBe('75.00');
    expect(entries).toBe(2); // abertura + 1 débito
  });

  test('50 vezes em paralelo, distribuídas por 3 instâncias: continua um único débito', async () => {
    const f = await newWallet(app, '100.00');
    const c = cmd(f, { money: m('25.00') });
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => instances[i % instances.length]!.submitWagerTransaction.execute(c)),
    );
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['75.00', 2]);
  });

  test('mesma key com payloads diferentes em paralelo: um vence, os demais são conflito', async () => {
    const f = await newWallet(app, '1000.00');
    const base = cmd(f);
    const attempts = Array.from({ length: 12 }, (_, i) =>
      app.submitWagerTransaction.execute({ ...base, money: m(`${10 + (i % 3)}.00`) }).then(
        (r): SubmitWagerTransactionResult | Error => r,
        (e: Error): SubmitWagerTransactionResult | Error => e,
      ),
    );
    const out = await Promise.all(attempts);
    const winners = out.filter((o): o is SubmitWagerTransactionResult => !(o instanceof Error) && !o.idempotentReplay);
    expect(winners).toHaveLength(1);
    for (const o of out) {
      if (o instanceof Error) expect(o).toBeInstanceOf(IdempotencyConflictError);
    }
    const { entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect(entries).toBe(2);
  });
});

describe('disputa pelo saldo da mesma wallet', () => {
  test('cenário obrigatório: saldo 100, duas apostas de 80 simultâneas', async () => {
    const f = await newWallet(app, '100.00');
    const rs = await Promise.all([
      app.submitWagerTransaction.execute(cmd(f, { money: m('80.00') })),
      app.submitWagerTransaction.execute(cmd(f, { money: m('80.00') })),
    ]);
    const st = statuses(rs);
    expect(count(st, 'PROCESSED')).toBe(1);
    expect(count(st, 'REJECTED')).toBe(1);
    expect(String(rs.find((r) => String(r.status) === 'REJECTED')?.failureCode)).toBe('INSUFFICIENT_FUNDS');

    const [debits] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM wallet_ledger_entries WHERE wallet_id = '${f.walletId}' AND direction = 'DEBIT'`,
    );
    expect(debits?.n).toBe('1');
    const { balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(balance).toBe('20.00');
  });

  test('hot wallet: 40 apostas de 10.00 sobre saldo 100.00 em 3 instâncias -> exatamente 10 passam, saldo 0', async () => {
    const f = await newWallet(app, '100.00');
    const rs = await Promise.all(
      Array.from({ length: 40 }, (_, i) =>
        instances[i % instances.length]!.submitWagerTransaction.execute(cmd(f, { money: m('10.00') })),
      ),
    );
    const st = statuses(rs);
    expect(count(st, 'PROCESSED')).toBe(10);
    expect(count(st, 'REJECTED')).toBe(30);
    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['0.00', 11]);
  });

  test('mistura de BET/WIN/LOSS concorrentes mantém saldo == ledger e nunca negativo', async () => {
    const f = await newWallet(app, '50.00');
    const kinds = ['BET', 'WIN', 'LOSS', 'BET', 'BET'];
    const rs = await Promise.all(
      Array.from({ length: 60 }, (_, i) =>
        instances[i % instances.length]!.submitWagerTransaction.execute(
          cmd(f, { kind: kinds[i % kinds.length]!, money: m(`${(i % 7) + 1}.00`) }),
        ),
      ),
    );
    expect(rs.every((r) => ['PROCESSED', 'REJECTED'].includes(String(r.status)))).toBe(true);
    const { balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(Number(balance)).toBeGreaterThanOrEqual(0);
    // nenhum snapshot observado foi negativo
    expect(rs.every((r) => Number(r.balance.amount) >= 0)).toBe(true);
  });
});

describe('wallets distintas em paralelo', () => {
  test('20 wallets x 2 apostas de 80 sobre 100: cada wallet resolve a sua disputa de forma independente', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => newWallet(app, '100.00')));
    const rs = await Promise.all(
      wallets.flatMap((f, w) =>
        [0, 1].map((k) =>
          instances[(w + k) % instances.length]!.submitWagerTransaction.execute(cmd(f, { money: m('80.00') })),
        ),
      ),
    );
    expect(count(statuses(rs), 'PROCESSED')).toBe(20);
    expect(count(statuses(rs), 'REJECTED')).toBe(20);
    for (const f of wallets) {
      const { balance } = await expectLedgerConsistent(app.db, f.walletId);
      expect(balance).toBe('20.00');
    }
  });
});

describe('reversões concorrentes', () => {
  test('várias reversões simultâneas da mesma BET: exatamente uma é aplicada', async () => {
    const f = await newWallet(app, '100.00');
    const bet = cmd(f, { money: m('30.00') });
    await app.submitWagerTransaction.execute(bet);

    const ref = bet.externalTransactionId;
    const attempts = Array.from({ length: 12 }, (_, i) =>
      instances[i % instances.length]!.submitWagerTransaction.execute(
        cmd(f, { kind: i % 2 ? 'REFUND' : 'ROLLBACK', money: m('30.00'), referenceExternalTransactionId: ref }),
      ),
    );
    const rs = await Promise.all(attempts);
    expect(count(statuses(rs), 'PROCESSED')).toBe(1);
    expect(rs.filter((r) => String(r.status) === 'REJECTED').every((r) => String(r.failureCode) === 'ALREADY_REVERSED')).toBe(true);

    const { balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(balance).toBe('100.00'); // devolvido uma única vez
  });

  test('reversão chegando antes da BET fica PENDING_REFERENCE e não afeta o saldo', async () => {
    const f = await newWallet(app, '100.00');
    const betExt = `bet-${Math.random().toString(36).slice(2)}`;
    const refund = await app.submitWagerTransaction.execute(
      cmd(f, { kind: 'REFUND', money: m('30.00'), referenceExternalTransactionId: betExt }),
    );
    expect(String(refund.status)).toBe('PENDING_REFERENCE');
    await app.submitWagerTransaction.execute(cmd(f, { externalTransactionId: betExt, money: m('30.00') }));
    const { balance } = await expectLedgerConsistent(app.db, f.walletId);
    expect(balance).toBe('70.00'); // o refund pendente só será aplicado pelo worker
  });
});
