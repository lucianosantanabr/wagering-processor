import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import type { ProcessPendingReferences } from '@application/process-pending-references';
import type { ReferencePolicy } from '@application/reference-policy';
import { buildServices } from '@infra/composition';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, startApp } from '../integration/support/app';
import { FakeClock } from '../integration/support/outbox-fakes';

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

const POLICY: ReferencePolicy = { baseMs: 1_000, capMs: 8_000, maxAttempts: 4 };
const money = (amount: string) => ({ amount, currency: 'BRL' });

let app: TestApp;
let extra: MikroORM[] = [];
let clock: FakeClock;
let instances: ReturnType<typeof buildServices>[] = [];

beforeAll(async () => {
  app = await startApp();
  extra = await Promise.all([1, 2].map(() => MikroORM.init(buildOrmConfig(app.db.url))));
  clock = new FakeClock();
  // "3 instâncias": cada uma com o próprio pool; relógio compartilhado para avançar o tempo de forma determinística
  instances = [app.db.orm, ...extra].map((orm) => buildServices(orm, { clock, referencePolicy: POLICY }));
}, 180_000);
beforeEach(async () => {
  // Neutraliza pendencias de testes anteriores (NULL = nunca vence) para contar so as deste teste.
  await app.db.query("UPDATE wager_transactions SET next_attempt_at = NULL WHERE status = 'PENDING_REFERENCE'");
});
afterAll(async () => {
  await Promise.all(extra.map((o) => o.close(true)));
  await app?.stop();
}, 60_000);

const workers = (): ProcessPendingReferences[] => instances.map((s) => s.processPendingReferences);
const sum = (rs: Awaited<ReturnType<ProcessPendingReferences['runOnce']>>[], k: 'selected' | 'resolved' | 'skipped' | 'failed') =>
  rs.reduce((a, r) => a + r[k], 0);

/** Roda todos os workers ao mesmo tempo, rodada após rodada, até não haver mais nada vencido. */
async function drainWithAllWorkers() {
  const total = { resolved: 0, skipped: 0, failed: 0 };
  for (let i = 0; i < 20; i++) {
    clock.advance(1000);
    const rs = await Promise.all(workers().map((w) => w.runOnce()));
    total.resolved += sum(rs, 'resolved');
    total.skipped += sum(rs, 'skipped');
    total.failed += sum(rs, 'failed');
    if (sum(rs, 'selected') === 0) break;
  }
  return total;
}

describe('vários workers sobre as mesmas pendências', () => {
  test('3 workers, 30 reversões pendentes na mesma wallet: cada uma é aplicada exatamente uma vez', async () => {
    const f = await newWallet(app, '1000.00');
    const N = 30;
    const bets = Array.from({ length: N }, () => cmd(f, { money: money('10.00') }));
    // as reversões chegam ANTES das apostas (fora de ordem)
    const refunds = bets.map((b) =>
      cmd(f, { kind: 'REFUND', money: money('10.00'), referenceExternalTransactionId: b.externalTransactionId }),
    );
    const pending = await Promise.all(refunds.map((r, i) => instances[i % 3]!.submitWagerTransaction.execute(r)));
    expect(pending.every((p) => String(p.status) === 'PENDING_REFERENCE')).toBe(true);

    await Promise.all(bets.map((b, i) => instances[i % 3]!.submitWagerTransaction.execute(b)));
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('700.00');

    const total = await drainWithAllWorkers();
    expect(total.resolved).toBe(N); // nem a mais, nem a menos
    expect(total.failed).toBe(0);

    const { balance, entries } = await expectLedgerConsistent(app.db, f.walletId);
    expect([balance, entries]).toEqual(['1000.00', 1 + N + N]); // abertura + N débitos + N créditos
    const [st] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM wager_transactions WHERE wallet_id = '${f.walletId}' AND kind = 'REFUND' AND status = 'PROCESSED'`,
    );
    expect(st?.n).toBe(String(N));
  });

  test('o mesmo item disputado por 3 workers ao mesmo tempo: um aplica, os outros pulam (SKIP LOCKED)', async () => {
    const f = await newWallet(app, '100.00');
    const bet = cmd(f, { money: money('25.00') });
    const refund = cmd(f, { kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: bet.externalTransactionId });
    await instances[0]!.submitWagerTransaction.execute(refund);
    await instances[0]!.submitWagerTransaction.execute(bet);

    clock.advance(1000);
    const rs = await Promise.all(workers().map((w) => w.runOnce()));
    expect(sum(rs, 'resolved')).toBe(1);
    expect(sum(rs, 'failed')).toBe(0);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('100.00');
  });

  test('várias wallets em paralelo + apostas novas concorrentes na mesma wallet: sem deadlock e saldo == ledger', async () => {
    const wallets = await Promise.all(Array.from({ length: 6 }, () => newWallet(app, '500.00')));
    const hot = wallets[0]!;

    // pendências em todas as wallets
    const betsByWallet = wallets.map((w) => Array.from({ length: 5 }, () => cmd(w, { money: money('10.00') })));
    for (const [wi, w] of wallets.entries()) {
      for (const [i, b] of betsByWallet[wi]!.entries()) {
        await instances[i % 3]!.submitWagerTransaction.execute(
          cmd(w, { kind: 'REFUND', money: money('10.00'), referenceExternalTransactionId: b.externalTransactionId }),
        );
      }
    }
    await Promise.all(
      wallets.flatMap((w, wi) => betsByWallet[wi]!.map((b, i) => instances[i % 3]!.submitWagerTransaction.execute(b))),
    );

    // enquanto os workers resolvem, chega tráfego novo na wallet "quente"
    clock.advance(1000);
    const traffic = Array.from({ length: 40 }, (_, i) =>
      instances[i % 3]!.submitWagerTransaction.execute(cmd(hot, { money: money('1.00') })),
    );
    const [rounds] = await Promise.all([
      drainWithAllWorkers(),
      ...traffic,
    ]);
    expect(rounds.failed).toBe(0);
    expect(rounds.resolved).toBe(30); // 6 wallets x 5

    for (const [wi, w] of wallets.entries()) {
      const { balance } = await expectLedgerConsistent(app.db, w.walletId);
      // wallet quente: 500 - 40 apostas de 1.00 (as 5 de 10.00 foram revertidas); demais voltam a 500.00
      expect(balance).toBe(wi === 0 ? '460.00' : '500.00');
    }
  });
});
