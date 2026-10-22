import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from 'bun:test';
import {
  type Infra,
  type Instance,
  createWallet,
  divergentWallets,
  drainEvents,
  http,
  ledgerState,
  money,
  sendWagerMessages,
  startInfra,
  startInstance,
  submit,
  sumMetric,
  txBody,
  wagerMessage,
  waitUntil,
} from './support/cluster';

setDefaultTimeout(300_000);

let infra: Infra;
let cluster: Instance[] = [];
const pick = (i: number) => cluster[i % cluster.length]!;

beforeAll(async () => {
  infra = await startInfra();
  // 3 PROCESSOS do SO, subindo ao mesmo tempo, compartilhando o mesmo PostgreSQL e as mesmas filas SQS.
  cluster = await Promise.all(['p1', 'p2', 'p3'].map((id) => startInstance(infra, id)));
}, 400_000);
afterAll(async () => {
  await Promise.all(cluster.map((i) => i.kill().catch(() => undefined)));
  await infra?.stop();
}, 120_000);

const count = (xs: { status: number }[], s: number) => xs.filter((x) => x.status === s).length;

describe('3 processos, HTTP', () => {
  test('a mesma aposta enviada 60 vezes em paralelo, espalhada pelos 3 processos: um único débito', async () => {
    const w = await createWallet(pick(0), '100.00');
    const body = txBody(w);
    const rs = await Promise.all(Array.from({ length: 60 }, (_, i) => submit(pick(i), body)));

    expect(count(rs, 201)).toBe(1);
    expect(count(rs, 200)).toBe(59); // todas as demais são replays idempotentes
    expect(new Set(rs.map((r) => r.json.transactionId)).size).toBe(1);
    const s = await ledgerState(infra, w.walletId);
    expect(s).toMatchObject({ balance: '75.00', entries: 2, consistent: true });
  });

  test('cenário obrigatório entre processos: saldo 100, duas apostas de 80 chegando a processos diferentes', async () => {
    const results: { status: number; json: any }[][] = [];
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet(pick(0), '100.00')));
    for (const w of wallets) {
      results.push(await Promise.all([submit(pick(0), txBody(w, { money: money('80.00') })), submit(pick(1), txBody(w, { money: money('80.00') }))]));
    }
    for (const [i, pair] of results.entries()) {
      expect(count(pair, 201)).toBe(1); // exatamente uma PROCESSED
      expect(count(pair, 422)).toBe(1); // a outra REJECTED por saldo
      expect(pair.find((r) => r.status === 422)!.json.failureCode).toBe('INSUFFICIENT_FUNDS');
      const s = await ledgerState(infra, wallets[i]!.walletId);
      expect(s).toMatchObject({ balance: '20.00', entries: 2, consistent: true }); // um único débito
    }
  });

  test('wallet quente: 90 apostas de 10.00 sobre 300.00 distribuídas pelos processos -> exatamente 30 passam, saldo 0', async () => {
    const w = await createWallet(pick(0), '300.00');
    const rs = await Promise.all(Array.from({ length: 90 }, (_, i) => submit(pick(i), txBody(w, { money: money('10.00') }))));
    expect(count(rs, 201)).toBe(30);
    expect(count(rs, 422)).toBe(90 - 30);
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '0.00', entries: 31, consistent: true });
  });

  test('o mesmo pedido e a MESMA key com payload diferente em processos diferentes: um vence, os outros são 409', async () => {
    const w = await createWallet(pick(0), '1000.00');
    const base = txBody(w);
    const rs = await Promise.all(
      Array.from({ length: 12 }, (_, i) => submit(pick(i), { ...base, money: money(`${10 + (i % 3)}.00`) })),
    );
    expect(count(rs, 201)).toBe(1);
    expect(rs.every((r) => [200, 201, 409].includes(r.status))).toBe(true);
    expect((await ledgerState(infra, w.walletId)).entries).toBe(2);
  });
});

describe('3 processos, SQS (consumidores competindo)', () => {
  test('120 mensagens em 12 wallets: processadas uma vez cada, por mais de um processo; saldo == ledger', async () => {
    const handled = async () =>
      (await sumMetric(cluster, 'sqs_messages_total{result="processed"}')) +
      (await sumMetric(cluster, 'sqs_messages_total{result="duplicate"}'));
    const before = await handled();
    const wallets = await Promise.all(Array.from({ length: 12 }, () => createWallet(pick(0), '100.00')));
    const msgs = wallets.flatMap((w) => Array.from({ length: 10 }, () => wagerMessage(w, { money: money('5.00') })));
    await sendWagerMessages(infra, msgs);

    await waitUntil(
      async () => {
        const [r] = await infra.query<{ n: string }>(
          `SELECT count(*)::text n FROM inbox_messages WHERE processed_at IS NOT NULL AND message_id = ANY(ARRAY[${msgs.map((m) => `'${m.messageId}'`).join(',')}])`,
        );
        return Number(r!.n) === msgs.length;
      },
      180_000,
      500,
      '120 mensagens processadas',
    );

    for (const w of wallets) {
      expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '50.00', entries: 11, consistent: true });
    }
    // Pelo menos uma entrega de cada mensagem foi confirmada. Pode haver reentrega (se um processo demorar mais que
    // o visibility timeout), contada como "duplicate" pelo outro: o que NÃO pode haver é efeito duplicado,
    // e isso é verificado acima, wallet a wallet (saldo, nº de lançamentos).
    expect((await handled()) - before).toBeGreaterThanOrEqual(120);

    const perInstance = await Promise.all(cluster.map((i) => sumMetric([i], 'sqs_messages_total{result="processed"}')));
    expect(perInstance.filter((n) => n > 0).length).toBeGreaterThanOrEqual(2); // o trabalho foi realmente dividido
    const [dlq] = await infra.query<{ n: string }>(`SELECT count(*)::text n FROM wager_transactions WHERE status = 'FAILED'`);
    expect(dlq!.n).toBe('0');
  });

  test('o MESMO use case por dois canais: 30 apostas por HTTP e 30 por SQS na mesma wallet, ao mesmo tempo', async () => {
    const w = await createWallet(pick(0), '500.00');
    const http$ = Promise.all(Array.from({ length: 30 }, (_, i) => submit(pick(i), txBody(w, { money: money('5.00') }))));
    const sqs$ = sendWagerMessages(infra, Array.from({ length: 30 }, () => wagerMessage(w, { money: money('5.00') })));
    const [rs] = await Promise.all([http$, sqs$]);
    expect(count(rs, 201)).toBe(30);

    await waitUntil(async () => (await ledgerState(infra, w.walletId)).entries === 61, 120_000, 300, '60 débitos aplicados');
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '200.00', entries: 61, consistent: true });
  });
});

describe('3 processos, PENDING_REFERENCE e outbox', () => {
  test('20 reversões chegam antes das apostas (em processos diferentes): cada uma é aplicada exatamente uma vez', async () => {
    const w = await createWallet(pick(0), '1000.00');
    const bets = Array.from({ length: 20 }, () => txBody(w, { money: money('10.00') }));
    const refunds = bets.map((b) =>
      txBody(w, { kind: 'REFUND', money: money('10.00'), referenceExternalTransactionId: b.externalTransactionId }),
    );
    const resolvedBefore = await sumMetric(cluster, 'pending_reference_total{outcome="resolved"}');

    const pending = await Promise.all(refunds.map((r, i) => submit(pick(i), r)));
    expect(pending.every((p) => p.status === 202 && p.json.status === 'PENDING_REFERENCE')).toBe(true);
    await Promise.all(bets.map((b, i) => submit(pick(i + 1), b)));

    await waitUntil(
      async () => {
        const [r] = await infra.query<{ n: string }>(
          `SELECT count(*)::text n FROM wager_transactions WHERE wallet_id = '${w.walletId}' AND kind = 'REFUND' AND status = 'PROCESSED'`,
        );
        return r!.n === '20';
      },
      120_000,
      300,
      '20 reversões resolvidas',
    );
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '1000.00', entries: 41, consistent: true });
    // soma dos 3 workers == 20: nenhuma reversão foi aplicada por dois processos
    expect((await sumMetric(cluster, 'pending_reference_total{outcome="resolved"}')) - resolvedBefore).toBe(20);
  });

  test('3 publishers: cada evento da outbox é publicado exatamente uma vez e todos chegam à fila de eventos', async () => {
    await waitUntil(
      async () => {
        const [r] = await infra.query<{ n: string }>(`SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL`);
        return r!.n === '0';
      },
      120_000,
      500,
      'outbox esvaziada',
    );
    const rows = await infra.query<{ id: string }>(`SELECT id FROM outbox_messages`);
    expect(rows.length).toBeGreaterThan(200);

    // somando os 3 processos, o nº de publicações == nº de linhas: nenhum evento saiu duas vezes nem ficou para trás
    expect(await sumMetric(cluster, 'outbox_published_total')).toBe(rows.length);
    const perInstance = await Promise.all(cluster.map((i) => sumMetric([i], 'outbox_published_total')));
    expect(perInstance.filter((n) => n > 0).length).toBeGreaterThanOrEqual(2);

    const delivered = await drainEvents(infra);
    expect(new Set(delivered)).toEqual(new Set(rows.map((r) => r.id))); // o conjunto entregue é exatamente o gravado
  });
});

describe('invariantes globais após toda a carga', () => {
  test('nenhuma wallet do banco diverge do próprio ledger', async () => {
    expect(await divergentWallets(infra)).toBe(0);
    const [t] = await infra.query<{ n: string }>(`SELECT count(*)::text n FROM wallets`);
    expect(Number(t!.n)).toBe(27); // 1 + 10 + 1 + 1 + 12 + 1 + 1 criadas nos testes acima
  });

  test('a reconciliação (endpoint) confirma, em qualquer processo, as wallets mais disputadas', async () => {
    const hot = await infra.query<{ id: string }>(
      `SELECT wallet_id AS id FROM wallet_ledger_entries GROUP BY wallet_id ORDER BY count(*) DESC LIMIT 3`,
    );
    for (const [i, h] of hot.entries()) {
      const r = await http(pick(i), 'POST', `/wallets/${h.id}/reconciliation`);
      expect(r.status).toBe(200);
      expect(r.json).toMatchObject({ consistent: true, difference: money('0.00') });
    }
  });

  test('todos os logs dos 3 processos são JSON válido, trazem level/time/msg e não vazam valores monetários', async () => {
    const lines = cluster.flatMap((i) => i.logs);
    expect(lines.length).toBeGreaterThan(100);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, any>);
    expect(parsed.every((l) => typeof l.level === 'string' && typeof l.time === 'string' && typeof l.msg === 'string')).toBe(true);
    expect(new Set(parsed.map((l) => l.instance))).toEqual(new Set(['p1', 'p2', 'p3']));
    const text = lines.join('\n');
    expect(text).not.toContain('"amount"');
    expect(parsed.filter((l) => l.level === 'error' || l.level === 'fatal')).toEqual([]); // nenhum erro em toda a carga
  });
});

describe('encerramento gracioso (SIGTERM)', () => {
  // No Windows o SIGTERM vira terminação forçada; o caminho gracioso é coberto nos testes em processo
  // (consumer.stop / workers.stop). Em Linux/macOS este teste exercita o handler de verdade.
  test.skipIf(process.platform === 'win32')('SIGTERM: o processo conclui o que tem em andamento e sai com código 0', async () => {
    const extra = await startInstance(infra, 'p-term');
    const code = await Promise.race([
      extra.terminate(),
      new Promise<number>((_, rej) => setTimeout(() => rej(new Error('did not exit within 30s')), 30_000)),
    ]);
    expect(code).toBe(0);
    expect(extra.logs.some((l) => l.includes('shutting down'))).toBe(true);
  });
});
