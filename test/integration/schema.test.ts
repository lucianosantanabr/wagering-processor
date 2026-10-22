import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { type TestDb, startTestDb } from './support/postgres';

let db: TestDb;
// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  db = await startTestDb();
}, 180_000);
afterAll(async () => {
  await db?.stop();
}, 60_000);

const NOW = '2026-08-01T12:00:00Z';

/** Executa vários statements como UMA transação (protocolo simples do PG): constraints deferidas rodam no commit. */
const tx = (...statements: string[]) => db.query(statements.join(';\n'));

const insertWallet = (id: string, balance: string, opts: { player?: string; currency?: string } = {}) =>
  `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
   VALUES ('${id}', '${opts.player ?? randomUUID()}', '${opts.currency ?? 'BRL'}', ${balance}, 1, '${NOW}', '${NOW}')`;

const insertTx = (
  id: string,
  walletId: string,
  o: Partial<{
    kind: string; amount: string; status: string; extId: string; key: string; provider: string;
    failure: string | null; processed: boolean; refExt: string | null; refId: string | null; player: string;
  }> = {},
) => {
  const status = o.status ?? 'PROCESSED';
  const processed = o.processed ?? status === 'PROCESSED';
  const ext = o.extId ?? `ext-${id}`;
  const failure =
    'failure' in o ? o.failure : status === 'REJECTED' || status === 'FAILED' ? 'INSUFFICIENT_FUNDS' : null;
  return `INSERT INTO wager_transactions
    (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
     round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id,
     status, failure_code, balance_snapshot, created_at, processed_at)
   VALUES ('${id}', '${o.provider ?? 'provider-a'}', '${ext}', '${o.key ?? `provider-a:${ext}`}', '${'a'.repeat(64)}',
     '${walletId}', '${o.player ?? randomUUID()}', 'round-1', 'game-1', '${o.kind ?? 'BET'}', ${o.amount ?? '10.00'}, 'BRL',
     ${o.refExt ? `'${o.refExt}'` : 'NULL'}, ${o.refId ? `'${o.refId}'` : 'NULL'},
     '${status}', ${failure ? `'${failure}'` : 'NULL'}, 0.00, '${NOW}', ${processed ? `'${NOW}'` : 'NULL'})`;
};

const insertLedger = (
  walletId: string,
  txId: string,
  direction: 'CREDIT' | 'DEBIT',
  amount: string,
  before: string,
  after: string,
) =>
  `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
   VALUES ('${randomUUID()}', '${walletId}', '${txId}', '${direction}', ${amount}, 'BRL', ${before}, ${after}, '${NOW}')`;

/** Wallet 100.00 válida, com transação OPENING e lançamento de abertura. Devolve ids. */
async function seedWallet(balance = '100.00') {
  const walletId = randomUUID();
  const openingTx = randomUUID();
  await tx(
    insertWallet(walletId, balance),
    insertTx(openingTx, walletId, { kind: 'OPENING', provider: 'internal', amount: balance }),
    insertLedger(walletId, openingTx, 'CREDIT', balance, '0.00', balance),
  );
  return { walletId, openingTx };
}

const expectDbError = async (p: Promise<unknown>, pattern: RegExp) => {
  const err = await p.then(
    () => undefined,
    (e: Error) => e,
  );
  expect(err).toBeDefined();
  expect(String(err?.message)).toMatch(pattern);
};

describe('migrations', () => {
  test('up criou as tabelas esperadas', async () => {
    const rows = await db.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`,
    );
    const names = rows.map((r) => r.table_name);
    for (const t of ['wallets', 'wager_transactions', 'wallet_ledger_entries', 'inbox_messages', 'outbox_messages']) {
      expect(names).toContain(t);
    }
  });
});

describe('wallets', () => {
  test('saldo negativo é rejeitado pelo CHECK', async () => {
    await expectDbError(tx(insertWallet(randomUUID(), '-0.01')), /wallets_balance_check|check constraint/i);
  });

  test('uma wallet por (player, currency)', async () => {
    const player = randomUUID();
    await tx(insertWallet(randomUUID(), '0.00', { player }));
    await expectDbError(tx(insertWallet(randomUUID(), '0.00', { player })), /wallets_player_currency_key|unique/i);
    await tx(insertWallet(randomUUID(), '0.00', { player, currency: 'USD' })); // outra moeda é ok
  });

  test('version só sobe (+1) quando o saldo muda', async () => {
    const { walletId } = await seedWallet('100.00');
    await expectDbError(
      db.query(`UPDATE wallets SET version = 2 WHERE id = '${walletId}'`),
      /version must not change/i,
    );
    await expectDbError(
      db.query(`UPDATE wallets SET balance = 50.00, version = 5 WHERE id = '${walletId}'`),
      /increase by exactly 1/i,
    );
  });

  test('wallet não pode ser excluída nem ter identidade alterada', async () => {
    const { walletId } = await seedWallet();
    await expectDbError(db.query(`DELETE FROM wallets WHERE id = '${walletId}'`), /cannot be deleted/i);
    await expectDbError(
      db.query(`UPDATE wallets SET player_id = '${randomUUID()}' WHERE id = '${walletId}'`),
      /immutable/i,
    );
  });
});

describe('saldo materializado == ledger (verificado no commit)', () => {
  test('wallet com saldo > 0 sem lançamento correspondente não comita', async () => {
    await expectDbError(tx(insertWallet(randomUUID(), '50.00')), /does not match ledger/i);
  });

  test('alterar saldo sem lançamento não comita', async () => {
    const { walletId } = await seedWallet('100.00');
    await expectDbError(
      db.query(`UPDATE wallets SET balance = 60.00, version = 2 WHERE id = '${walletId}'`),
      /does not match ledger/i,
    );
    const [w] = await db.query<{ balance: string }>(`SELECT balance FROM wallets WHERE id = '${walletId}'`);
    expect(w?.balance).toBe('100.00'); // transação inteira foi revertida
  });

  test('saldo + lançamento na mesma transação comitam juntos', async () => {
    const { walletId } = await seedWallet('100.00');
    const betTx = randomUUID();
    await tx(
      insertTx(betTx, walletId, { kind: 'BET', amount: '30.00' }),
      `UPDATE wallets SET balance = 70.00, version = 2 WHERE id = '${walletId}'`,
      insertLedger(walletId, betTx, 'DEBIT', '30.00', '100.00', '70.00'),
    );
    const [w] = await db.query<{ balance: string; version: number }>(
      `SELECT balance, version FROM wallets WHERE id = '${walletId}'`,
    );
    expect(w).toEqual({ balance: '70.00', version: 2 });
  });
});

describe('wallet_ledger_entries', () => {
  test('é append-only: UPDATE e DELETE são bloqueados', async () => {
    const { walletId } = await seedWallet();
    await expectDbError(
      db.query(`UPDATE wallet_ledger_entries SET amount = 1.00 WHERE wallet_id = '${walletId}'`),
      /append-only/i,
    );
    await expectDbError(
      db.query(`DELETE FROM wallet_ledger_entries WHERE wallet_id = '${walletId}'`),
      /append-only/i,
    );
    await expectDbError(db.query(`TRUNCATE wallet_ledger_entries`), /append-only/i);
  });

  test('aritmética inconsistente é rejeitada pelo CHECK', async () => {
    const { walletId } = await seedWallet('100.00');
    const betTx = randomUUID();
    await expectDbError(
      tx(
        insertTx(betTx, walletId, { kind: 'BET', amount: '30.00' }),
        insertLedger(walletId, betTx, 'DEBIT', '30.00', '100.00', '60.00'),
      ),
      /wallet_ledger_entries_arithmetic|check constraint/i,
    );
  });

  test('no máximo um lançamento por (wallet, transação)', async () => {
    const { walletId, openingTx } = await seedWallet('100.00');
    await expectDbError(
      tx(insertLedger(walletId, openingTx, 'CREDIT', '10.00', '100.00', '110.00')),
      /wallet_ledger_entries_wallet_tx_key|unique/i,
    );
  });

  test('encadeamento: balance_before precisa ser o último balance_after', async () => {
    const { walletId } = await seedWallet('100.00');
    const winTx = randomUUID();
    await expectDbError(
      tx(
        insertTx(winTx, walletId, { kind: 'WIN', amount: '10.00' }),
        insertLedger(walletId, winTx, 'CREDIT', '10.00', '90.00', '100.00'),
      ),
      /ledger chain broken/i,
    );
  });

  test('lançamento não pode apontar para transação de outra wallet', async () => {
    const a = await seedWallet('100.00');
    const b = await seedWallet('100.00');
    await expectDbError(
      tx(insertLedger(b.walletId, a.openingTx, 'CREDIT', '5.00', '100.00', '105.00')),
      /wallet_ledger_entries_transaction_fk|foreign key/i,
    );
  });
});

describe('wager_transactions', () => {
  test('idempotency_key e (provider, external id) são únicos', async () => {
    const { walletId } = await seedWallet();
    await tx(insertTx(randomUUID(), walletId, { status: 'REJECTED', extId: 'e1', key: 'k1' }));
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { status: 'REJECTED', extId: 'e2', key: 'k1' })),
      /idempotency_key_key|unique/i,
    );
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { status: 'REJECTED', extId: 'e1', key: 'k-other' })),
      /provider_external_key|unique/i,
    );
    // outro provedor pode reutilizar o mesmo external id
    await tx(insertTx(randomUUID(), walletId, { status: 'REJECTED', extId: 'e1', key: 'k-b', provider: 'provider-b' }));
  });

  test('REFUND/ROLLBACK exigem referência', async () => {
    const { walletId } = await seedWallet();
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { kind: 'REFUND', status: 'REJECTED' })),
      /reference_required/i,
    );
  });

  test('failure_code obrigatório em REJECTED/FAILED e proibido nos demais', async () => {
    const { walletId } = await seedWallet();
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { status: 'REJECTED', failure: null })),
      /failure_code_consistent/i,
    );
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { status: 'PENDING', failure: 'INSUFFICIENT_FUNDS', processed: false })),
      /failure_code_consistent/i,
    );
  });

  test('transação terminal é imutável', async () => {
    const { walletId } = await seedWallet();
    const id = randomUUID();
    await tx(insertTx(id, walletId, { status: 'REJECTED' }));
    await expectDbError(
      db.query(`UPDATE wager_transactions SET failure_code = 'OTHER' WHERE id = '${id}'`),
      /terminal status/i,
    );
    await expectDbError(db.query(`DELETE FROM wager_transactions WHERE id = '${id}'`), /cannot be deleted/i);
  });

  test('colunas de negócio são imutáveis mesmo fora de estado terminal', async () => {
    const { walletId } = await seedWallet();
    const id = randomUUID();
    await tx(insertTx(id, walletId, { status: 'PENDING', processed: false, kind: 'REFUND', refExt: 'bet-1' }));
    await expectDbError(
      db.query(`UPDATE wager_transactions SET amount = 99.00 WHERE id = '${id}'`),
      /business columns are immutable/i,
    );
    // transição válida segue permitida
    await db.query(`UPDATE wager_transactions SET status = 'PENDING_REFERENCE' WHERE id = '${id}'`);
    await expectDbError(
      db.query(`UPDATE wager_transactions SET status = 'PENDING' WHERE id = '${id}'`),
      /PENDING_REFERENCE -> PENDING/i,
    );
  });

  test('uma referência só pode ser revertida uma vez', async () => {
    const { walletId } = await seedWallet('100.00');
    const bet = randomUUID();
    await tx(
      insertTx(bet, walletId, { kind: 'BET', amount: '10.00' }),
      `UPDATE wallets SET balance = 90.00, version = 2 WHERE id = '${walletId}'`,
      insertLedger(walletId, bet, 'DEBIT', '10.00', '100.00', '90.00'),
    );
    const refund1 = randomUUID();
    await tx(
      insertTx(refund1, walletId, { kind: 'REFUND', amount: '10.00', refExt: `ext-${bet}`, refId: bet }),
      `UPDATE wallets SET balance = 100.00, version = 3 WHERE id = '${walletId}'`,
      insertLedger(walletId, refund1, 'CREDIT', '10.00', '90.00', '100.00'),
    );
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { kind: 'REFUND', amount: '10.00', refExt: `ext-${bet}`, refId: bet })),
      /single_reversal_idx|unique/i,
    );
    // Interpretação adotada: a referência é revertida no máximo uma vez, por qualquer tipo
    // (REFUND *ou* ROLLBACK), para nunca creditar duas vezes o mesmo alvo.
    await expectDbError(
      tx(insertTx(randomUUID(), walletId, { kind: 'ROLLBACK', amount: '10.00', refExt: `ext-${bet}`, refId: bet })),
      /single_reversal_idx|unique/i,
    );
  });
});

describe('inbox e outbox', () => {
  test('inbox deduplica por (consumer_name, message_id)', async () => {
    const ins = `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
                 VALUES ('wager-consumer', 'msg-1', '${'b'.repeat(64)}', '${NOW}')`;
    await db.query(ins);
    await expectDbError(db.query(ins), /inbox_messages_pkey|unique|duplicate/i);
    await db.query(ins.replace("'wager-consumer'", "'other-consumer'")); // outro consumer é ok
  });

  test('outbox grava envelope jsonb e atribui seq crescente', async () => {
    const ins = (id: string) =>
      `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at)
       VALUES ('${id}', '${randomUUID()}', 'WagerTransactionProcessed', '{"version":1}'::jsonb, '${NOW}')`;
    const a = randomUUID();
    const b = randomUUID();
    await db.query(ins(a));
    await db.query(ins(b));
    const rows = await db.query<{ id: string; seq: string }>(
      `SELECT id, seq FROM outbox_messages WHERE id IN ('${a}','${b}') ORDER BY seq`,
    );
    expect(rows.map((r) => r.id)).toEqual([a, b]);
  });
});

describe('migrations reversíveis', () => {
  test('down remove tudo e up recria', async () => {
    await db.orm.migrator.down();
    const after = await db.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'mikro_orm_migrations'`,
    );
    expect(after).toEqual([]);
    await db.orm.migrator.up();
    const [r] = await db.query<{ n: string }>(`SELECT count(*)::text AS n FROM wallets`);
    expect(r?.n).toBe('0');
  });
});
