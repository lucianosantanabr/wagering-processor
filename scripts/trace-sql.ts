/**
 * Mostra, na prática, o SQL que cada operação executa (log de queries do MikroORM ligado).
 * Sobe um PostgreSQL real em container (podman/docker), aplica as migrations e roda:
 *   BET -> REFUND (aceito) -> REFUND repetido (replay) -> outro REFUND da mesma BET -> REFUND sem referência
 *
 *   bun scripts/trace-sql.ts
 *
 * Os ids aparecem abreviados: <WALLET>, <T1> (a BET), <T2> (o REFUND) etc.
 */
import { randomUUID } from 'node:crypto';
import { MikroORM } from '@mikro-orm/postgresql';
import type { SubmitWagerTransactionCommand } from '@application/submit-wager-transaction';
import { buildServices } from '@infra/composition';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { removeContainer, runContainer } from '../test/integration/support/container';

const MAX_LEN = Number(process.env.TRACE_MAX_LEN ?? 250);
const captured: string[] = [];
const aliases = new Map<string, string>();

// ── formatação ──
const stripAnsi = (s: string) => s.replace(/\u001b\[[0-9;]*m/g, '');
function pretty(raw: string): string {
  let s = stripAnsi(raw).replace(/^\[query\]\s*/, '').replace(/\s+/g, ' ').trim();
  for (const [id, label] of aliases) s = s.split(id).join(label);
  s = s.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, (u) => `${u.slice(0, 8)}…`);
  s = s.replace(/\[took (\d+) ms(?:, (\d+) results?)?(?:, (\d+) rows? affected)?\]/, (m) => m.replace('[took ', '[').replace(' ms', 'ms'));
  return s.length > MAX_LEN ? `${s.slice(0, MAX_LEN)}…` : s;
}
const flush = (): string[] => captured.splice(0).map(pretty);

function print(title: string, note: string): void {
  const lines = flush();
  console.log(`\n${'═'.repeat(100)}\n${title}\n${note}\n${'─'.repeat(100)}`);
  lines.forEach((l, i) => console.log(`${String(i + 1).padStart(2)}. ${l}`));
  console.log(`    (${lines.length} comandos SQL)`);
}

async function main(): Promise<void> {
  const pg = await runContainer({
    image: 'postgres:16-alpine',
    port: 5432,
    env: { POSTGRES_USER: 'wagering', POSTGRES_PASSWORD: 'wagering', POSTGRES_DB: 'wagering' },
    readyCommand: ['pg_isready', '-U', 'wagering', '-d', 'wagering'],
  });
  const url = `postgresql://wagering:wagering@127.0.0.1:${pg.hostPort}/wagering`;

  let orm: MikroORM | undefined;
  try {
    // A imagem do postgres sobe um servidor TEMPORARIO para o initdb e reinicia em seguida; o pg_isready ja responde
    // na fase temporaria. Esperar evita conectar nela e tomar um ECONNRESET quando ele reinicia.
    await new Promise((r) => setTimeout(r, 4000));
    for (let i = 0; i < 40 && !orm; i++) {
      let candidate: MikroORM | undefined;
      try {
        candidate = await MikroORM.init({
          ...buildOrmConfig(url, { logWriter: (m) => void captured.push(m) }),
          // <- liga o log de SQL (+ os valores dos parametros, desligue com TRACE_PARAMS=0)
          debug: process.env.TRACE_PARAMS === '0' ? ['query'] : ['query', 'query-params'],
        });
        await candidate.em.execute('select 1');
        orm = candidate;
      } catch {
        await candidate?.close(true).catch(() => undefined);
        await new Promise((r) => setTimeout(r, 500));
      }
    }
    if (!orm) throw new Error('postgres indisponivel');
    await orm.migrator.up();
    const svc = buildServices(orm);

    // ── cenário: wallet com 1000,00 ──
    const playerId = randomUUID();
    const wallet = await svc.createWallet.execute({ playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } });
    aliases.set(wallet.id, '<WALLET>');
    aliases.set(playerId, '<PLAYER>');
    flush(); // descarta o ruído de migrations e da criação da wallet

    const base = { providerId: 'provider-a', playerId, walletId: wallet.id, roundId: 'round-987', gameId: 'fortune-chimp' };
    const money = (amount: string) => ({ amount, currency: 'BRL' });
    const bet: SubmitWagerTransactionCommand = { ...base, externalTransactionId: 'transaction-123', idempotencyKey: 'provider-a:transaction-123', kind: 'BET', money: money('25.00') };
    const refund: SubmitWagerTransactionCommand = { ...base, externalTransactionId: 'refund-456', idempotencyKey: 'provider-a:refund-456', kind: 'REFUND', money: money('25.00'), referenceExternalTransactionId: 'transaction-123' };

    const r1 = await svc.submitWagerTransaction.execute(bet);
    aliases.set(r1.transactionId, '<T1>');
    print('1) BET 25,00 (transaction-123)', `resultado: ${r1.status}, saldo ${r1.balance.amount}`);

    const r2 = await svc.submitWagerTransaction.execute(refund);
    aliases.set(r2.transactionId, '<T2>');
    print('2) REFUND 25,00 de transaction-123 (ACEITO)', `resultado: ${r2.status}, saldo ${r2.balance.amount}, replay=${r2.idempotentReplay}`);

    // estado final das tabelas (consulta de verificacao; nao faz parte do fluxo)
    const q = (sql: string) => orm!.em.execute(sql) as Promise<Record<string, unknown>[]>;
    const show = async (title: string, sql: string) => {
      const rows = await q(sql);
      flush();
      console.log(`\n  ${title}`);
      for (const r of rows) console.log('   ' + pretty(Object.entries(r).map(([k, v]) => `${k}=${v instanceof Date ? v.toISOString().slice(11, 23) : String(v)}`).join('  ')));
    };
    console.log(`\n${'═'.repeat(100)}\nESTADO DAS TABELAS DEPOIS DO REFUND\n${'─'.repeat(100)}`);
    await show('wallets', `select balance::text, version from wallets where id = '${wallet.id}'`);
    await show('wager_transactions', `select kind, status, amount::text, external_transaction_id, reference_external_transaction_id as ref_ext, reference_transaction_id as ref_id, balance_snapshot::text as snapshot from wager_transactions where wallet_id = '${wallet.id}' order by created_at, id`);
    await show('wallet_ledger_entries', `select seq::text, direction, amount::text, balance_before::text as before, balance_after::text as after, transaction_id from wallet_ledger_entries where wallet_id = '${wallet.id}' order by seq`);
    await show('outbox_messages', `select event_type, published_at from outbox_messages where payload->'data'->>'walletId' = '${wallet.id}' order by seq`);
    flush();

    const r3 = await svc.submitWagerTransaction.execute(refund);
    print('3) o MESMO REFUND de novo (replay)', `resultado: ${r3.status}, saldo ${r3.balance.amount}, replay=${r3.idempotentReplay}  -> nenhum INSERT/UPDATE`);

    const r4 = await svc.submitWagerTransaction.execute({ ...refund, externalTransactionId: 'refund-457', idempotencyKey: 'provider-a:refund-457' });
    print('4) OUTRO REFUND da MESMA BET (rejeitado)', `resultado: ${r4.status} ${r4.failureCode}, saldo ${r4.balance.amount}  -> grava a rejeicao, sem ledger`);

    const r5 = await svc.submitWagerTransaction.execute({ ...refund, externalTransactionId: 'refund-789', idempotencyKey: 'provider-a:refund-789', referenceExternalTransactionId: 'transaction-999' });
    print('5) REFUND de uma BET que NAO existe (fora de ordem)', `resultado: ${r5.status}, saldo ${r5.balance.amount}  -> pendente, sem ledger`);
  } finally {
    if (orm) await orm.close(true);
    await removeContainer(pg.id);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
