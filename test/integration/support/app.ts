import { expect } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { SubmitWagerTransactionCommand } from '@application/submit-wager-transaction';
import { type Services, buildServices } from '@infra/composition';
import { type TestDb, startTestDb } from './postgres';

export interface TestApp extends Services {
  db: TestDb;
  stop(): Promise<void>;
}

export async function startApp(overrides?: Parameters<typeof buildServices>[1]): Promise<TestApp> {
  const db = await startTestDb();
  const services = buildServices(db.orm, overrides);
  return { ...services, db, stop: () => db.stop() };
}

export interface Fixture {
  walletId: string;
  playerId: string;
}

export async function newWallet(app: Pick<TestApp, 'createWallet'>, balance = '100.00', currency = 'BRL'): Promise<Fixture> {
  const playerId = randomUUID();
  const w = await app.createWallet.execute({ playerId, initialBalance: { amount: balance, currency } });
  return { walletId: w.id, playerId };
}

let counter = 0;
/** Comando válido de aposta/operação; sobrescreva o que o cenário precisar. */
export function cmd(f: Fixture, over: Partial<SubmitWagerTransactionCommand> = {}): SubmitWagerTransactionCommand {
  counter += 1;
  const ext = over.externalTransactionId ?? `ext-${counter}-${randomUUID().slice(0, 8)}`;
  return {
    providerId: 'provider-a',
    externalTransactionId: ext,
    idempotencyKey: `provider-a:${ext}`,
    playerId: f.playerId,
    walletId: f.walletId,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    ...over,
  };
}

/** Invariante final de todos os testes: saldo da wallet == saldo reconstruído pelo ledger. */
export async function expectLedgerConsistent(db: TestDb, walletId: string): Promise<{ balance: string; entries: number }> {
  const [w] = await db.query<{ balance: string; version: number }>(
    `SELECT balance::text AS balance, version FROM wallets WHERE id = '${walletId}'`,
  );
  const [l] = await db.query<{ rebuilt: string; entries: string }>(
    `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::text AS rebuilt,
            COUNT(*)::text AS entries
       FROM wallet_ledger_entries WHERE wallet_id = '${walletId}'`,
  );
  expect(Number(w?.balance)).toBeCloseTo(Number(l?.rebuilt), 2);
  expect(w?.balance).toBe(Number(l?.rebuilt).toFixed(2));
  // version = 1 (abertura) + número de movimentos além do lançamento de abertura
  expect(w?.version).toBe(Number(l?.entries));
  return { balance: w?.balance ?? '', entries: Number(l?.entries) };
}

export async function outboxTypes(db: TestDb, walletId: string): Promise<string[]> {
  const rows = await db.query<{ event_type: string }>(
    `SELECT event_type FROM outbox_messages
      WHERE payload->'data'->>'walletId' = '${walletId}' ORDER BY seq`,
  );
  return rows.map((r) => r.event_type);
}
