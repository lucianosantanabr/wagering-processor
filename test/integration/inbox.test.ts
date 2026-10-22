import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { MessageIdReuseError } from '@application/errors';
import { InboxMessage } from '@domain/inbox-message';
import { WagerMessageHandler } from '@application/wager-message-handler';
import { buildServices } from '@infra/composition';
import { isTransientInfrastructureError } from '@infra/persistence/transient-errors';
import { type TestApp, cmd, expectLedgerConsistent, newWallet, startApp } from './support/app';
import { RecordingMetrics } from './support/outbox-fakes';
import { wagerMessage } from './support/sqs-env';

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

const CONSUMER = 'wager-transactions-consumer';
const metrics = new RecordingMetrics();
let app: TestApp;

beforeAll(async () => {
  app = await startApp({ metrics });
}, 180_000);
afterAll(async () => {
  await app?.stop();
}, 60_000);

const money = (amount: string) => ({ amount, currency: 'BRL' });
const hash = (c = 'a') => c.repeat(64);

const inboxRow = async (messageId: string) =>
  (
    await app.db.query<{ payload_hash: string; processed_at: string | null; n: string }>(
      `SELECT payload_hash, processed_at, count(*) OVER ()::text AS n FROM inbox_messages
        WHERE consumer_name = '${CONSUMER}' AND message_id = '${messageId}'`,
    )
  )[0];

const txCount = async (walletId: string) =>
  Number((await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM wager_transactions WHERE wallet_id = '${walletId}' AND kind <> 'OPENING'`))[0]!.n);

describe('InboxRepository.tryReceive', () => {
  test('1ª vez: nova. Depois: devolve o que já estava registrado (hash e processed_at), sem sobrescrever', async () => {
    const messageId = `msg-${randomUUID()}`;
    const first = await app.uow.run((r) =>
      r.inbox.tryReceive(InboxMessage.receive({ messageId, consumerName: CONSUMER, payloadHash: hash('a'), now: new Date() })),
    );
    expect(first).toEqual({ isNew: true });

    await app.uow.run((r) => r.inbox.markProcessed(CONSUMER, messageId, new Date('2026-01-01T00:00:00Z')));

    // outra entrega com hash DIFERENTE: o registro original é devolvido e permanece intacto
    const again = await app.uow.run((r) =>
      r.inbox.tryReceive(InboxMessage.receive({ messageId, consumerName: CONSUMER, payloadHash: hash('b'), now: new Date() })),
    );
    expect(again.isNew).toBe(false);
    expect(again.existing?.payloadHash).toBe(hash('a'));
    expect(again.existing?.processedAt?.toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect((await inboxRow(messageId))?.payload_hash).toBe(hash('a')); // não foi sobrescrito
  });

  test('o mesmo messageId em OUTRO consumer é independente', async () => {
    const messageId = `msg-${randomUUID()}`;
    const recv = (consumerName: string) =>
      app.uow.run((r) => r.inbox.tryReceive(InboxMessage.receive({ messageId, consumerName, payloadHash: hash(), now: new Date() })));
    expect((await recv('consumer-x')).isNew).toBe(true);
    expect((await recv('consumer-y')).isNew).toBe(true);
    expect((await recv('consumer-x')).isNew).toBe(false);
  });
});

describe('releitura da MESMA mensagem (mesmo messageId, mesmo corpo)', () => {
  test('a 2ª entrega devolve o resultado original sem novo efeito, e a inbox não muda', async () => {
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    const c = cmd(f, { money: money('30.00'), inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('c') } });

    const first = await app.submitWagerTransaction.execute(c);
    const processedAt = (await inboxRow(messageId))!.processed_at;
    expect(processedAt).not.toBeNull();

    const second = await app.submitWagerTransaction.execute(c);
    expect(first.idempotentReplay).toBe(false);
    expect(second).toEqual({ ...first, idempotentReplay: true });

    expect(await txCount(f.walletId)).toBe(1);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('70.00');
    const row = await inboxRow(messageId);
    expect(row).toMatchObject({ payload_hash: hash('c'), n: '1' }); // continua uma linha
    expect(row!.processed_at).toBe(processedAt); // processed_at original preservado
  });

  test('20 entregas simultâneas da mesma mensagem: um único efeito e todas respondem igual', async () => {
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    const c = cmd(f, { money: money('25.00'), inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('d') } });
    const results = await Promise.all(Array.from({ length: 20 }, () => app.submitWagerTransaction.execute(c)));

    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(await txCount(f.walletId)).toBe(1);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('75.00');
    expect((await inboxRow(messageId))?.n).toBe('1');
  });

  test('messageId NOVO com o mesmo pedido de negócio (mesma idempotency key): replay pela 2ª camada', async () => {
    const f = await newWallet(app, '100.00');
    const base = cmd(f, { money: money('10.00') });
    const m1 = `msg-${randomUUID()}`;
    const m2 = `msg-${randomUUID()}`;
    const a = await app.submitWagerTransaction.execute({ ...base, inbox: { consumerName: CONSUMER, messageId: m1, payloadHash: hash('e') } });
    const b = await app.submitWagerTransaction.execute({ ...base, inbox: { consumerName: CONSUMER, messageId: m2, payloadHash: hash('f') } });

    expect(b).toEqual({ ...a, idempotentReplay: true });
    expect(await txCount(f.walletId)).toBe(1);
    expect((await inboxRow(m1))?.n).toBe('1');
    expect((await inboxRow(m2))?.n).toBe('1'); // as duas mensagens ficam registradas
    expect((await inboxRow(m2))!.processed_at).not.toBeNull();
  });
});

describe('MESMO messageId com conteúdo DIFERENTE', () => {
  test('é erro permanente: nada é processado e o registro original da inbox permanece', async () => {
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    const original = cmd(f, { money: money('10.00'), inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('1') } });
    await app.submitWagerTransaction.execute(original);
    const before = await inboxRow(messageId);

    // outro pedido (outra key, outro valor) reutilizando o MESMO messageId
    const reused = cmd(f, { money: money('50.00'), inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('2') } });
    await expect(app.submitWagerTransaction.execute(reused)).rejects.toBeInstanceOf(MessageIdReuseError);

    expect(await txCount(f.walletId)).toBe(1); // o 2º pedido NÃO virou transação
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('90.00'); // só o débito de 10.00
    expect(await inboxRow(messageId)).toEqual(before); // hash e processed_at originais intactos
    const [ghost] = await app.db.query<{ n: string }>(`SELECT count(*)::text n FROM wager_transactions WHERE idempotency_key = '${reused.idempotencyKey}'`);
    expect(ghost!.n).toBe('0');
  });

  test('é contado em métrica própria', async () => {
    const f = await newWallet(app, '100.00');
    const messageId = `msg-${randomUUID()}`;
    await app.submitWagerTransaction.execute(cmd(f, { inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('3') } }));
    const before = metrics.counters.get('inbox_conflicts_total') ?? 0;
    await expect(
      app.submitWagerTransaction.execute(cmd(f, { inbox: { consumerName: CONSUMER, messageId, payloadHash: hash('4') } })),
    ).rejects.toBeInstanceOf(MessageIdReuseError);
    expect((metrics.counters.get('inbox_conflicts_total') ?? 0) - before).toBe(1);
  });
});

describe('WagerMessageHandler (decisão ack / DLQ)', () => {
  const handler = () =>
    new WagerMessageHandler(app.submitWagerTransaction, isTransientInfrastructureError, { maxReceiveCount: 3 });

  test('releitura -> ack como duplicata; messageId reutilizado com outro corpo -> DLQ permanente', async () => {
    const f = await newWallet(app, '100.00');
    const h = handler();
    const original = wagerMessage(f, {}, { money: money('20.00') });
    const body = JSON.stringify(original);

    const first = await h.handle(body, 1);
    expect(first).toMatchObject({ action: 'ack', outcome: 'processed' });

    const redelivery = await h.handle(body, 2);
    expect(redelivery).toMatchObject({ action: 'ack', outcome: 'duplicate' });

    // mesmo messageId, conteúdo diferente (outro valor e outra key)
    const tampered = JSON.stringify(wagerMessage(f, { messageId: original.messageId }, { money: money('99.00') }));
    const verdict = await h.handle(tampered, 1);
    expect(verdict).toMatchObject({ action: 'dead-letter', failureClass: 'permanent' });
    expect((verdict as { reason: string }).reason).toMatch(/messageId/i);

    expect(await txCount(f.walletId)).toBe(1);
    expect((await expectLedgerConsistent(app.db, f.walletId)).balance).toBe('80.00');
  });

  test('uma mensagem NOVA com outro messageId continua sendo processada normalmente no mesmo wallet', async () => {
    const f = await newWallet(app, '100.00');
    const h = handler();
    expect((await h.handle(JSON.stringify(wagerMessage(f)), 1)).action).toBe('ack');
    expect((await h.handle(JSON.stringify(wagerMessage(f)), 1)).action).toBe('ack');
    expect(await txCount(f.walletId)).toBe(2);
  });
});
