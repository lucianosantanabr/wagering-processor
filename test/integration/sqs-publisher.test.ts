import { setDefaultTimeout, afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { buildOutboxPublisher } from '@infra/composition';
import { ensureQueue, pingQueue, SqsEventPublisher } from '@infra/messaging/sqs';
import { type TestApp, cmd, newWallet, startApp } from './support/app';
import { type TestSqs, startLocalstack } from './support/localstack';

let app: TestApp;
let sqs: TestSqs;
let queueUrl: string;

// Testes de integracao esperam containers e backoff real.
setDefaultTimeout(120_000);

beforeAll(async () => {
  [app, sqs] = await Promise.all([startApp(), startLocalstack()]);
  queueUrl = await ensureQueue(sqs.client, 'wager-events.fifo');
}, 300_000);
afterAll(async () => {
  await Promise.all([app?.stop(), sqs?.stop()]);
}, 60_000);

async function receiveAll(max: number, waitMs = 8000) {
  const out: { body: any; attrs: Record<string, string | undefined>; groupId?: string }[] = [];
  const deadline = Date.now() + waitMs;
  while (out.length < max && Date.now() < deadline) {
    const r = await sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        MessageAttributeNames: ['All'],
        MessageSystemAttributeNames: ['MessageGroupId'],
      }),
    );
    for (const m of r.Messages ?? []) {
      out.push({
        body: JSON.parse(m.Body ?? '{}'),
        attrs: Object.fromEntries(Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue])),
        groupId: m.Attributes?.MessageGroupId,
      });
      await sqs.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
    }
  }
  return out;
}

describe('SqsEventPublisher (LocalStack real)', () => {
  test('ensureQueue é idempotente e cria fila FIFO', async () => {
    expect(await ensureQueue(sqs.client, 'wager-events.fifo')).toBe(queueUrl);
    const attrs = await sqs.client.send(new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['FifoQueue'] }));
    expect(attrs.Attributes?.FifoQueue).toBe('true');
    await pingQueue(sqs.client, queueUrl);
  });

  test('a outbox publica no SQS o envelope do evento, só depois do commit', async () => {
    const f = await newWallet(app, '1000.00');
    await app.submitWagerTransaction.execute(cmd(f, { money: { amount: '25.00', currency: 'BRL' } }));

    const pub = buildOutboxPublisher(app, new SqsEventPublisher(sqs.client, queueUrl));
    const round = await pub.runOnce();
    expect(round).toMatchObject({ claimed: 4, published: 4, failed: 0 });

    const got = await receiveAll(4);
    expect(got).toHaveLength(4);
    const byType = Object.groupBy(got, (m) => m.body.eventType as string);
    expect(Object.keys(byType).sort()).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);

    const changed = byType.WalletBalanceChanged!.find((m) => m.body.data.direction === 'DEBIT')!;
    expect(changed.body).toMatchObject({
      eventType: 'WalletBalanceChanged',
      version: 1,
      aggregateId: f.walletId,
      data: {
        walletId: f.walletId,
        money: { amount: '25.00', currency: 'BRL' },
        balanceBefore: { amount: '1000.00', currency: 'BRL' },
        balanceAfter: { amount: '975.00', currency: 'BRL' },
        walletVersion: 2,
      },
    });
    expect(changed.body.eventId).toBe(changed.attrs.eventId);
    expect(changed.attrs.eventType).toBe('WalletBalanceChanged');
    expect(changed.groupId).toBe(f.walletId); // ordem por agregado (otimização do broker)
    expect(typeof changed.body.occurredAt).toBe('string');
    expect(changed.body.correlationId).toBeDefined();
  });

  test('broker indisponível: a falha é registrada e a mensagem fica pendente (nada se perde)', async () => {
    await newWallet(app, '10.00');
    const downUrl = `${queueUrl}-missing`;
    const pub = buildOutboxPublisher(app, new SqsEventPublisher(sqs.client, downUrl));
    const round = await pub.runOnce();
    expect(round.failed).toBeGreaterThan(0);
    expect(round.published).toBe(0);
    const [r] = await app.db.query<{ n: string }>(
      `SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL AND attempts > 0`,
    );
    expect(Number(r?.n)).toBeGreaterThan(0);
  });
});
