import { randomUUID } from 'node:crypto';
import {
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  type SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { type WagerQueues, wagerQueues } from '@infra/messaging/sqs';
import type { Fixture } from './app';
import { sleep } from './outbox-fakes';

export interface QueueEnv {
  queues: WagerQueues;
  send(body: unknown, opts?: { groupId?: string; dedupId?: string; raw?: boolean }): Promise<void>;
  /** Mensagens existentes na fila principal (visíveis + em voo + com atraso). */
  depth(): Promise<{ visible: number; inFlight: number; total: number }>;
  dlqDepth(): Promise<number>;
  /** Lê (e apaga) tudo que estiver na DLQ. */
  drainDlq(): Promise<{ body: string; attrs: Record<string, string | undefined> }[]>;
}

/** Filas isoladas por teste (nome aleatório), com visibilidade curta para acelerar os redeliveries. */
export function queueEnv(client: SQSClient, maxReceiveCount = 3, visibilityTimeoutSeconds = 2): QueueEnv {
  const suffix = randomUUID().slice(0, 8);
  const queues = wagerQueues(
    client,
    { main: `wager-transactions-${suffix}.fifo`, dlq: `wager-transactions-dlq-${suffix}.fifo` },
    { maxReceiveCount, visibilityTimeoutSeconds },
  );

  const counts = async (resolver: WagerQueues['main']) => {
    const url = await resolver.get();
    const r = await client.send(
      new GetQueueAttributesCommand({
        QueueUrl: url,
        AttributeNames: [
          'ApproximateNumberOfMessages',
          'ApproximateNumberOfMessagesNotVisible',
          'ApproximateNumberOfMessagesDelayed',
        ],
      }),
    );
    const a = r.Attributes ?? {};
    const visible = Number(a.ApproximateNumberOfMessages ?? 0) + Number(a.ApproximateNumberOfMessagesDelayed ?? 0);
    const inFlight = Number(a.ApproximateNumberOfMessagesNotVisible ?? 0);
    return { visible, inFlight, total: visible + inFlight };
  };

  return {
    queues,
    async send(body, opts = {}) {
      const payload = opts.raw ? (body as string) : JSON.stringify(body);
      let groupId = opts.groupId;
      if (!groupId && !opts.raw) {
        groupId = (body as { data?: { walletId?: string } })?.data?.walletId;
      }
      await client.send(
        new SendMessageCommand({
          QueueUrl: await queues.main.get(),
          MessageBody: payload,
          MessageGroupId: groupId ?? 'default',
          MessageDeduplicationId: opts.dedupId ?? randomUUID(),
        }),
      );
    },
    depth: () => counts(queues.main),
    dlqDepth: async () => (await counts(queues.dlq)).total,
    async drainDlq() {
      const out: { body: string; attrs: Record<string, string | undefined> }[] = [];
      const url = await queues.dlq.get();
      for (let i = 0; i < 5; i++) {
        const r = await client.send(
          new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'] }),
        );
        for (const m of r.Messages ?? []) {
          out.push({
            body: m.Body ?? '',
            attrs: Object.fromEntries(Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue])),
          });
        }
        if (!r.Messages?.length && out.length) break;
      }
      return out;
    },
  };
}

let seq = 0;
/** Mensagem `WagerTransactionRequested` no formato do enunciado do desafio. */
export function wagerMessage(f: Fixture, over: Record<string, unknown> = {}, dataOver: Record<string, unknown> = {}) {
  seq += 1;
  const ext = `sqs-${seq}-${randomUUID().slice(0, 8)}`;
  return {
    messageId: `msg-${randomUUID()}`,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: {
      providerId: 'provider-a',
      externalTransactionId: ext,
      idempotencyKey: `provider-a:${ext}`,
      playerId: f.playerId,
      walletId: f.walletId,
      roundId: 'round-987',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
      ...dataOver,
    },
    ...over,
  };
}

export async function waitFor(cond: () => Promise<boolean>, timeoutMs = 30_000, stepMs = 150): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(stepMs);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

/** Espera a fila principal esvaziar (nada visível nem em voo) de forma estável. */
export async function waitDrained(env: QueueEnv, timeoutMs = 40_000): Promise<void> {
  let stable = 0;
  await waitFor(async () => {
    stable = (await env.depth()).total === 0 ? stable + 1 : 0;
    return stable >= 3;
  }, timeoutMs);
}
