/**
 * Envia uma mensagem `WagerTransactionRequested` (formato do enunciado do desafio) para a fila de entrada.
 * Serve para exercitar o consumer sem precisar de AWS CLI e sem brigar com aspas de shell.
 *
 *   bun scripts/send-wager-message.ts <walletId> <playerId> [kind=BET] [amount=10.00] [--times=1] [--ref=<externalId>]
 *
 * `--times=N` entrega a MESMA mensagem N vezes (mesmo messageId): demonstra a deduplicação pela inbox.
 * Variáveis (iguais às do app): AWS_ENDPOINT_URL (padrão http://localhost:4566), SQS_QUEUE_NAME, AWS_REGION.
 */
import { GetQueueUrlCommand, SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';

const positional = process.argv.slice(2).filter((a) => !a.startsWith('--'));
const flags = Object.fromEntries(
  process.argv
    .slice(2)
    .filter((a) => a.startsWith('--'))
    .map((a) => a.slice(2).split('=') as [string, string]),
);
const [walletId, playerId, kind = 'BET', amount = '10.00'] = positional;

if (!walletId || !playerId) {
  console.error('uso: bun scripts/send-wager-message.ts <walletId> <playerId> [kind=BET] [amount=10.00] [--times=1] [--ref=<externalId>]');
  process.exit(2);
}

const queueName = process.env.SQS_QUEUE_NAME ?? 'wager-transactions.fifo';
const client = new SQSClient({
  region: process.env.AWS_REGION ?? 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
  credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
});

const { QueueUrl } = await client.send(new GetQueueUrlCommand({ QueueName: queueName }));
const externalTransactionId = `demo-${randomUUID().slice(0, 8)}`;
const message = {
  messageId: `msg-${randomUUID()}`,
  type: 'WagerTransactionRequested',
  occurredAt: new Date().toISOString(),
  data: {
    providerId: 'provider-a',
    externalTransactionId,
    idempotencyKey: `provider-a:${externalTransactionId}`,
    playerId,
    walletId,
    roundId: 'round-987',
    gameId: 'fortune-chimp',
    kind,
    money: { amount, currency: 'BRL' },
    ...(flags.ref ? { referenceExternalTransactionId: flags.ref } : {}),
  },
};

const times = Number(flags.times ?? 1);
for (let i = 0; i < times; i++) {
  await client.send(
    new SendMessageCommand({
      QueueUrl,
      MessageBody: JSON.stringify(message),
      MessageGroupId: walletId, // ordem por wallet
      MessageDeduplicationId: randomUUID(), // entregas distintas para o FIFO (a dedup que importa é a da inbox)
    }),
  );
}
console.log(JSON.stringify({ sent: times, queue: queueName, messageId: message.messageId, externalTransactionId, kind, amount }));
client.destroy();
