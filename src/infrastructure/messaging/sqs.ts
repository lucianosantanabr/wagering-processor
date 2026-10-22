import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  QueueDoesNotExist,
  SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import type { EventPublisher } from '@application/ports';
import type { OutboxMessage } from '@domain/outbox-message';

export interface SqsConfig {
  region: string;
  /** LocalStack/MiniStack em dev e testes; ausente na AWS real. */
  endpoint?: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export function sqsConfigFromEnv(env: NodeJS.ProcessEnv = process.env): SqsConfig {
  return {
    region: env.AWS_REGION ?? 'us-east-1',
    endpoint: env.AWS_ENDPOINT_URL,
    accessKeyId: env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? 'test',
  };
}

export function createSqsClient(config: SqsConfig): SQSClient {
  return new SQSClient({
    region: config.region,
    endpoint: config.endpoint,
    credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
    maxAttempts: 2,
  });
}

/** Resolve a URL da fila; cria (idempotente) se não existir. Em produção as filas vêm da infraestrutura. */
export async function ensureQueue(
  client: SQSClient,
  name: string,
  attributes: Record<string, string> = {},
): Promise<string> {
  try {
    const found = await client.send(new GetQueueUrlCommand({ QueueName: name }));
    if (found.QueueUrl) return found.QueueUrl;
  } catch (e) {
    if (!(e instanceof QueueDoesNotExist) && (e as { name?: string }).name !== 'AWS.SimpleQueueService.NonExistentQueue') {
      throw e;
    }
  }
  const created = await client.send(
    new CreateQueueCommand({
      QueueName: name,
      Attributes: { ...(name.endsWith('.fifo') ? { FifoQueue: 'true' } : {}), ...attributes },
    }),
  );
  if (!created.QueueUrl) throw new Error(`could not create queue ${name}`);
  return created.QueueUrl;
}

/**
 * Resolve a URL da fila sob demanda e a guarda em cache; se o SQS estiver fora do ar, NÃO cacheia a
 * falha: a próxima chamada tenta de novo. Assim o processo sobe mesmo com o broker indisponível.
 */
export class QueueResolver {
  private cached?: Promise<string>;

  constructor(
    private readonly client: SQSClient,
    readonly queueName: string,
    /** Atributos fixos ou calculados na criação (ex.: RedrivePolicy depende do ARN da DLQ). */
    private readonly attributes:
      | Record<string, string>
      | (() => Promise<Record<string, string>>) = {},
  ) {}

  get(): Promise<string> {
    if (!this.cached) {
      this.cached = (async () =>
        ensureQueue(
          this.client,
          this.queueName,
          typeof this.attributes === 'function' ? await this.attributes() : this.attributes,
        ))().catch((e) => {
        this.cached = undefined;
        throw e;
      });
    }
    return this.cached;
  }
}

export interface WagerQueueNames {
  main: string;
  dlq: string;
}

export interface WagerQueues {
  main: QueueResolver;
  dlq: QueueResolver;
}

/**
 * Filas de entrada: `main` com redrive para `dlq`. O corte por tentativas é feito explicitamente pelo
 * consumer; o `RedrivePolicy` do broker (maxReceiveCount + 1) é só uma rede de segurança para quando o
 * consumer morre antes de decidir.
 */
export function wagerQueues(
  client: SQSClient,
  names: WagerQueueNames,
  options: { maxReceiveCount: number; visibilityTimeoutSeconds?: number },
): WagerQueues {
  const dlq = new QueueResolver(client, names.dlq);
  const main = new QueueResolver(client, names.main, async () => {
    const dlqUrl = await dlq.get();
    const attrs = await client.send(new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }));
    return {
      VisibilityTimeout: String(options.visibilityTimeoutSeconds ?? 30),
      RedrivePolicy: JSON.stringify({
        deadLetterTargetArn: attrs.Attributes?.QueueArn,
        maxReceiveCount: options.maxReceiveCount + 1,
      }),
    };
  });
  return { main, dlq };
}

/** Health check: a fila responde. */
export async function pingQueue(client: SQSClient, queueUrl: string): Promise<void> {
  await client.send(new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['QueueArn'] }));
}

/**
 * Publica eventos de integração numa fila FIFO. `MessageGroupId = aggregateId` preserva a ordem
 * por agregado no broker e `MessageDeduplicationId = eventId` deduplica no broker — ambos são só
 * OTIMIZAÇÃO: a correção vem da idempotência do consumidor (inbox por eventId).
 */
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly queue: string | QueueResolver,
  ) {}

  async publish(message: OutboxMessage): Promise<void> {
    const queueUrl = typeof this.queue === 'string' ? this.queue : await this.queue.get();
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.aggregateId,
        MessageDeduplicationId: message.id,
        MessageAttributes: {
          eventType: { DataType: 'String', StringValue: message.eventType },
          eventId: { DataType: 'String', StringValue: message.id },
        },
      }),
    );
  }
}
