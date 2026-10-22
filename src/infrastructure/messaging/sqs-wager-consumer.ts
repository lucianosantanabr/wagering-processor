import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  type SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { type Logger, type Metrics, noopLogger, noopMetrics } from '@application/observability';
import type { Disposition, WagerMessageHandler } from '@application/wager-message-handler';
import { exponentialBackoffMs } from '@domain/backoff';
import type { WagerQueues } from './sqs';

export interface ConsumerOptions {
  maxMessages: number; // por receive (máx. 10 no SQS)
  waitTimeSeconds: number; // long polling
  retryBaseMs: number; // backoff exponencial entre tentativas (via visibility timeout)
  retryCapMs: number;
  errorPauseMs: number; // pausa quando o próprio receive falha (SQS fora do ar)
}

export const DEFAULT_CONSUMER_OPTIONS: ConsumerOptions = {
  maxMessages: 10,
  waitTimeSeconds: 5,
  retryBaseMs: 1_000,
  retryCapMs: 60_000,
  errorPauseMs: 1_000,
};

/**
 * Consumer do `wager-transactions.fifo`.
 *
 *  - Ordem: mensagens do mesmo `MessageGroupId` (walletId) são tratadas em sequência; grupos distintos em paralelo.
 *    É otimização — a correção vem do lock da wallet e da inbox/idempotência no banco.
 *  - Ack SOMENTE após o commit: `handler.handle` só retorna depois que o use case comitou.
 *  - Crash entre commit e ack => a mensagem reaparece e vira duplicata inofensiva (inbox / idempotency key).
 *  - Retry: `ChangeMessageVisibility` com backoff exponencial (não bloqueia o loop).
 *  - DLQ: copia para a fila morta (com o motivo) e só então apaga a original.
 *  - SIGTERM: `stop()` aborta o long poll, termina o que já está em execução e DEVOLVE (visibilidade 0) o que
 *    foi recebido mas ainda não começou.
 */
export class SqsWagerConsumer {
  private running = false;
  private loop?: Promise<void>;
  private abort?: AbortController;

  constructor(
    private readonly client: SQSClient,
    private readonly queues: WagerQueues,
    private readonly handler: WagerMessageHandler,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
    private readonly options: ConsumerOptions = DEFAULT_CONSUMER_OPTIONS,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  /** Para de receber, conclui as mensagens em andamento e devolve as não iniciadas. */
  async stop(): Promise<void> {
    this.running = false;
    this.abort?.abort();
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      let messages: Message[];
      try {
        messages = await this.receive();
      } catch (e) {
        if (!this.running) return; // abortado pelo stop()
        this.logger.error('sqs receive failed', { error: (e as Error).message });
        await new Promise((r) => setTimeout(r, this.options.errorPauseMs));
        continue;
      }
      if (messages.length > 0) await this.processBatch(messages);
    }
  }

  private async receive(): Promise<Message[]> {
    const queueUrl = await this.queues.main.get();
    this.abort = new AbortController();
    const out = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: this.options.maxMessages,
        WaitTimeSeconds: this.options.waitTimeSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }),
      { abortSignal: this.abort.signal },
    );
    return out.Messages ?? [];
  }

  private async processBatch(messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const m of messages) {
      const key = m.Attributes?.MessageGroupId ?? m.MessageId ?? '';
      groups.set(key, [...(groups.get(key) ?? []), m]);
    }
    await Promise.all([...groups.values()].map((g) => this.processGroup(g)));
  }

  private async processGroup(group: Message[]): Promise<void> {
    for (const [i, message] of group.entries()) {
      if (!this.running) {
        await this.release(group.slice(i)); // shutdown: não inicia o que ainda não começou
        return;
      }
      const proceed = await this.processOne(message);
      if (!proceed) {
        // falha neste grupo: o restante (posterior na ordem) volta para a fila atrás dela
        await this.release(group.slice(i + 1));
        return;
      }
    }
  }

  /** Devolve `true` se o grupo pode seguir para a próxima mensagem. */
  private async processOne(message: Message): Promise<boolean> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    const started = performance.now();
    let disposition: Disposition;
    try {
      disposition = await this.handler.handle(message.Body ?? '', receiveCount, message.MessageId);
    } catch (e) {
      // O handler não deve lançar; se lançar, tratamos como transitório.
      disposition = { action: 'retry', reason: (e as Error).message };
    }
    this.metrics.observe('wager_message_processing_duration_seconds', (performance.now() - started) / 1000);

    try {
      switch (disposition.action) {
        case 'ack':
          await this.ack(message);
          this.metrics.increment('sqs_messages_total', { result: disposition.outcome });
          return true;
        case 'retry': {
          const seconds = Math.max(
            1,
            Math.ceil(exponentialBackoffMs(receiveCount - 1, this.options.retryBaseMs, this.options.retryCapMs) / 1000),
          );
          await this.setVisibility(message, seconds);
          this.metrics.increment('sqs_retries_total');
          this.metrics.increment('sqs_messages_total', { result: 'retry' });
          return false;
        }
        case 'dead-letter':
          await this.deadLetter(message, disposition, receiveCount);
          this.metrics.increment('sqs_dlq_messages_total', { class: disposition.failureClass });
          this.metrics.increment('sqs_messages_total', { result: 'dead_letter' });
          return true; // a mensagem venenosa saiu do caminho; o grupo pode seguir
      }
    } catch (e) {
      // Falha de ack/visibilidade/DLQ: a mensagem simplesmente reaparece e é reprocessada de forma idempotente.
      this.logger.warn('sqs message disposition failed; message will be redelivered', {
        messageId: message.MessageId,
        action: disposition.action,
        error: (e as Error).message,
      });
      this.metrics.increment('sqs_disposition_failures_total', { action: disposition.action });
      return false;
    }
  }

  private async ack(message: Message): Promise<void> {
    const queueUrl = await this.queues.main.get();
    await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
  }

  private async setVisibility(message: Message, seconds: number): Promise<void> {
    const queueUrl = await this.queues.main.get();
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: seconds,
      }),
    );
  }

  /** Visibilidade 0: a mensagem volta à fila imediatamente, sem consumir backoff. */
  private async release(messages: Message[]): Promise<void> {
    await Promise.allSettled(messages.map((m) => this.setVisibility(m, 0)));
  }

  private async deadLetter(
    message: Message,
    d: Extract<Disposition, { action: 'dead-letter' }>,
    receiveCount: number,
  ): Promise<void> {
    const dlqUrl = await this.queues.dlq.get();
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: dlqUrl,
        MessageBody: message.Body ?? '',
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'dead-letter',
        MessageDeduplicationId: `dlq-${message.MessageId}`,
        MessageAttributes: {
          failureClass: { DataType: 'String', StringValue: d.failureClass },
          failureReason: { DataType: 'String', StringValue: d.reason.slice(0, 1000) },
          receiveCount: { DataType: 'Number', StringValue: String(receiveCount) },
        },
      }),
    );
    await this.ack(message); // só apaga a original depois de a cópia estar na DLQ
  }
}
