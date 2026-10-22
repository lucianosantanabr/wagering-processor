import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { Metrics } from '@application/observability';
import type { QueueResolver } from './sqs';

/** Amostra a profundidade das filas (visíveis / em voo) para o gauge `sqs_queue_messages`. */
export class QueueDepthSampler {
  constructor(
    private readonly client: SQSClient,
    private readonly queues: Record<string, QueueResolver>,
    private readonly metrics: Metrics,
  ) {}

  /** Uma amostra de todas as filas; erros de uma fila não impedem as demais. */
  async sample(): Promise<void> {
    await Promise.allSettled(
      Object.entries(this.queues).map(async ([label, resolver]) => {
        const out = await this.client.send(
          new GetQueueAttributesCommand({
            QueueUrl: await resolver.get(),
            AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
          }),
        );
        const a = out.Attributes ?? {};
        this.metrics.set('sqs_queue_messages', Number(a.ApproximateNumberOfMessages ?? 0), { queue: label, state: 'visible' });
        this.metrics.set('sqs_queue_messages', Number(a.ApproximateNumberOfMessagesNotVisible ?? 0), {
          queue: label,
          state: 'in_flight',
        });
      }),
    );
  }
}
