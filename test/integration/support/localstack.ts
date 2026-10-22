import type { SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient } from '@infra/messaging/sqs';
import { removeContainer, runContainer } from './container';

export interface TestSqs {
  client: SQSClient;
  endpoint: string;
  stop(): Promise<void>;
}

/** Sobe um LocalStack real (apenas SQS) em container e espera o serviço ficar disponível. */
export async function startLocalstack(): Promise<TestSqs> {
  const c = await runContainer({
    image: 'localstack/localstack:3.8',
    port: 4566,
    env: { SERVICES: 'sqs', AWS_DEFAULT_REGION: 'us-east-1', DEBUG: '0' },
    readyCommand: ['sh', '-c', `curl -sf http://localhost:4566/_localstack/health | grep -qE '"sqs": "(available|running)"'`],
    readyAttempts: 120,
  });
  const endpoint = `http://127.0.0.1:${c.hostPort}`;
  const client = createSqsClient({ region: 'us-east-1', endpoint, accessKeyId: 'test', secretAccessKey: 'test' });
  return {
    client,
    endpoint,
    stop: async () => {
      client.destroy();
      await removeContainer(c.id);
    },
  };
}
