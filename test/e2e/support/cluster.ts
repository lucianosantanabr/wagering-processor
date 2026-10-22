import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { MikroORM } from '@mikro-orm/postgresql';
import { SQSClient } from '@aws-sdk/client-sqs';
import { buildOrmConfig } from '@infra/persistence/mikro-orm.config';
import { createSqsClient, wagerQueues } from '@infra/messaging/sqs';
import { removeContainer, runContainer } from '../../integration/support/container';

export const MAX_RECEIVE = 5;
export const VISIBILITY_SECONDS = 3;

/** Infraestrutura compartilhada pelas instâncias: PostgreSQL e LocalStack REAIS, em containers. */
export interface Infra {
  databaseUrl: string;
  sqsEndpoint: string;
  sqs: SQSClient;
  /** Nomes de fila exclusivos desta execução (as instâncias recebem os mesmos por variável de ambiente). */
  queueNames: { main: string; dlq: string; events: string };
  queues: ReturnType<typeof wagerQueues>;
  /** Conexão só para consultar o banco (NÃO roda migrations: quem migra são os processos). */
  orm: MikroORM;
  query<T = Record<string, unknown>>(sql: string): Promise<T[]>;
  stop(): Promise<void>;
}

export async function startInfra(): Promise<Infra> {
  const [pg, ls] = await Promise.all([
    runContainer({
      image: 'postgres:16-alpine',
      port: 5432,
      env: { POSTGRES_USER: 'wagering', POSTGRES_PASSWORD: 'wagering', POSTGRES_DB: 'wagering' },
      readyCommand: ['pg_isready', '-U', 'wagering', '-d', 'wagering'],
    }),
    runContainer({
      image: 'localstack/localstack:3.8',
      port: 4566,
      env: { SERVICES: 'sqs', AWS_DEFAULT_REGION: 'us-east-1', DEBUG: '0' },
      readyCommand: ['sh', '-c', `curl -sf http://localhost:4566/_localstack/health | grep -qE '"sqs": "(available|running)"'`],
      readyAttempts: 120,
    }),
  ]);
  const databaseUrl = `postgresql://wagering:wagering@127.0.0.1:${pg.hostPort}/wagering`;
  const sqsEndpoint = `http://127.0.0.1:${ls.hostPort}`;
  const sqs = createSqsClient({ region: 'us-east-1', endpoint: sqsEndpoint, accessKeyId: 'test', secretAccessKey: 'test' });

  const run = randomUUID().slice(0, 8);
  const queueNames = {
    main: `wager-transactions-${run}.fifo`,
    dlq: `wager-transactions-dlq-${run}.fifo`,
    events: `wager-events-${run}.fifo`,
  };
  // MESMOS atributos que o main.ts calcula a partir das variáveis de ambiente (senão o SQS recusa a recriação).
  const queues = wagerQueues(sqs, queueNames, { maxReceiveCount: MAX_RECEIVE, visibilityTimeoutSeconds: VISIBILITY_SECONDS });

  // Conecta sem migrar: as tabelas só existem depois que os processos subirem. Tenta até o banco aceitar conexões.
  let orm: MikroORM | undefined;
  for (let i = 0; i < 40 && !orm; i++) {
    try {
      orm = await MikroORM.init(buildOrmConfig(databaseUrl));
    } catch {
      await new Promise((r) => setTimeout(r, 500));
    }
  }
  if (!orm) throw new Error('could not connect to postgres');
  const db = orm;

  return {
    databaseUrl,
    sqsEndpoint,
    sqs,
    queueNames,
    queues,
    orm: db,
    query: (sql) => db.em.execute(sql) as Promise<never>,
    stop: async () => {
      await db.close(true);
      sqs.destroy();
      await Promise.all([removeContainer(pg.id), removeContainer(ls.id)]);
    },
  };
}

// ───────────────────────────── processos ─────────────────────────────

export interface Instance {
  id: string;
  port: number;
  url: string;
  /** Linhas de stdout/stderr já recebidas (logs JSON do servidor). */
  logs: string[];
  exited: Promise<number>;
  isAlive(): boolean;
  /** SIGKILL: o processo morre sem executar nenhum handler (equivale a um crash/OOM-kill). */
  kill(): Promise<void>;
  /** SIGTERM (só faz sentido fora do Windows, onde o sinal vira terminação forçada). */
  terminate(): Promise<number>;
  metrics(): Promise<string>;
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address() as { port: number };
      srv.close(() => resolve(port));
    });
  });
}

async function pump(stream: ReadableStream<Uint8Array> | null | undefined, sink: string[]): Promise<void> {
  if (!stream) return;
  const decoder = new TextDecoder();
  let buf = '';
  // Consumir continuamente é obrigatório: um pipe cheio bloquearia o processo filho.
  for await (const chunk of stream) {
    buf += decoder.decode(chunk, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop() ?? '';
    for (const l of lines) if (l.trim()) sink.push(l.trim());
  }
}

/** Sobe `bun src/main.ts` como um processo do SO, com as MESMAS variáveis de ambiente da produção. */
export async function startInstance(
  infra: Infra,
  id: string,
  env: Record<string, string> = {},
  opts: { waitReady?: boolean } = {},
): Promise<Instance> {
  const port = await freePort();
  const logs: string[] = [];
  const proc = Bun.spawn([process.execPath, 'src/main.ts'], {
    cwd: process.cwd(),
    env: {
      ...(process.env as Record<string, string>),
      DATABASE_URL: infra.databaseUrl,
      PORT: String(port),
      HOSTNAME: id,
      LOG_LEVEL: 'info',
      AWS_ENDPOINT_URL: infra.sqsEndpoint,
      AWS_REGION: 'us-east-1',
      AWS_ACCESS_KEY_ID: 'test',
      AWS_SECRET_ACCESS_KEY: 'test',
      SQS_QUEUE_NAME: infra.queueNames.main,
      SQS_DLQ_NAME: infra.queueNames.dlq,
      SQS_EVENTS_QUEUE_NAME: infra.queueNames.events,
      SQS_MAX_RECEIVE_COUNT: String(MAX_RECEIVE),
      SQS_VISIBILITY_TIMEOUT_SECONDS: String(VISIBILITY_SECONDS),
      PENDING_REFERENCE_BASE_MS: '200',
      PENDING_REFERENCE_CAP_MS: '1000',
      PENDING_REFERENCE_MAX_ATTEMPTS: '400',
      ...env,
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  void pump(proc.stdout, logs);
  void pump(proc.stderr, logs);

  let alive = true;
  const exited = proc.exited.then((code) => {
    alive = false;
    return code;
  });
  const url = `http://127.0.0.1:${port}`;

  const instance: Instance = {
    id,
    port,
    url,
    logs,
    exited,
    isAlive: () => alive,
    kill: async () => {
      proc.kill('SIGKILL');
      await exited;
    },
    terminate: async () => {
      proc.kill('SIGTERM');
      return exited;
    },
    metrics: async () => (await fetch(`${url}/metrics`)).text(),
  };

  if (opts.waitReady !== false) await waitReady(instance);
  return instance;
}

export async function waitReady(i: Instance, timeoutMs = 90_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!i.isAlive()) throw new Error(`instance ${i.id} exited during startup:\n${i.logs.slice(-15).join('\n')}`);
    try {
      const res = await fetch(`${i.url}/health/ready`);
      if (res.status === 200) return;
    } catch {
      /* ainda subindo */
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`instance ${i.id} not ready after ${timeoutMs}ms:\n${i.logs.slice(-15).join('\n')}`);
}

// ───────────────────────────── helpers de teste ─────────────────────────────

export async function waitUntil(cond: () => Promise<boolean>, timeoutMs = 60_000, stepMs = 200, what = 'condition'): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  throw new Error(`timed out waiting for ${what} (${timeoutMs}ms)`);
}

/** Soma de uma série Prometheus em texto (0 se não existe). */
export function metricValue(text: string, series: string): number {
  const line = text.split('\n').find((l) => l.startsWith(`${series} `));
  return line ? Number(line.split(' ').pop()) : 0;
}

export const money = (amount: string) => ({ amount, currency: 'BRL' });

export async function http(
  inst: Pick<Instance, 'url'>,
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; json: any }> {
  const res = await fetch(`${inst.url}${path}`, {
    method,
    headers: { ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json: any;
  try {
    json = text ? JSON.parse(text) : undefined;
  } catch {
    json = text;
  }
  return { status: res.status, json };
}

export async function createWallet(inst: Pick<Instance, 'url'>, balance = '100.00') {
  const playerId = randomUUID();
  const r = await http(inst, 'POST', '/wallets', { playerId, initialBalance: money(balance) });
  if (r.status !== 201) throw new Error(`create wallet failed: ${r.status} ${JSON.stringify(r.json)}`);
  return { walletId: r.json.id as string, playerId };
}

let seq = 0;
export function txBody(w: { walletId: string; playerId: string }, over: Record<string, unknown> = {}) {
  seq += 1;
  const ext = `e2e-${seq}-${randomUUID().slice(0, 8)}`;
  return {
    providerId: 'provider-a',
    externalTransactionId: ext,
    playerId: w.playerId,
    walletId: w.walletId,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: money('25.00'),
    ...over,
  };
}

export const submit = (inst: Pick<Instance, 'url'>, body: Record<string, unknown>) =>
  http(inst, 'POST', '/wagering/transactions', body, {
    'idempotency-key': `${body.providerId}:${body.externalTransactionId}`,
  });

/** Mensagem `WagerTransactionRequested` no formato do enunciado do desafio. */
export function wagerMessage(w: { walletId: string; playerId: string }, over: Record<string, unknown> = {}) {
  const b = txBody(w, over);
  return {
    messageId: `msg-${randomUUID()}`,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: { ...b, idempotencyKey: `${b.providerId}:${b.externalTransactionId}` },
  };
}

/** Publica mensagens na fila de entrada (em lotes de 10); grupo FIFO = walletId, como faria o provedor. */
export async function sendWagerMessages(infra: Infra, messages: ReturnType<typeof wagerMessage>[]): Promise<void> {
  const { SendMessageBatchCommand } = await import('@aws-sdk/client-sqs');
  const url = await infra.queues.main.get();
  for (let i = 0; i < messages.length; i += 10) {
    const out = await infra.sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: url,
        Entries: messages.slice(i, i + 10).map((m, k) => ({
          Id: String(k),
          MessageBody: JSON.stringify(m),
          MessageGroupId: m.data.walletId as string,
          MessageDeduplicationId: randomUUID(),
        })),
      }),
    );
    if (out.Failed?.length) throw new Error(`send failed: ${JSON.stringify(out.Failed)}`);
  }
}

/** Lê (e apaga) tudo da fila de eventos publicados pela outbox; devolve todos os `eventId` recebidos. */
export async function drainEvents(infra: Infra): Promise<string[]> {
  const { DeleteMessageBatchCommand, ReceiveMessageCommand } = await import('@aws-sdk/client-sqs');
  const url = await infra.sqs.send(new (await import('@aws-sdk/client-sqs')).GetQueueUrlCommand({ QueueName: infra.queueNames.events }));
  const queueUrl = url.QueueUrl!;
  const ids: string[] = [];
  let empty = 0;
  while (empty < 3) {
    const r = await infra.sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    const msgs = r.Messages ?? [];
    if (msgs.length === 0) {
      empty += 1;
      continue;
    }
    empty = 0;
    for (const m of msgs) ids.push(JSON.parse(m.Body ?? '{}').eventId as string);
    await infra.sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: msgs.map((m, k) => ({ Id: String(k), ReceiptHandle: m.ReceiptHandle! })),
      }),
    );
  }
  return ids;
}

export const sumMetric = async (instances: Instance[], series: string): Promise<number> => {
  const texts = await Promise.all(instances.filter((i) => i.isAlive()).map((i) => i.metrics()));
  return texts.reduce((a, t) => a + metricValue(t, series), 0);
};

/** Quantas wallets têm saldo diferente do saldo reconstruído pelo ledger (deve ser sempre 0). */
export async function divergentWallets(infra: Infra): Promise<number> {
  const [r] = await infra.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM wallets w
      WHERE w.balance <> COALESCE((SELECT SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END)
                                     FROM wallet_ledger_entries l WHERE l.wallet_id = w.id), 0)`,
  );
  return Number(r!.n);
}

/** Invariante final: saldo da wallet == saldo reconstruído pelo ledger, e version == nº de lançamentos. */
export async function ledgerState(infra: Infra, walletId: string) {
  const [w] = await infra.query<{ balance: string; version: number }>(
    `SELECT balance::text AS balance, version FROM wallets WHERE id = '${walletId}'`,
  );
  const [l] = await infra.query<{ rebuilt: string; entries: string }>(
    `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::text AS rebuilt,
            COUNT(*)::text AS entries
       FROM wallet_ledger_entries WHERE wallet_id = '${walletId}'`,
  );
  return {
    balance: w!.balance,
    rebuilt: Number(l!.rebuilt).toFixed(2),
    entries: Number(l!.entries),
    version: w!.version,
    consistent: w!.balance === Number(l!.rebuilt).toFixed(2) && w!.version === Number(l!.entries),
  };
}
