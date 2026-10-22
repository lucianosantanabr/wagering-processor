import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import {
  type Infra,
  type Instance,
  createWallet,
  divergentWallets,
  drainEvents,
  http,
  ledgerState,
  money,
  sendWagerMessages,
  startInfra,
  startInstance,
  submit,
  txBody,
  wagerMessage,
  waitUntil,
} from './support/cluster';

setDefaultTimeout(400_000);

let infra: Infra;
let alive: Instance[] = [];

beforeAll(async () => {
  infra = await startInfra();
}, 400_000);
afterEach(async () => {
  await Promise.all(alive.map((i) => i.kill().catch(() => undefined)));
  alive = [];
});
afterAll(async () => {
  await infra?.stop();
}, 120_000);

async function boot(ids: string[], env: Record<string, string> = {}): Promise<Instance[]> {
  const started = await Promise.all(ids.map((id) => startInstance(infra, id, env)));
  alive.push(...started);
  return started;
}

const dlqDepth = async () => {
  const out = await infra.sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: await infra.queues.dlq.get(),
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return Number(out.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(out.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
};

describe('processo morto no meio da carga (kill -9)', () => {
  test('SQS: um dos 3 processos morre processando; outro assume; cada mensagem tem efeito exatamente uma vez', async () => {
    const [p1, p2, p3] = await boot(['p1', 'p2', 'p3']);
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet(p2!, '1000.00')));
    const msgs = wallets.flatMap((w) => Array.from({ length: 30 }, () => wagerMessage(w, { money: money('5.00') })));
    const ids = msgs.map((m) => `'${m.messageId}'`).join(',');
    const done = async () =>
      Number(
        (await infra.query<{ n: string }>(`SELECT count(*)::text n FROM inbox_messages WHERE processed_at IS NOT NULL AND message_id = ANY(ARRAY[${ids}])`))[0]!.n,
      );

    await sendWagerMessages(infra, msgs);
    await waitUntil(async () => (await done()) >= 40, 120_000, 50, 'carga em andamento');
    const doneAtKill = await done();
    await p1!.kill(); // SIGKILL: sem handlers, sem devolver mensagens, sem fechar conexões
    expect(p1!.isAlive()).toBe(false);
    expect(doneAtKill).toBeLessThan(msgs.length); // ainda havia trabalho em andamento quando ele morreu

    // um processo novo entra no lugar (como faria o orquestrador); mensagens em voo reaparecem após o visibility timeout
    const [p4] = await boot(['p4']);
    await waitUntil(async () => (await done()) === msgs.length, 240_000, 500, '300 mensagens processadas');

    // efeito exatamente uma vez, mesmo com mensagens reentregues
    for (const w of wallets) {
      expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '850.00', entries: 31, consistent: true });
    }
    const [tx] = await infra.query<{ n: string; d: string }>(
      `SELECT count(*)::text n, count(DISTINCT idempotency_key)::text d FROM wager_transactions
        WHERE kind = 'BET' AND wallet_id = ANY(ARRAY[${wallets.map((w) => `'${w.walletId}'`).join(',')}]::uuid[])`,
    );
    expect(tx).toEqual({ n: '300', d: '300' });
    expect(await dlqDepth()).toBe(0); // nada foi parar na DLQ por causa do crash
    expect(await divergentWallets(infra)).toBe(0);
    expect([p2, p3, p4].every((i) => i!.isAlive())).toBe(true);
  });

  test('HTTP: o provedor reenvia (mesma key) para outra instância quando uma morre; cada aposta é debitada uma única vez', async () => {
    const targets = await boot(['p1', 'p2', 'p3']);
    const w = await createWallet(targets[1]!, '10000.00');
    const bodies = Array.from({ length: 90 }, () => txBody(w, { money: money('25.00') }));

    let retried = 0;
    const sendWithRetry = async (body: Record<string, unknown>, first: number) => {
      for (let attempt = 0; attempt < 8; attempt++) {
        const target = targets[(first + attempt) % targets.length]!;
        try {
          const r = await submit(target, body);
          if (r.status === 503) continue; // falha transitória: reenviar com a MESMA key
          if (attempt > 0) retried += 1;
          return r;
        } catch {
          /* conexão recusada/reiniciada: o processo morreu; tenta a próxima instância */
        }
      }
      throw new Error('provider gave up');
    };

    // requisições escalonadas no tempo; o p1 morre no meio do fluxo
    const inflight = bodies.map(async (b, i) => {
      await new Promise((r) => setTimeout(r, i * 12));
      return sendWithRetry(b, i);
    });
    setTimeout(() => void targets[0]!.kill(), 450);
    const results = await Promise.all(inflight);

    expect(results.every((r) => r.status === 200 || r.status === 201)).toBe(true); // nunca 409/500/422
    expect(retried).toBeGreaterThan(0); // o crash realmente forçou reenvios
    expect(targets[0]!.isAlive()).toBe(false);

    // cada external id foi debitado exatamente uma vez, haja o que houver com respostas perdidas
    const [debits] = await infra.query<{ n: string }>(
      `SELECT count(*)::text n FROM wallet_ledger_entries WHERE wallet_id = '${w.walletId}' AND direction = 'DEBIT'`,
    );
    expect(debits!.n).toBe('90');
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '7750.00', entries: 91, consistent: true });
  });
});

describe('reinício completo do serviço', () => {
  test('todos os processos morrem com eventos pendentes e uma reversão aguardando; ao reiniciar, tudo converge e o estado é consistente', async () => {
    // Publisher DESLIGADO nestas instancias: por construcao, todo evento gravado fica PENDENTE na outbox quando o
    // cluster morrer (nao depende de timing). Quem publica sao as instancias novas, depois do reinicio.
    const before = await boot(['a1', 'a2'], { OUTBOX_PUBLISHER_ENABLED: 'false' });
    const w = await createWallet(before[0]!, '1000.00');

    const betExt = txBody(w, { money: money('40.00') });
    const refund = txBody(w, { kind: 'REFUND', money: money('40.00'), referenceExternalTransactionId: betExt.externalTransactionId });
    const firstRefund = await submit(before[1]!, refund);
    expect(firstRefund.status).toBe(202); // referência ainda não chegou
    const others = Array.from({ length: 8 }, () => txBody(w, { money: money('10.00') }));
    const sent = await Promise.all(others.map((b, i) => submit(before[i % 2]!, b)));
    expect(sent.every((r) => r.status === 201)).toBe(true);

    // Só os eventos DESTA wallet (a tabela é compartilhada com os testes anteriores deste arquivo).
    const [counts] = await infra.query<{ pending: string; total: string }>(
      `SELECT count(*) FILTER (WHERE published_at IS NULL)::text AS pending, count(*)::text AS total
         FROM outbox_messages WHERE payload->'data'->>'walletId' = '${w.walletId}'`,
    );
    // 2 (criação da wallet) + 1 (reversão pendente) + 8 apostas x 2 eventos = 19, TODOS ainda pendentes:
    expect(counts).toEqual({ pending: '19', total: '19' }); // commit feito, nenhum evento publicado...
    await Promise.all(before.map((i) => i.kill())); // ...e TODO o serviço morre
    alive = [];
    expect(before.every((i) => !i.isAlive())).toBe(true);

    // estado em repouso: nada se perdeu e o banco é consistente mesmo com o serviço fora do ar
    expect(await divergentWallets(infra)).toBe(0);
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '920.00', entries: 9, consistent: true });

    // ── reinício ──
    const after = await boot(['b1', 'b2']);

    // 1) a outbox pendente é publicada pelas instâncias novas
    await waitUntil(
      async () => (await infra.query<{ n: string }>(`SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL`))[0]!.n === '0',
      120_000,
      300,
      'outbox esvaziada após o reinício',
    );

    // 2) a reversão que esperava a referência é resolvida assim que a BET chega a uma instância nova
    expect((await submit(after[0]!, betExt)).status).toBe(201);
    await waitUntil(
      async () =>
        (await infra.query<{ s: string }>(`SELECT status s FROM wager_transactions WHERE id = '${firstRefund.json.transactionId}'`))[0]!.s === 'PROCESSED',
      120_000,
      300,
      'PENDING_REFERENCE resolvida após o reinício',
    );
    expect(await ledgerState(infra, w.walletId)).toMatchObject({ balance: '920.00', entries: 11, consistent: true }); // BET -40, REFUND +40

    // 3) idempotência persiste ao reinício: reenviar um pedido antigo é replay com o resultado ORIGINAL
    const replay = await submit(after[1]!, others[0]!);
    expect(replay.status).toBe(200);
    expect(replay.json).toMatchObject({ idempotentReplay: true, status: 'PROCESSED', balance: sent[0]!.json.balance });

    // 4) os eventos de TODO o histórico chegaram à fila (nenhum se perdeu no crash)
    await waitUntil(
      async () => (await infra.query<{ n: string }>(`SELECT count(*)::text n FROM outbox_messages WHERE published_at IS NULL`))[0]!.n === '0',
      60_000,
      300,
      'outbox final',
    );
    const rows = await infra.query<{ id: string }>(`SELECT id FROM outbox_messages`);
    expect(new Set(await drainEvents(infra))).toEqual(new Set(rows.map((r) => r.id)));

    expect(await divergentWallets(infra)).toBe(0);
    const reconciliation = await http(after[0]!, 'POST', `/wallets/${w.walletId}/reconciliation`);
    expect(reconciliation.json).toMatchObject({ consistent: true, difference: money('0.00') });
  });
});
