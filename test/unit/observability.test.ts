import { describe, expect, test } from 'bun:test';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import type { Repositories } from '@application/ports';
import { InstrumentedUnitOfWork, lockConflictType } from '@infra/observability/instrumented-unit-of-work';
import { METRIC_CATALOG } from '@infra/observability/metric-catalog';
import { PinoLogger, sanitize } from '@infra/observability/pino-logger';
import { PrometheusMetrics } from '@infra/observability/prometheus-metrics';

class Sink {
  lines: string[] = [];
  write(chunk: string): void {
    this.lines.push(chunk.trim());
  }
  json(): Record<string, any>[] {
    return this.lines.map((l) => JSON.parse(l));
  }
}
const newLogger = (level = 'info') => {
  const sink = new Sink();
  return { sink, logger: new PinoLogger({ destination: sink, level, service: 'test-svc', instance: 'pod-1' }) };
};

describe('sanitize / PinoLogger', () => {
  test('valores monetários, corpos e segredos nunca chegam ao log; identificadores sim', () => {
    const out = sanitize({
      correlationId: 'c1',
      walletId: 'w1',
      money: { amount: '25.00', currency: 'BRL' },
      amount: '25.00',
      balance: '975.00',
      initialBalance: { amount: '1000.00' },
      payload: { any: 'thing' },
      body: '{"x":1}',
      nested: { token: 'abc', keep: 'ok', Authorization: 'Bearer x' },
    }) as Record<string, any>;
    expect(out).toMatchObject({ correlationId: 'c1', walletId: 'w1', nested: { keep: 'ok' } });
    for (const k of ['money', 'amount', 'balance', 'initialBalance', 'payload', 'body']) expect(out[k]).toBe('[REDACTED]');
    expect(out.nested.token).toBe('[REDACTED]');
    expect(out.nested.Authorization).toBe('[REDACTED]');
    expect(JSON.stringify(out)).not.toContain('25.00');
  });

  test('mensagens de erro do PostgreSQL com a LINHA inteira (valores) são redigidas', () => {
    const check = 'new row for relation "wallets" violates check constraint "wallets_balance_check"; Failing row contains (0192f291, 0192f28f, BRL, -25.00, 3)';
    const unique = 'duplicate key value violates unique constraint "x"; Key (idempotency_key)=(provider-a:abc) already exists.';
    const out = sanitize({ error: check, e2: new Error(unique) }) as Record<string, any>;
    expect(out.error).toContain('violates check constraint');
    expect(out.error).not.toContain('-25.00');
    expect(out.error).toContain('Failing row contains [REDACTED]');
    expect(out.e2.message).not.toContain('provider-a:abc');
    expect(out.e2.message).toContain('[REDACTED]');
  });

  test('trunca strings longas, limita profundidade e serializa Error/Date com segurança', () => {
    const out = sanitize({
      long: 'x'.repeat(2000),
      deep: { a: { b: { c: { d: { e: 1 } } } } },
      err: new Error('boom'),
      at: new Date('2026-08-01T00:00:00Z'),
    }) as Record<string, any>;
    expect(out.long.length).toBeLessThan(600);
    expect(out.long).toContain('[truncated]');
    expect(JSON.stringify(out.deep)).toContain('[MaxDepth]');
    expect(out.err).toEqual({ name: 'Error', message: 'boom' });
    expect(out.at).toBe('2026-08-01T00:00:00.000Z');
  });

  test('emite uma linha JSON por evento com level, time ISO, msg, service e o contexto', () => {
    const { sink, logger } = newLogger();
    logger.info('wager transaction handled', {
      correlationId: 'corr-1',
      messageId: 'msg-1',
      transactionId: 't-1',
      walletId: 'w-1',
      providerId: 'provider-a',
      money: { amount: '99.99', currency: 'BRL' },
    });
    logger.error('request failed', { error: 'db down' });
    logger.warn('careful');

    expect(sink.lines).toHaveLength(3);
    const [a, b, c] = sink.json();
    expect(a).toMatchObject({
      level: 'info',
      msg: 'wager transaction handled',
      service: 'test-svc',
      instance: 'pod-1',
      correlationId: 'corr-1',
      messageId: 'msg-1',
      transactionId: 't-1',
      walletId: 'w-1',
      providerId: 'provider-a',
      money: '[REDACTED]',
    });
    expect(a!.time).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    expect(sink.lines[0]).not.toContain('99.99');
    expect(b).toMatchObject({ level: 'error', error: 'db down' });
    expect(c).toMatchObject({ level: 'warn', msg: 'careful' });
  });

  test('respeita o nível configurado', () => {
    const { sink, logger } = newLogger('warn');
    logger.info('hidden');
    logger.warn('shown');
    expect(sink.json().map((l) => l.msg)).toEqual(['shown']);
  });
});

describe('PrometheusMetrics', () => {
  test('expõe counter, histogram e gauge no formato Prometheus, com rótulos', async () => {
    const m = new PrometheusMetrics();
    m.increment('wager_transactions_total', { kind: 'BET', status: 'PROCESSED' });
    m.increment('wager_transactions_total', { kind: 'BET', status: 'PROCESSED' });
    m.increment('outbox_published_total', {}, 5);
    m.observe('wallet_lock_wait_seconds', 0.02);
    m.set('outbox_lag_seconds', 12.5);
    const text = await m.render();

    expect(m.contentType()).toContain('text/plain');
    expect(text).toContain('# TYPE wager_transactions_total counter');
    expect(text).toContain('wager_transactions_total{kind="BET",status="PROCESSED"} 2');
    expect(text).toContain('outbox_published_total 5');
    expect(text).toContain('# TYPE wallet_lock_wait_seconds histogram');
    expect(text).toContain('wallet_lock_wait_seconds_bucket{le="0.025"} 1');
    expect(text).toContain('wallet_lock_wait_seconds_count 1');
    expect(text).toContain('outbox_lag_seconds 12.5');
  });

  test('rótulos ausentes viram "" e rótulos extras são ignorados (sem lançar)', async () => {
    const m = new PrometheusMetrics();
    expect(() => m.increment('duplicates_detected_total', { unexpected: 'x' })).not.toThrow();
    m.increment('sqs_dlq_messages_total', { class: 'permanent', extra: 'ignored' });
    const text = await m.render();
    expect(text).toContain('duplicates_detected_total{source=""} 1');
    expect(text).toContain('sqs_dlq_messages_total{class="permanent"} 1');
    expect(text).not.toContain('extra=');
  });

  test('nome desconhecido ou tipo errado é ignorado e avisado uma única vez', () => {
    const { sink, logger } = newLogger();
    const m = new PrometheusMetrics(logger);
    m.increment('nao_existe_total');
    m.increment('nao_existe_total');
    m.observe('outbox_lag_seconds', 1); // é gauge, não histogram
    expect(sink.json().filter((l) => l.msg.includes('metric ignored'))).toHaveLength(2); // uma por nome
  });
});

describe('catálogo de métricas', () => {
  const walk = (dir: string): string[] =>
    readdirSync(dir).flatMap((f) => {
      const p = join(dir, f);
      return statSync(p).isDirectory() ? walk(p) : p.endsWith('.ts') ? [p] : [];
    });

  const used = new Set<string>();
  for (const file of walk('src')) {
    if (file.endsWith('metric-catalog.ts')) continue;
    const src = readFileSync(file, 'utf8');
    for (const m of src.matchAll(/\.(?:increment|observe|set)\(\s*'([a-z0-9_]+)'/g)) used.add(m[1]!);
  }

  test('todo nome usado no código existe no catálogo', () => {
    expect(used.size).toBeGreaterThan(15);
    expect([...used].filter((n) => !(n in METRIC_CATALOG))).toEqual([]);
  });

  test('nenhuma entrada do catálogo fica órfã (sem uso no código)', () => {
    expect(Object.keys(METRIC_CATALOG).filter((n) => !used.has(n))).toEqual([]);
  });

  test('nomes seguem as convenções do Prometheus', () => {
    for (const [name, def] of Object.entries(METRIC_CATALOG)) {
      expect(name).toMatch(/^[a-z_][a-z0-9_]*$/);
      if (def.type === 'counter') expect(name.endsWith('_total')).toBe(true);
      if (def.type === 'histogram') expect(name.endsWith('_seconds')).toBe(true);
      expect(def.labels.every((l) => /^[a-z_][a-z0-9_]*$/.test(l))).toBe(true);
    }
  });

  test('cobre o conjunto mínimo exigido pelo desafio', () => {
    const required = [
      'wager_transactions_total', // transações por status
      'duplicates_detected_total', // duplicatas detectadas
      'sqs_retries_total', // retries
      'sqs_dlq_messages_total', // mensagens em DLQ
      'lock_conflicts_total', // conflitos de lock
      'outbox_lag_seconds', // outbox lag
      'wager_transaction_duration_seconds', // latência de processamento
    ];
    expect(required.filter((n) => !(n in METRIC_CATALOG))).toEqual([]);
  });
});

describe('InstrumentedUnitOfWork', () => {
  const repos = (): Repositories =>
    ({
      wallets: {
        findByIdForUpdate: async () => {
          await new Promise((r) => setTimeout(r, 30));
          return null;
        },
      },
    }) as unknown as Repositories;

  test('mede transações (commit/rollback), a espera pelo lock da wallet e classifica conflitos', async () => {
    const m = new PrometheusMetrics();
    let fail: unknown;
    const inner = {
      run: async <T>(fn: (r: Repositories) => Promise<T>) => {
        const r = await fn(repos());
        if (fail) throw fail;
        return r;
      },
    };
    const uow = new InstrumentedUnitOfWork(inner, m);

    await uow.run((r) => r.wallets.findByIdForUpdate('w'));
    fail = Object.assign(new Error('deadlock detected'), { code: '40P01' });
    await expect(uow.run(async () => undefined)).rejects.toThrow('deadlock');
    fail = Object.assign(new Error('lock not available'), { code: '55P03' });
    await expect(uow.run(async () => undefined)).rejects.toThrow();
    fail = new Error('outro erro qualquer');
    await expect(uow.run(async () => undefined)).rejects.toThrow('outro');

    const text = await m.render();
    expect(text).toContain('db_transaction_duration_seconds_count{outcome="commit"} 1');
    expect(text).toContain('db_transaction_duration_seconds_count{outcome="rollback"} 3');
    expect(text).toContain('lock_conflicts_total{type="deadlock"} 1');
    expect(text).toContain('lock_conflicts_total{type="lock_timeout"} 1');
    expect(text).not.toContain('type="serialization"} 1');
    expect(text).toContain('wallet_lock_wait_seconds_count 1');
    // a espera medida inclui os ~30 ms do lock: nenhum bucket abaixo de 25 ms acumula a amostra
    expect(text).toContain('wallet_lock_wait_seconds_bucket{le="0.01"} 0');
  });

  test('lockConflictType reconhece os códigos do PostgreSQL (inclusive em `cause`)', () => {
    expect(lockConflictType({ code: '40P01' })).toBe('deadlock');
    expect(lockConflictType({ code: '55P03' })).toBe('lock_timeout');
    expect(lockConflictType({ cause: { code: '40001' } })).toBe('serialization');
    expect(lockConflictType(new Error('x'))).toBeNull();
    expect(lockConflictType(null)).toBeNull();
  });
});
