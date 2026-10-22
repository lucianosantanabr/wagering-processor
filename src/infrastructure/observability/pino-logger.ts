import pino, { type DestinationStream } from 'pino';
import type { Logger } from '@application/observability';

/**
 * Chaves que nunca vão para o log: valores monetários, corpos de requisição/mensagem e segredos.
 * Os logs carregam IDENTIFICADORES (correlationId, messageId, transactionId, walletId, providerId) e
 * resultados (status, failureCode) — nunca o payload financeiro completo.
 */
const SENSITIVE_KEY =
  /^(payload|body|data|money|amount|balance|balances|balancebefore|balanceafter|initialbalance|storedbalance|calculatedbalance|difference|authorization|cookie|set-cookie|password|secret|token|apikey|api_key|accesskeyid|secretaccesskey)$/i;

const MAX_STRING = 500;
const MAX_DEPTH = 4;
const REDACTED = '[REDACTED]';

/**
 * Mensagens de erro do PostgreSQL podem carregar LINHAS inteiras: `Failing row contains (…, 25.00, …)` numa
 * violação de CHECK, ou `Key (col)=(valor)` numa violação de unicidade. Isso vazaria valores financeiros
 * para o log, então tudo entre parênteses depois desses marcadores é removido.
 */
const DB_ROW_DETAIL = /(Failing row contains|Key \([^)]*\)=)\s*\([^)]*\)/gi;

function redactString(s: string): string {
  const clean = s.replace(DB_ROW_DETAIL, '$1 [REDACTED]');
  return clean.length > MAX_STRING ? `${clean.slice(0, MAX_STRING)}…[truncated]` : clean;
}

export function sanitize(value: unknown, depth = 0): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return redactString(value);
  if (typeof value !== 'object') return value;
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Error) return { name: value.name, message: sanitize(value.message) };
  if (depth >= MAX_DEPTH) return '[MaxDepth]';
  if (Array.isArray(value)) return value.slice(0, 20).map((v) => sanitize(v, depth + 1));
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([k, v]) => [k, SENSITIVE_KEY.test(k) ? REDACTED : sanitize(v, depth + 1)]),
  );
}

export interface PinoLoggerOptions {
  level?: string;
  service?: string;
  /** Instância (host/pod) para distinguir réplicas nos logs. */
  instance?: string;
  /** Padrão: stdout. Os testes injetam um destino para capturar as linhas. */
  destination?: DestinationStream;
}

/** Logs estruturados em JSON, uma linha por evento: `{"level":"info","time":…,"msg":…,…contexto}`. */
export class PinoLogger implements Logger {
  private readonly log: pino.Logger;

  constructor(options: PinoLoggerOptions = {}) {
    this.log = pino(
      {
        level: options.level ?? process.env.LOG_LEVEL ?? 'info',
        base: { service: options.service ?? 'wagering', instance: options.instance ?? process.env.HOSTNAME },
        timestamp: pino.stdTimeFunctions.isoTime,
        messageKey: 'msg',
        formatters: { level: (label) => ({ level: label }) },
      },
      options.destination,
    );
  }

  info(message: string, context?: Record<string, unknown>): void {
    this.log.info((context && sanitize(context)) as object, message);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.log.warn((context && sanitize(context)) as object, message);
  }

  error(message: string, context?: Record<string, unknown>): void {
    this.log.error((context && sanitize(context)) as object, message);
  }
}
