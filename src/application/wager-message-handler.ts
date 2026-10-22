import { IdempotencyConflictError, MessageIdReuseError, ValidationError, WalletNotFoundError } from './errors';
import { DomainError } from '@domain/errors';
import { sha256Canonical } from '@domain/payload-hash';
import type { MoneyProps } from '@domain/money';
import { type Logger, type Metrics, noopLogger, noopMetrics } from './observability';
import type { SubmitWagerTransaction, SubmitWagerTransactionCommand } from './submit-wager-transaction';

export const WAGER_CONSUMER_NAME = 'wager-transactions-consumer';
export const WAGER_REQUESTED = 'WagerTransactionRequested';

/**
 * O que fazer com a mensagem depois de tentar processá-la:
 *  - ack          terminou (inclusive rejeição de NEGÓCIO e duplicata): pode apagar da fila;
 *  - retry        falha TRANSITÓRIA: devolver à fila com backoff;
 *  - dead-letter  nunca vai dar certo (PERMANENTE) ou as tentativas acabaram: mover para a DLQ.
 */
export type Disposition =
  | { action: 'ack'; outcome: 'processed' | 'rejected' | 'pending_reference' | 'duplicate'; transactionId: string }
  | { action: 'retry'; reason: string }
  | { action: 'dead-letter'; failureClass: 'permanent' | 'exhausted'; reason: string };

/** Mensagem malformada: nenhum reenvio a conserta. */
export class PermanentMessageError extends Error {}

interface Envelope {
  messageId: string;
  type: string;
  occurredAt?: string;
  data: Record<string, unknown>;
}

function text(obj: Record<string, unknown>, field: string, required = true): string | undefined {
  const v = obj[field];
  if (v === undefined || v === null) {
    if (required) throw new PermanentMessageError(`data.${field} is required`);
    return undefined;
  }
  if (typeof v !== 'string') throw new PermanentMessageError(`data.${field} must be a string`);
  return v;
}

/** Valida o ENVELOPE e o formato; regras de negócio ficam a cargo do use case. */
export function parseWagerMessage(body: string): { envelope: Envelope; command: SubmitWagerTransactionCommand; bodyHash: string } {
  let raw: unknown;
  try {
    raw = JSON.parse(body);
  } catch {
    throw new PermanentMessageError('body is not valid JSON');
  }
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new PermanentMessageError('body must be a JSON object');
  }
  const env = raw as Record<string, unknown>;
  if (typeof env.messageId !== 'string' || env.messageId.trim() === '') {
    throw new PermanentMessageError('messageId is required');
  }
  if (env.type !== WAGER_REQUESTED) {
    throw new PermanentMessageError(`unsupported message type "${String(env.type)}"`);
  }
  if (env.data === null || typeof env.data !== 'object' || Array.isArray(env.data)) {
    throw new PermanentMessageError('data must be a JSON object');
  }
  const data = env.data as Record<string, unknown>;
  const m = data.money;
  if (m === null || typeof m !== 'object' || Array.isArray(m)) throw new PermanentMessageError('data.money is required');
  const money: MoneyProps = {
    amount: text(m as Record<string, unknown>, 'amount') as string,
    currency: text(m as Record<string, unknown>, 'currency') as string,
  };

  const bodyHash = sha256Canonical(raw);
  return {
    envelope: { messageId: env.messageId, type: env.type, occurredAt: env.occurredAt as string | undefined, data },
    bodyHash,
    command: {
      providerId: text(data, 'providerId') as string,
      externalTransactionId: text(data, 'externalTransactionId') as string,
      idempotencyKey: text(data, 'idempotencyKey') as string,
      playerId: text(data, 'playerId') as string,
      walletId: text(data, 'walletId') as string,
      roundId: text(data, 'roundId') as string,
      gameId: text(data, 'gameId') as string,
      kind: text(data, 'kind') as string,
      money,
      referenceExternalTransactionId: text(data, 'referenceExternalTransactionId', false),
      correlationId: env.messageId,
      inbox: { consumerName: WAGER_CONSUMER_NAME, messageId: env.messageId, payloadHash: bodyHash },
    },
  };
}

export interface HandlerOptions {
  /** Recebimentos (tentativas) permitidos antes de ir para a DLQ. */
  maxReceiveCount: number;
}

/**
 * Traduz o resultado do use case (o MESMO da API HTTP) numa decisão para o transporte.
 * Não conhece SQS: só recebe o corpo e quantas vezes a mensagem já foi entregue.
 */
export class WagerMessageHandler {
  constructor(
    private readonly submit: SubmitWagerTransaction,
    private readonly isTransient: (e: unknown) => boolean,
    private readonly options: HandlerOptions,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
  ) {}

  async handle(body: string, receiveCount: number, transportId?: string): Promise<Disposition> {
    let parsed: ReturnType<typeof parseWagerMessage>;
    try {
      parsed = parseWagerMessage(body);
    } catch (e) {
      return this.deadLetter('permanent', (e as Error).message, { sqsMessageId: transportId });
    }

    const { command } = parsed;
    const logCtx = {
      sqsMessageId: transportId,
      messageId: parsed.envelope.messageId,
      correlationId: command.correlationId,
      providerId: command.providerId,
      walletId: command.walletId,
    };

    try {
      const result = await this.submit.execute(command);
      // O use case só retorna depois do COMMIT: apagar a mensagem (ack) é seguro.
      const outcome = result.idempotentReplay
        ? 'duplicate'
        : result.status === 'REJECTED'
          ? 'rejected'
          : result.status === 'PENDING_REFERENCE'
            ? 'pending_reference'
            : 'processed';
      // (a métrica de duplicatas é contada no use case, que enxerga HTTP e SQS)
      return { action: 'ack', outcome, transactionId: result.transactionId };
    } catch (e) {
      if (this.isPermanent(e)) {
        return this.deadLetter('permanent', (e as Error).message, logCtx);
      }
      // transitória, ou desconhecida: tratada como transitória até o limite de tentativas
      if (receiveCount >= this.options.maxReceiveCount) {
        return this.deadLetter('exhausted', `retries exhausted: ${(e as Error).message}`, logCtx);
      }
      this.logger.warn('message processing failed; will retry', {
        ...logCtx,
        receiveCount,
        transient: this.isTransient(e),
        error: (e as Error).message,
      });
      return { action: 'retry', reason: (e as Error).message };
    }
  }

  /** Erros que nenhuma nova tentativa resolve: contrato inválido, conflito de idempotência, wallet inexistente. */
  private isPermanent(e: unknown): boolean {
    if (this.isTransient(e)) return false;
    return (
      e instanceof ValidationError ||
      e instanceof IdempotencyConflictError ||
      e instanceof MessageIdReuseError ||
      e instanceof WalletNotFoundError ||
      e instanceof DomainError ||
      e instanceof PermanentMessageError
    );
  }

  private deadLetter(
    failureClass: 'permanent' | 'exhausted',
    reason: string,
    ctx: Record<string, unknown>,
  ): Disposition {
    this.logger.error('message sent to dead-letter queue', { ...ctx, failureClass, reason });
    return { action: 'dead-letter', failureClass, reason };
  }
}
