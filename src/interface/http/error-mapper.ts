import {
  ApplicationError,
  IdempotencyConflictError,
  TransactionNotFoundError,
  ValidationError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '@application/errors';
import { DomainError } from '@domain/errors';
import { isTransientInfrastructureError } from '@infra/persistence/transient-errors';

/**
 * Mapeamento ÚNICO de falhas para HTTP (usado por todos os endpoints):
 *
 *  400  payload/parâmetro inválido ............ VALIDATION_ERROR            (corrigir antes de reenviar)
 *  404  recurso inexistente ................... WALLET_NOT_FOUND | TRANSACTION_NOT_FOUND | NOT_FOUND
 *  409  conflito .............................. IDEMPOTENCY_CONFLICT | WALLET_ALREADY_EXISTS (não reenviar igual)
 *  422  rejeição por regra de negócio ......... TRANSACTION_REJECTED + failureCode (POST /wagering/transactions)
 *  202  aceito, processamento pendente ........ transação em PENDING_REFERENCE (reenvio desnecessário)
 *  503  falha transitória de infraestrutura ... SERVICE_UNAVAILABLE + Retry-After (reenviar com a MESMA key)
 *  500  erro inesperado ....................... INTERNAL_ERROR
 */
export interface MappedError {
  status: number;
  body: { code: string; message: string };
  headers?: Record<string, string>;
}

export function mapError(e: unknown): MappedError {
  if (e instanceof ValidationError) return fail(400, e);
  if (e instanceof DomainError) return { status: 400, body: { code: e.code, message: e.message } };
  if (e instanceof WalletNotFoundError || e instanceof TransactionNotFoundError) return fail(404, e);
  if (e instanceof IdempotencyConflictError || e instanceof WalletAlreadyExistsError) return fail(409, e);

  if (isTransientInfrastructureError(e)) {
    return {
      status: 503,
      body: { code: 'SERVICE_UNAVAILABLE', message: 'temporary infrastructure failure; retry with the same Idempotency-Key' },
      headers: { 'Retry-After': '1' },
    };
  }

  // Erros do próprio Fastify/Nest (JSON malformado, rota inexistente, payload grande…)
  const http = e as { statusCode?: unknown; status?: unknown; message?: unknown } | null;
  const code = typeof http?.statusCode === 'number' ? http.statusCode : typeof http?.status === 'number' ? http.status : undefined;
  if (code && code >= 400 && code < 500) {
    return {
      status: code,
      body: { code: code === 404 ? 'NOT_FOUND' : 'BAD_REQUEST', message: String(http?.message ?? 'bad request') },
    };
  }

  if (e instanceof ApplicationError) return fail(500, e);
  return { status: 500, body: { code: 'INTERNAL_ERROR', message: 'unexpected error' } };
}

function fail(status: number, e: ApplicationError): MappedError {
  return { status, body: { code: e.code, message: e.message } };
}
