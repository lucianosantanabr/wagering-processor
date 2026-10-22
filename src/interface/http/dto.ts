import { ValidationError } from '@application/errors';
import type { CreateWalletCommand } from '@application/create-wallet';
import type { SubmitWagerTransactionCommand } from '@application/submit-wager-transaction';
import type { MoneyProps } from '@domain/money';

type Json = Record<string, unknown>;

function asObject(value: unknown, what: string): Json {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new ValidationError(`${what} must be a JSON object`);
  }
  return value as Json;
}

function str(obj: Json, field: string, required = true): string | undefined {
  const v = obj[field];
  if (v === undefined || v === null) {
    if (required) throw new ValidationError(`${field} is required`);
    return undefined;
  }
  if (typeof v !== 'string') throw new ValidationError(`${field} must be a string`);
  return v;
}

/** Só checa o FORMATO (strings). Os valores (escala, sinal, ISO-4217…) são validados por `Money`. */
function money(obj: Json, field: string): MoneyProps {
  const m = asObject(obj[field], field);
  return { amount: str(m, 'amount') as string, currency: str(m, 'currency') as string };
}

export function parseCreateWallet(body: unknown, correlationId?: string): CreateWalletCommand {
  const b = asObject(body, 'body');
  return { playerId: str(b, 'playerId') as string, initialBalance: money(b, 'initialBalance'), correlationId };
}

export function parseSubmitTransaction(
  body: unknown,
  idempotencyKey: string | string[] | undefined,
  correlationId?: string,
): SubmitWagerTransactionCommand {
  if (typeof idempotencyKey !== 'string' || idempotencyKey.trim() === '') {
    throw new ValidationError('Idempotency-Key header is required');
  }
  const b = asObject(body, 'body');
  return {
    providerId: str(b, 'providerId') as string,
    externalTransactionId: str(b, 'externalTransactionId') as string,
    idempotencyKey, // o header é a fonte da verdade
    playerId: str(b, 'playerId') as string,
    walletId: str(b, 'walletId') as string,
    roundId: str(b, 'roundId') as string,
    gameId: str(b, 'gameId') as string,
    kind: str(b, 'kind') as string,
    money: money(b, 'money'),
    referenceExternalTransactionId: str(b, 'referenceExternalTransactionId', false),
    correlationId,
  };
}
