import { createHash } from 'node:crypto';
import { Money, type MoneyProps } from './money';

/**
 * Algoritmo do payloadHash:
 *   1. seleciona apenas os campos de negócio (header Idempotency-Key e metadados de transporte ficam de fora);
 *   2. normaliza `money` via {@link Money} ("25" e "25.00" produzem o mesmo hash);
 *   3. omite campos `undefined`;
 *   4. serializa em JSON canônico (chaves ordenadas recursivamente, sem espaços);
 *   5. SHA-256 em hex.
 */
export interface PayloadHashFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const parts = Object.keys(obj)
      .filter((k) => obj[k] !== undefined)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`);
    return `{${parts.join(',')}}`;
  }
  return JSON.stringify(value);
}

export function computePayloadHash(fields: PayloadHashFields): string {
  const business = {
    providerId: fields.providerId,
    externalTransactionId: fields.externalTransactionId,
    playerId: fields.playerId,
    walletId: fields.walletId,
    roundId: fields.roundId,
    gameId: fields.gameId,
    kind: fields.kind,
    money: Money.from(fields.money).toJSON(),
    referenceExternalTransactionId: fields.referenceExternalTransactionId,
  };
  return sha256Canonical(business);
}

/** SHA-256 (hex) do JSON canônico de qualquer valor. */
export function sha256Canonical(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}
