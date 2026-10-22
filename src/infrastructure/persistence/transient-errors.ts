import { ConnectionException, DeadlockException, LockWaitTimeoutException } from '@mikro-orm/core';
import { TransientError } from '@application/errors';

const TRANSIENT_PG_CODES = new Set([
  '57P01', '57P02', '57P03', // admin shutdown / crash / cannot connect now
  '08000', '08001', '08003', '08004', '08006', // connection exceptions
  '53300', '53200', // too many connections / out of memory
  '40001', '40P01', '55P03', // serialization failure / deadlock / lock not available
]);
const TRANSIENT_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);
const TRANSIENT_MESSAGE =
  /timeout (exceeded )?(when|acquiring)|connection (terminated|refused|ended)|terminating connection|Connection terminated/i;

/**
 * Falha de infraestrutura que provavelmente passa sozinha (banco indisponível, deadlock, lock timeout,
 * pool esgotado, rede). Usada pela API (503) e pelo consumer (retry com backoff) — um só critério.
 */
export function isTransientInfrastructureError(e: unknown): boolean {
  if (e instanceof TransientError) return true;
  if (e instanceof ConnectionException || e instanceof DeadlockException || e instanceof LockWaitTimeoutException) {
    return true;
  }
  const err = e as { code?: unknown; cause?: unknown; message?: unknown } | null;
  if (typeof err?.code === 'string' && (TRANSIENT_PG_CODES.has(err.code) || TRANSIENT_NODE_CODES.has(err.code))) {
    return true;
  }
  if (typeof err?.message === 'string' && TRANSIENT_MESSAGE.test(err.message)) return true;
  return err?.cause ? isTransientInfrastructureError(err.cause) : false;
}
