/** Backoff exponencial determinístico: base * 2^attempt, limitado por `capMs`. `attempt` começa em 0. */
export function exponentialBackoffMs(attempt: number, baseMs: number, capMs: number): number {
  const exp = Math.min(attempt, 30); // evita overflow de 2 ** n
  return Math.min(baseMs * 2 ** exp, capMs);
}
