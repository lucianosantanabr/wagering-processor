import { exponentialBackoffMs } from '@domain/backoff';

/**
 * Política de espera por referências fora de ordem (item 7.1 do desafio).
 *
 * Backoff exponencial: baseMs * 2^tentativa, limitado por capMs. Com os padrões (2 s, teto 5 min, 15
 * tentativas) a espera total é de ~43 min: 2+4+8+16+32+64+128+256 s e depois 7 x 300 s.
 *
 * Por que um limite de TENTATIVAS e não só um TTL: o custo de cada tentativa é uma leitura indexada
 * e uma transação curta, então o limite controla diretamente a carga gerada por referências que nunca
 * chegam. Por que ~43 min: reordenação de entrega ocorre em segundos/minutos; uma referência que não
 * chegou em dezenas de minutos indica mensagem perdida, não atraso. Nesse caso a reversão é
 * `REJECTED` com `REFERENCE_NOT_FOUND` e o provedor pode reenviar a original e a reversão.
 */
export interface ReferencePolicy {
  baseMs: number;
  capMs: number;
  /** Tentativas de reprocessamento antes de rejeitar com REFERENCE_NOT_FOUND. */
  maxAttempts: number;
}

export const DEFAULT_REFERENCE_POLICY: ReferencePolicy = { baseMs: 2_000, capMs: 5 * 60_000, maxAttempts: 15 };

/** Quando fazer a próxima tentativa, dado quantas já foram feitas (0 = ainda nenhuma). */
export function nextReferenceAttemptAt(now: Date, attemptsDone: number, policy: ReferencePolicy): Date {
  return new Date(now.getTime() + exponentialBackoffMs(attemptsDone, policy.baseMs, policy.capMs));
}
