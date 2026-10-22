/**
 * Taxonomia estável e legível por máquina. A coluna "Provedor deve" orienta a decisão de reenvio.
 *
 * REJECTED (regra de negócio; terminal):
 *  INSUFFICIENT_FUNDS         BET sem saldo                                   -> não reenviar igual; pode tentar novamente depois com nova transação
 *  REVERSAL_WOULD_OVERDRAW    reversão deixaria o saldo negativo              -> não reenviar; caso operacional
 *  REFERENCE_NOT_FOUND        referência nunca chegou (limite esgotado)       -> corrigir a referência / reenviar a original e uma nova reversão
 *  REFERENCE_NOT_PROCESSED    referência existe mas foi rejeitada/falhou      -> não reenviar
 *  REFERENCE_MISMATCH         provider/player/wallet/moeda/rodada divergem    -> corrigir payload
 *  INVALID_REFERENCE_KIND     REFUND de não-BET; ROLLBACK de LOSS/ROLLBACK    -> corrigir payload
 *  ALREADY_REVERSED           referência já revertida por este tipo           -> desistir (já tratado)
 *  AMOUNT_MISMATCH            valor diferente do da referência                -> corrigir payload
 *  WALLET_MISMATCH            wallet inexistente ou não pertence ao player    -> corrigir payload
 *  CURRENCY_MISMATCH          moeda da operação difere da moeda da wallet     -> corrigir payload
 *
 * FAILED (infraestrutura permanente; terminal e auditável):
 *  PROCESSING_ERROR           erro permanente inesperado ao processar
 *  RETRIES_EXHAUSTED          tentativas transitórias esgotadas
 */
export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalWouldOverdraw = 'REVERSAL_WOULD_OVERDRAW',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',
  AlreadyReversed = 'ALREADY_REVERSED',
  AmountMismatch = 'AMOUNT_MISMATCH',
  WalletMismatch = 'WALLET_MISMATCH',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  ProcessingError = 'PROCESSING_ERROR',
  RetriesExhausted = 'RETRIES_EXHAUSTED',
}
