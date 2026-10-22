/** Erros de aplicação: o chamador (HTTP/SQS) os traduz para status/ação conforme a taxonomia. */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Payload inválido (formato, valores, OPENING etc.). Não reenviar sem corrigir. */
export class ValidationError extends ApplicationError {
  readonly code = 'VALIDATION_ERROR';
}

/** Wallet inexistente. Não persistimos transação para ela. */
export class WalletNotFoundError extends ApplicationError {
  readonly code = 'WALLET_NOT_FOUND';
}

export class TransactionNotFoundError extends ApplicationError {
  readonly code = 'TRANSACTION_NOT_FOUND';
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = 'WALLET_ALREADY_EXISTS';
}

/** Mesma idempotency key (ou mesmo id externo) com payload diferente: conflito, não replay. */
export class IdempotencyConflictError extends ApplicationError {
  readonly code = 'IDEMPOTENCY_CONFLICT';
}

/**
 * O mesmo `messageId` chegou com conteúdo DIFERENTE do já registrado na inbox. Uma releitura (redelivery) traz
 * exatamente o mesmo corpo; conteúdo diferente sob o mesmo id é um defeito do emissor. Nenhuma nova tentativa
 * resolve: erro permanente (DLQ), e NADA é processado.
 */
export class MessageIdReuseError extends ApplicationError {
  readonly code = 'MESSAGE_ID_REUSED';
}

/**
 * Violação de unicidade ao inserir (corrida entre duas transações com a mesma chave).
 * O use case reexecuta: na nova transação a linha já existe e o fluxo vira replay/conflito.
 */
export class DuplicateKeyError extends ApplicationError {
  readonly code = 'DUPLICATE_KEY';
  constructor(
    readonly constraint: string,
    message = `duplicate key on ${constraint}`,
  ) {
    super(message);
  }
}

/** Falha transitória (banco indisponível, lock/serialização esgotados): seguro reenviar. */
export class TransientError extends ApplicationError {
  readonly code = 'TRANSIENT_ERROR';
}
