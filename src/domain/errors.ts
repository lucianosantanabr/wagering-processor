export abstract class DomainError extends Error {
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidMoneyError extends DomainError {
  readonly code = 'INVALID_MONEY';
}

export class CurrencyMismatchError extends DomainError {
  readonly code = 'CURRENCY_MISMATCH';
}

export class InsufficientFundsError extends DomainError {
  readonly code = 'INSUFFICIENT_FUNDS';
}

/** Violação de invariante ao construir/mutar um agregado: erro de programação ou de contrato de entrada. */
export class InvalidWalletError extends DomainError {
  readonly code = 'INVALID_WALLET';
}

export class InvalidLedgerEntryError extends DomainError {
  readonly code = 'INVALID_LEDGER_ENTRY';
}

export class InvalidWagerTransactionError extends DomainError {
  readonly code = 'INVALID_WAGER_TRANSACTION';
}

/** Tentativa de transição a partir de um estado terminal (ou não permitida): erro de programação, não caminho de negócio. */
export class InvalidTransactionStateError extends DomainError {
  readonly code = 'INVALID_TRANSACTION_STATE';
}

export class InvalidInboxStateError extends DomainError {
  readonly code = 'INVALID_INBOX_STATE';
}

export class InvalidOutboxStateError extends DomainError {
  readonly code = 'INVALID_OUTBOX_STATE';
}
