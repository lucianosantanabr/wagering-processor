import { DomainError } from '@domain/errors';
import type { FailureCode } from '@domain/failure-code';
import { InboxMessage } from '@domain/inbox-message';
import { Money, type MoneyProps } from '@domain/money';
import { OutboxMessage } from '@domain/outbox-message';
import { computePayloadHash } from '@domain/payload-hash';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '@domain/wager-transaction';
import { type Logger, type Metrics, noopLogger, noopMetrics } from './observability';
import { DEFAULT_REFERENCE_POLICY, type ReferencePolicy, nextReferenceAttemptAt } from './reference-policy';
import type { IntegrationEvent } from '@domain/integration-event';
import {
  DuplicateKeyError,
  IdempotencyConflictError,
  MessageIdReuseError,
  ValidationError,
  WalletNotFoundError,
} from './errors';
import type { Clock, IdGenerator, Repositories, StoredTransaction, UnitOfWork } from './ports';
import type { WagerProcessor } from './wager-processor';

export interface SubmitWagerTransactionCommand {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  /** Presente quando a entrada é uma mensagem SQS: dedup persistente por (consumerName, messageId). */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

export interface SubmitWagerTransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: FailureCode;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SUBMITTABLE_KINDS: ReadonlySet<string> = new Set([
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
]);
const MAX_ATTEMPTS = 3;

/** Único caminho de escrita financeira: usado pela API HTTP e pelo consumer SQS. */
export class SubmitWagerTransaction {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly processor: WagerProcessor,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly referencePolicy: ReferencePolicy = DEFAULT_REFERENCE_POLICY,
    private readonly logger: Logger = noopLogger,
    private readonly metrics: Metrics = noopMetrics,
  ) {}

  async execute(cmd: SubmitWagerTransactionCommand): Promise<SubmitWagerTransactionResult> {
    const source = cmd.inbox ? 'sqs' : 'http';
    const started = performance.now();
    try {
      const result = await this.executeOnce(cmd);
      this.recordOutcome(cmd, source, started, result);
      return result;
    } catch (e) {
      this.metrics.observe('wager_transaction_duration_seconds', (performance.now() - started) / 1000, {
        source,
        result: 'error',
      });
      if (e instanceof IdempotencyConflictError) this.metrics.increment('idempotency_conflicts_total', { source });
      throw e;
    }
  }

  /** Métricas e log DEPOIS do commit (o que aconteceu de fato). O log leva ids e resultado, nunca valores. */
  private recordOutcome(
    cmd: SubmitWagerTransactionCommand,
    source: string,
    started: number,
    result: SubmitWagerTransactionResult,
  ): void {
    this.metrics.observe('wager_transaction_duration_seconds', (performance.now() - started) / 1000, {
      source,
      result: result.idempotentReplay ? 'replay' : 'new',
    });
    if (result.idempotentReplay) {
      this.metrics.increment('duplicates_detected_total', { source });
    } else {
      this.metrics.increment('wager_transactions_total', { kind: cmd.kind, status: result.status });
    }
    this.logger.info('wager transaction handled', {
      correlationId: cmd.correlationId,
      messageId: cmd.inbox?.messageId,
      transactionId: result.transactionId,
      walletId: cmd.walletId,
      providerId: cmd.providerId,
      kind: cmd.kind,
      status: result.status,
      failureCode: result.failureCode,
      idempotentReplay: result.idempotentReplay,
      source,
    });
  }

  private async executeOnce(cmd: SubmitWagerTransactionCommand): Promise<SubmitWagerTransactionResult> {
    const money = this.validate(cmd);
    const payloadHash = computePayloadHash({ ...cmd, money: money.toJSON() });

    for (let attempt = 1; ; attempt++) {
      try {
        return await this.uow.run((repos) => this.handle(repos, cmd, money, payloadHash));
      } catch (e) {
        // Corrida de inserts com a mesma chave: na nova transação a linha já existe (replay/conflito).
        if (e instanceof DuplicateKeyError && attempt < MAX_ATTEMPTS) {
          this.metrics.increment('idempotency_race_retries_total');
          continue;
        }
        if (e instanceof DuplicateKeyError) {
          throw new IdempotencyConflictError(`idempotency conflict on ${e.constraint}`);
        }
        throw e;
      }
    }
  }

  private async handle(
    repos: Repositories,
    cmd: SubmitWagerTransactionCommand,
    money: Money,
    payloadHash: string,
  ): Promise<SubmitWagerTransactionResult> {
    const now = this.clock.now();
    let markInbox = false;
    if (cmd.inbox) {
      const receipt = await repos.inbox.tryReceive(InboxMessage.receive({ ...cmd.inbox, now }));
      if (receipt.isNew) {
        markInbox = true; // 1ª vez: ao final desta MESMA transação marcamos processed_at
      } else if (receipt.existing?.payloadHash !== cmd.inbox.payloadHash) {
        // Mesmo messageId, conteúdo diferente do registrado: uma releitura traz o MESMO corpo, então isto é um
        // defeito do emissor. Não processa nada (o registro original da inbox permanece como está).
        this.metrics.increment('inbox_conflicts_total');
        throw new MessageIdReuseError(
          `messageId ${cmd.inbox.messageId} was already received with a different payload`,
        );
      }
      // Releitura legítima (mesmo corpo): a inbox e o efeito são gravados na MESMA transação, então se a linha da
      // inbox existe a transação com esta idempotency key também existe; a busca abaixo devolve o resultado
      // original (replay) sem tocar em mais nada nem tomar o lock da wallet.
    }
    const finish = async <T>(result: T): Promise<T> => {
      if (markInbox && cmd.inbox) {
        await repos.inbox.markProcessed(cmd.inbox.consumerName, cmd.inbox.messageId, this.clock.now());
      }
      return result;
    };

    // Caminho rápido: replay não precisa do lock da wallet.
    const early = await this.findExisting(repos, cmd, payloadHash);
    if (early) return finish(this.replay(early));

    // Serializa todas as operações da wallet. Duplicatas concorrentes esperam aqui e, ao
    // obter o lock, enxergam a transação já commitada (READ COMMITTED reavalia o snapshot).
    const wallet = await repos.wallets.findByIdForUpdate(cmd.walletId);
    if (!wallet) throw new WalletNotFoundError(`wallet ${cmd.walletId} not found`);

    const late = await this.findExisting(repos, cmd, payloadHash);
    if (late) return finish(this.replay(late));

    let tx: WagerTransaction;
    try {
      tx = WagerTransaction.create({
        id: this.ids.next(),
        providerId: cmd.providerId,
        externalTransactionId: cmd.externalTransactionId,
        idempotencyKey: cmd.idempotencyKey,
        payloadHash,
        walletId: cmd.walletId,
        playerId: cmd.playerId,
        roundId: cmd.roundId,
        gameId: cmd.gameId,
        kind: cmd.kind as Exclude<WagerTransactionKind, WagerTransactionKind.Opening>,
        money,
        referenceExternalTransactionId: cmd.referenceExternalTransactionId,
        now,
      });
    } catch (e) {
      if (e instanceof DomainError) throw new ValidationError(e.message);
      throw e;
    }

    const previousVersion = wallet.version;
    const correlationId = cmd.correlationId ?? this.ids.next();
    const { entry, events } = await this.processor.apply(tx, wallet, repos, {
      correlationId,
      causationId: cmd.inbox?.messageId ?? cmd.idempotencyKey,
    });

    // Ordem importa (FKs imediatas): transação -> saldo -> lançamento -> outbox.
    // Referência ausente: agenda a 1ª nova tentativa do worker (ver reference-policy.ts).
    const retry =
      tx.status === WagerTransactionStatus.PendingReference
        ? { attempts: 0, nextAttemptAt: nextReferenceAttemptAt(now, 0, this.referencePolicy) }
        : undefined;
    await repos.transactions.insert(tx, wallet.balance, retry);
    if (entry) {
      await repos.wallets.saveBalance(wallet, previousVersion);
      await repos.ledger.insert(entry);
    }
    await this.enqueue(repos, events);

    return finish({
      transactionId: tx.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    });
  }

  /**
   * Procura a transação por idempotency key e por (provider, id externo).
   * Mesmo payload => replay. Payload diferente (ou id externo já usado com outra chave) => conflito.
   */
  private async findExisting(
    repos: Repositories,
    cmd: SubmitWagerTransactionCommand,
    payloadHash: string,
  ): Promise<StoredTransaction | null> {
    const byKey = await repos.transactions.findByIdempotencyKey(cmd.idempotencyKey);
    if (byKey) {
      if (!byKey.tx.matchesPayload(payloadHash)) {
        throw new IdempotencyConflictError(
          `idempotency key ${cmd.idempotencyKey} was already used with a different payload`,
        );
      }
      return byKey;
    }
    const byExternal = await repos.transactions.findByProviderExternalId(cmd.providerId, cmd.externalTransactionId);
    if (byExternal) {
      // As duas buscas são statements separados: se a transação vencedora comitou ENTRE elas, a 1ª não a viu
      // e a 2ª sim. Com a MESMA idempotency key é o mesmo pedido (replay, ou conflito de payload), não um
      // conflito de chaves. (Sob o lock da wallet isso não ocorre; no caminho rápido, sem lock, ocorre.)
      if (byExternal.tx.idempotencyKey === cmd.idempotencyKey) {
        if (!byExternal.tx.matchesPayload(payloadHash)) {
          throw new IdempotencyConflictError(
            `idempotency key ${cmd.idempotencyKey} was already used with a different payload`,
          );
        }
        return byExternal;
      }
      throw new IdempotencyConflictError(
        `external transaction ${cmd.providerId}/${cmd.externalTransactionId} already exists under another idempotency key`,
      );
    }
    return null;
  }

  private replay(stored: StoredTransaction): SubmitWagerTransactionResult {
    return {
      transactionId: stored.tx.id,
      status: stored.tx.status,
      balance: stored.balanceSnapshot.toJSON(),
      idempotentReplay: true,
      ...(stored.tx.failureCode ? { failureCode: stored.tx.failureCode } : {}),
    };
  }

  private async enqueue(repos: Repositories, events: IntegrationEvent<unknown>[]): Promise<void> {
    for (const event of events) await repos.outbox.enqueue(OutboxMessage.enqueue(event));
  }

  /** Validação de contrato de entrada: tudo que falhar aqui é 400 e nunca chega ao banco. */
  private validate(cmd: SubmitWagerTransactionCommand): Money {
    const text = { providerId: cmd.providerId, externalTransactionId: cmd.externalTransactionId, idempotencyKey: cmd.idempotencyKey, roundId: cmd.roundId, gameId: cmd.gameId };
    for (const [field, value] of Object.entries(text)) {
      if (typeof value !== 'string' || value.trim() === '' || value.length > 255) {
        throw new ValidationError(`${field} must be a non-empty string up to 255 chars`);
      }
    }
    if (cmd.providerId === 'internal') throw new ValidationError('providerId "internal" is reserved');
    if (!UUID.test(cmd.walletId ?? '')) throw new ValidationError('walletId must be a UUID');
    if (!UUID.test(cmd.playerId ?? '')) throw new ValidationError('playerId must be a UUID');
    if (!SUBMITTABLE_KINDS.has(cmd.kind)) throw new ValidationError(`kind "${cmd.kind}" is not submittable`);
    try {
      return Money.from(cmd.money);
    } catch (e) {
      if (e instanceof DomainError) throw new ValidationError(e.message);
      throw e;
    }
  }
}
