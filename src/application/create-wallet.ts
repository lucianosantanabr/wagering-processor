import { DomainError } from '@domain/errors';
import { WagerTransactionProcessed, WalletBalanceChanged } from '@domain/events';
import { Money, type MoneyProps } from '@domain/money';
import { OutboxMessage } from '@domain/outbox-message';
import { sha256Canonical } from '@domain/payload-hash';
import { WagerTransaction } from '@domain/wager-transaction';
import { Wallet } from '@domain/wallet';
import { DuplicateKeyError, ValidationError, WalletAlreadyExistsError } from './errors';
import type { Clock, IdGenerator, UnitOfWork } from './ports';

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId?: string;
}

export interface CreateWalletResult {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class CreateWallet {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async execute(cmd: CreateWalletCommand): Promise<CreateWalletResult> {
    if (!UUID.test(cmd.playerId ?? '')) throw new ValidationError('playerId must be a UUID');
    let initial: Money;
    try {
      initial = Money.from(cmd.initialBalance);
    } catch (e) {
      if (e instanceof DomainError) throw new ValidationError(e.message);
      throw e;
    }

    try {
      return await this.uow.run(async (repos) => {
        if (await repos.wallets.findByPlayerAndCurrency(cmd.playerId, initial.currency)) {
          throw new WalletAlreadyExistsError(`player ${cmd.playerId} already has a ${initial.currency} wallet`);
        }
        const now = this.clock.now();
        const wallet = Wallet.open({ id: this.ids.next(), playerId: cmd.playerId, initialBalance: initial, now });
        await repos.wallets.insert(wallet);

        // Saldo inicial > 0: transação OPENING + lançamento CREDIT na MESMA transação SQL.
        if (initial.isPositive()) {
          const correlationId = cmd.correlationId ?? this.ids.next();
          const opening = WagerTransaction.opening({
            id: this.ids.next(),
            payloadHash: sha256Canonical({ walletId: wallet.id, money: initial.toJSON() }),
            walletId: wallet.id,
            playerId: wallet.playerId,
            money: initial,
            now,
          });
          opening.markProcessed(undefined, now);
          const entry = wallet.openingEntry({ entryId: this.ids.next(), transactionId: opening.id });

          await repos.transactions.insert(opening, wallet.balance);
          await repos.ledger.insert(entry);
          const ctx = () => ({ eventId: this.ids.next(), correlationId, occurredAt: now });
          await repos.outbox.enqueue(OutboxMessage.enqueue(WagerTransactionProcessed.from(opening, ctx())));
          await repos.outbox.enqueue(OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, entry, ctx())));
        }

        return {
          id: wallet.id,
          playerId: wallet.playerId,
          balance: wallet.balance.toJSON(),
          version: wallet.version,
        };
      });
    } catch (e) {
      // Corrida: duas criações simultâneas para o mesmo (player, moeda); a UNIQUE do banco decide.
      if (e instanceof DuplicateKeyError) {
        throw new WalletAlreadyExistsError(`player ${cmd.playerId} already has a ${initial.currency} wallet`);
      }
      throw e;
    }
  }
}
