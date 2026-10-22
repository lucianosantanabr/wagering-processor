import { Money } from '@domain/money';
import {
  type CreateWagerTransactionProps,
  WagerTransaction,
  WagerTransactionKind,
} from '@domain/wager-transaction';
import { Wallet } from '@domain/wallet';

export const NOW = new Date('2026-07-29T15:00:00.000Z');
export const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

export function openWallet(balance = '100.00', currency = 'BRL'): Wallet {
  return Wallet.open({
    id: 'wallet-1',
    playerId: 'player-1',
    initialBalance: Money.from({ amount: balance, currency }),
    now: NOW,
  });
}

let seq = 0;
export function makeTx(
  overrides: Partial<CreateWagerTransactionProps> = {},
): WagerTransaction {
  seq += 1;
  return WagerTransaction.create({
    id: `tx-${seq}`,
    providerId: 'provider-a',
    externalTransactionId: `ext-${seq}`,
    idempotencyKey: `provider-a:ext-${seq}`,
    payloadHash: 'hash',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: brl('25.00'),
    now: NOW,
    ...overrides,
  });
}
