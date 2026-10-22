import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res } from '@nestjs/common';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { SERVICES } from './tokens';
import type { Services } from '@infra/composition';
import { WagerTransactionStatus } from '@domain/wager-transaction';
import type { SubmitWagerTransactionResult } from '@application/submit-wager-transaction';
import { parseCreateWallet, parseSubmitTransaction } from './dto';
import { ORM } from './tokens';
import { Public } from './http.infra';
import type { MikroORM } from '@mikro-orm/postgresql';

// ───────────────────────────── wallets ─────────────────────────────

@Controller('wallets')
export class WalletsController {
  constructor(@Inject(SERVICES) private readonly s: Services) {}

  @Post()
  @HttpCode(201)
  create(@Body() body: unknown, @Req() req: FastifyRequest) {
    return this.s.createWallet.execute(parseCreateWallet(body, req.correlationId));
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: string) {
    return this.s.getWallet.execute(walletId);
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string) {
    return this.s.getLedger.execute({ walletId, cursor, limit });
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId') walletId: string) {
    return this.s.reconcileWallet.execute(walletId);
  }
}

// ───────────────────────────── wagering ─────────────────────────────

/**
 * Status HTTP do POST (consistente com o mapeamento de erros em error-mapper.ts):
 *   201 criada e PROCESSED · 200 replay idempotente PROCESSED · 202 PENDING_REFERENCE
 *   422 REJECTED por regra de negócio (corpo traz failureCode) · 500 FAILED
 * Nota: um GET numa transação REJECTED devolve 200 — é a representação de um recurso, não um erro.
 */
function submitStatus(r: SubmitWagerTransactionResult): number {
  switch (r.status) {
    case WagerTransactionStatus.Processed:
      return r.idempotentReplay ? 200 : 201;
    case WagerTransactionStatus.PendingReference:
    case WagerTransactionStatus.Pending:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    default:
      return 500;
  }
}

@Controller()
export class WageringController {
  constructor(@Inject(SERVICES) private readonly s: Services) {}

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Req() req: FastifyRequest,
    @Res({ passthrough: true }) res: FastifyReply,
  ) {
    const result = await this.s.submitWagerTransaction.execute(
      parseSubmitTransaction(body, idempotencyKey, req.correlationId),
    );
    const status = submitStatus(result);
    void res.status(status);
    if (status === 422) {
      return { code: 'TRANSACTION_REJECTED', message: String(result.failureCode), ...result };
    }
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  byId(@Param('transactionId') transactionId: string) {
    return this.s.getTransaction.byId(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  byExternal(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    return this.s.getTransaction.byProviderExternalId(providerId, externalTransactionId);
  }
}

// ───────────────────────────── metrics ─────────────────────────────

export interface MetricsExposer {
  contentType(): string;
  render(): Promise<string>;
}

/** Exposição Prometheus. Aberta como o health (é scrapeada pela infraestrutura, não por provedores). */
@Controller('metrics')
@Public()
export class MetricsController {
  constructor(@Inject('METRICS_EXPOSER') private readonly exposer: MetricsExposer | undefined) {}

  @Get()
  async metrics(@Res({ passthrough: true }) res: FastifyReply): Promise<string> {
    if (!this.exposer) {
      void res.status(404);
      return 'metrics are not enabled';
    }
    void res.header('content-type', this.exposer.contentType());
    return this.exposer.render();
  }
}

// ───────────────────────────── health ─────────────────────────────

export type ReadinessCheck = { name: string; check: () => Promise<void> };

const withTimeout = <T>(p: Promise<T>, ms: number): Promise<T> =>
  Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);

@Controller('health')
@Public()
export class HealthController {
  constructor(
    @Inject(ORM) private readonly orm: MikroORM,
    @Inject('READINESS_CHECKS') private readonly extraChecks: ReadinessCheck[],
  ) {}

  /** Processo vivo; não toca dependências. */
  @Get('live')
  live() {
    return { status: 'ok' };
  }

  /** Dependências alcançáveis (PostgreSQL e, quando configurado, SQS). 503 se alguma falhar. */
  @Get('ready')
  async ready(@Res({ passthrough: true }) res: FastifyReply) {
    const checks: ReadinessCheck[] = [
      { name: 'postgres', check: async () => void (await this.orm.em.getConnection().execute('select 1')) },
      ...this.extraChecks,
    ];
    const entries = await Promise.all(
      checks.map(async (c) => {
        try {
          await withTimeout(c.check(), 2000);
          return [c.name, 'up'] as const;
        } catch {
          return [c.name, 'down'] as const;
        }
      }),
    );
    const ok = entries.every(([, s]) => s === 'up');
    if (!ok) void res.status(503);
    return { status: ok ? 'ok' : 'unavailable', checks: Object.fromEntries(entries) };
  }
}
