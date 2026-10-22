import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD, APP_INTERCEPTOR, NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { Services } from '@infra/composition';
import {
  HealthController,
  type MetricsExposer,
  MetricsController,
  type ReadinessCheck,
  WageringController,
  WalletsController,
} from './controllers';
import { AllExceptionsFilter, AuthGuard, CorrelationInterceptor } from './http.infra';
import { ORM, SERVICES } from './tokens';

export interface AppDeps {
  orm: MikroORM;
  services: Services;
  /** Checks de prontidão adicionais (ex.: SQS). */
  readinessChecks?: ReadinessCheck[];
  /** Quando presente, habilita GET /metrics (formato Prometheus). */
  metricsExposer?: MetricsExposer;
}

/** Monta o app NestJS/Fastify sobre dependências já construídas (composition root externa). */
export async function createApp(deps: AppDeps): Promise<NestFastifyApplication> {
  @Module({
    controllers: [WalletsController, WageringController, HealthController, MetricsController],
    providers: [
      { provide: SERVICES, useValue: deps.services },
      { provide: ORM, useValue: deps.orm },
      { provide: 'READINESS_CHECKS', useValue: deps.readinessChecks ?? [] },
      { provide: 'METRICS_EXPOSER', useValue: deps.metricsExposer },
      { provide: APP_GUARD, useClass: AuthGuard },
      { provide: APP_INTERCEPTOR, useClass: CorrelationInterceptor },
      { provide: APP_FILTER, useClass: AllExceptionsFilter },
    ],
  })
  class AppModule {}

  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter(), { logger: false });
  // Métrica e log de cada requisição, no fim da resposta. O rótulo `route` é o PADRÃO da rota
  // (/wallets/:walletId), nunca a URL real: cardinalidade limitada e sem ids nem query string.
  const { logger, metrics } = deps.services;
  app.getHttpAdapter().getInstance().addHook('onResponse', (req, reply, done) => {
    const route = req.routeOptions?.url ?? 'unmatched';
    const status = String(reply.statusCode);
    metrics.observe('http_request_duration_seconds', reply.elapsedTime / 1000, { method: req.method, route, status });
    if (route !== '/health/live' && route !== '/metrics') {
      logger.info('http request', {
        correlationId: req.correlationId,
        method: req.method,
        route,
        status: reply.statusCode,
        durationMs: Math.round(reply.elapsedTime),
      });
    }
    done();
  });

  await app.init();
  await app.getHttpAdapter().getInstance().ready();
  return app;
}
