import { randomUUID } from 'node:crypto';
import {
  type CallHandler,
  type CanActivate,
  Catch,
  type ExceptionFilter,
  type ExecutionContext,
  type ArgumentsHost,
  Inject,
  Injectable,
  type NestInterceptor,
  SetMetadata,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { Observable } from 'rxjs';
import type { Services } from '@infra/composition';
import { SERVICES } from './tokens';
import { mapError } from './error-mapper';

declare module 'fastify' {
  interface FastifyRequest {
    correlationId?: string;
  }
}

// ───────────── autenticação: ponto de extensão (decisão documentada no ARCHITECTURE.md) ─────────────

export const IS_PUBLIC = 'isPublic';
/** Marca rotas abertas (health e metrics). Hoje todas as rotas passam pelo guard no-op. */
export const Public = () => SetMetadata(IS_PUBLIC, true);

/**
 * No-op deliberado. Desenho alvo: validar um JWT emitido por um IdP externo (Keycloak/Zitadel)
 * via JWKS e extrair o `providerId` do token, comparando com `providerId` do payload
 * (através de uma porta `ProviderIdentityPort`, AINDA A CRIAR: não existe no código).
 * Rotas `@Public()` (health e metrics) não exigem autenticação. Ver ARCHITECTURE.md, seção 8.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(@Inject(Reflector) private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    return true; // TODO(auth): verificar credenciais aqui
  }
}

// ───────────── correlation id ─────────────

export const CORRELATION_HEADER = 'x-correlation-id';

@Injectable()
export class CorrelationInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const req = context.switchToHttp().getRequest<FastifyRequest>();
    const res = context.switchToHttp().getResponse<FastifyReply>();
    const incoming = req.headers[CORRELATION_HEADER];
    req.correlationId = typeof incoming === 'string' && incoming.length > 0 && incoming.length <= 128 ? incoming : randomUUID();
    void res.header(CORRELATION_HEADER, req.correlationId);
    return next.handle();
  }
}

// ───────────── erros ─────────────

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(@Inject(SERVICES) private readonly services: Services) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const res = http.getResponse<FastifyReply>();
    const req = http.getRequest<FastifyRequest>();
    const mapped = mapError(exception);

    if (mapped.status >= 500) {
      this.services.logger.error('request failed', {
        correlationId: req.correlationId,
        method: req.method,
        url: req.url?.split('?')[0],
        status: mapped.status,
        errorName: exception instanceof Error ? exception.name : typeof exception,
        error: exception instanceof Error ? exception.message : String(exception),
      });
    }
    this.services.metrics.increment('http_errors_total', { status: String(mapped.status) });

    for (const [k, v] of Object.entries(mapped.headers ?? {})) void res.header(k, v);
    if (req.correlationId) void res.header(CORRELATION_HEADER, req.correlationId);
    void res.status(mapped.status).send({ ...mapped.body, ...(req.correlationId ? { correlationId: req.correlationId } : {}) });
  }
}
