# syntax=docker/dockerfile:1
# Imagem de runtime do serviço. O Bun executa o TypeScript direto: não há etapa de build/transpilação.

FROM oven/bun:1.4.3-alpine AS deps
WORKDIR /app
COPY package.json bun.lock ./
# Só dependências de produção (sem @types, @nestjs/testing, CLI do ORM...). Lockfile congelado: build reproduzível.
RUN bun install --frozen-lockfile --production

FROM oven/bun:1.4.3-alpine
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000
COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json ./
COPY src ./src

# Não roda como root.
USER bun
EXPOSE 3000

# Liveness (processo vivo). A readiness (/health/ready) depende de PostgreSQL/SQS e é para o orquestrador/LB.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD bun -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/health/live').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

# `exec` form: o Bun é o PID 1 e recebe SIGTERM diretamente (desligamento gracioso).
CMD ["bun", "src/main.ts"]
