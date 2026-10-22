# Jungle Wagering Processor

Processador distribuído de apostas (iGaming). Recebe transações `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK` de múltiplos provedores, por HTTP e por fila SQS, com entrega *at-least-once*. Garante que não há crédito ou débito duplicado, saldo negativo ou evento perdido, mesmo com mensagens duplicadas, fora de ordem, processos que morrem no meio e várias instâncias concorrentes. Saldo e ledger são atualizados na mesma transação SQL, e as garantias são aplicadas no schema do banco.

**Stack:** Bun · TypeScript (strict) · NestJS (Fastify) · PostgreSQL · MikroORM · AWS SQS (LocalStack) · Docker/Podman Compose.

> **Documentação**
> - [`ARCHITECTURE.md`](ARCHITECTURE.md): **diagrama da arquitetura**, fluxo das requisições, decisões, trade-offs e limitações.
> - Este arquivo: setup, execução, API e testes.

Verificação: o fluxo das seções 2 e 5 (subir a pilha com o app em container, criar wallet, apostar com replay, enviar a
mesma mensagem SQS 3 vezes, reconciliar, ler as métricas) e o perfil `cluster` foram executados de ponta a ponta com
`podman compose`; os scripts `stack:*`/`infra:*` do `package.json` são esses mesmos comandos com `docker compose`
(em Docker é só trocar o binário). Os testes da seção 7 rodam com Podman ou Docker.

## 1. Pré-requisitos

| Item | Versão | Observação |
|---|---|---|
| [Bun](https://bun.sh) | 1.4.x | runtime, gerenciador de pacotes e test runner. Node **não** é necessário |
| Docker **ou** Podman | Docker 24+ / Podman 5+ | com `compose`. O mesmo `docker-compose.yml` serve aos dois |

Podman: a máquina precisa estar de pé (`podman machine start`); use `podman compose ...` no lugar de `docker compose ...`.

## 2. Subir tudo (o caminho mais curto)

```bash
bun run stack:up        # = docker compose up -d --build --wait
```

Sobe **PostgreSQL 16**, **LocalStack** (SQS) e **1 instância do app** em `http://localhost:3000`. O app aplica as migrations sozinho e cria as filas na primeira necessidade. `--wait` só retorna quando tudo está `healthy`.

```bash
curl -s http://localhost:3000/health/ready
# {"status":"ok","checks":{"postgres":"up","sqs":"up","sqs-events":"up"}}
```

Para ver **várias instâncias** (mesmo banco, mesmas filas) — é o cenário de concorrência do desafio:

```bash
bun run stack:cluster   # = docker compose --profile cluster up -d --build --wait
# app em :3000, :3001 e :3002
```

Encerrar e apagar os volumes: `bun run infra:down`. Logs: `docker compose logs -f app` (uma linha JSON por evento).

> Com Podman, troque `docker compose` por `podman compose` nos comandos acima (os scripts do `package.json` usam `docker compose`; rode o comando equivalente diretamente).

## 3. Rodar o app localmente (sem container para o app)

```bash
bun install
bun run infra:up                       # só PostgreSQL (porta 5433) e LocalStack (4566)
cp .env.example .env                   # ajuste se necessário
set -a; source .env; set +a            # PowerShell: veja a nota abaixo
bun run start                          # http://localhost:3000
```

PowerShell: carregue o `.env` com
`Get-Content .env | % { $k,$v = $_ -split '=',2; Set-Item "env:$k" $v }`.

A porta do PostgreSQL é **5433** no host (a 5432 costuma estar ocupada por instalações locais). Dentro da rede do compose o app usa `postgres:5432`.

## 4. Variáveis de ambiente

| Variável | Padrão | O que faz |
|---|---|---|
| `DATABASE_URL` | — (**obrigatória**) | conexão PostgreSQL |
| `PORT` | `3000` | porta HTTP |
| `LOG_LEVEL` | `info` | nível dos logs JSON (`debug`, `info`, `warn`, `error`) |
| `AWS_REGION` / `AWS_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `us-east-1` / — / `test` / `test` | cliente SQS. `AWS_ENDPOINT_URL` aponta para o LocalStack; **omita na AWS real** |
| `SQS_QUEUE_NAME` / `SQS_DLQ_NAME` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` | fila de entrada e sua DLQ |
| `SQS_EVENTS_QUEUE_NAME` | `wager-events.fifo` | fila onde a outbox publica os eventos de integração |
| `SQS_MAX_RECEIVE_COUNT` | `5` | tentativas antes de mover a mensagem para a DLQ |
| `SQS_VISIBILITY_TIMEOUT_SECONDS` | `30` | tempo de "em voo" de uma mensagem; deve exceder o pior tempo de processamento |
| `OUTBOX_IDLE_INTERVAL_MS` | `500` | intervalo de polling do publisher da outbox quando não há trabalho |
| `OUTBOX_PUBLISHER_ENABLED` | `true` | `false` desliga o publisher da outbox naquela instância (réplicas só de API/consumer). Os eventos continuam sendo gravados atomicamente e ficam pendentes até alguma instância com o publisher ligado |
| `PENDING_REFERENCE_BASE_MS` / `_CAP_MS` / `_MAX_ATTEMPTS` | `2000` / `300000` / `15` | espera por referências fora de ordem (≈ 43 min no total; ver `ARCHITECTURE.md`, seção 7) |

## 5. Usando a API

> Os exemplos usam `bash`. No PowerShell, aspas dentro de JSON inline são removidas pelo `curl.exe`: grave o corpo em um arquivo e use `--data-binary "@corpo.json"`, ou use `Invoke-RestMethod`.

```bash
PLAYER=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1

# 1) criar wallet  -> 201
curl -s -X POST http://localhost:3000/wallets -H 'content-type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
WID=<id devolvido acima>

# 2) apostar. Idempotency-Key é obrigatório; repetir devolve a resposta original (200, idempotentReplay=true)
BODY="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WID\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
curl -s -w '  [HTTP %{http_code}]\n' -X POST http://localhost:3000/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' -d "$BODY"

# 3) consultar
curl -s http://localhost:3000/wallets/$WID
curl -s "http://localhost:3000/wallets/$WID/ledger?limit=50"          # cursor opaco em nextCursor
curl -s http://localhost:3000/providers/provider-a/wagering/transactions/transaction-123
curl -s -X POST http://localhost:3000/wallets/$WID/reconciliation     # saldo armazenado x reconstruído pelo ledger
```

### Status HTTP (iguais em todos os endpoints)

| Situação | HTTP | Provedor deve |
|---|---|---|
| criada e processada / replay idempotente | `201` / `200` | — |
| aceita, aguardando a referência (`PENDING_REFERENCE`) | `202` | não reenviar |
| payload inválido, falta `Idempotency-Key` | `400` | corrigir |
| wallet/transação inexistente | `404` | corrigir o id |
| mesma key com payload diferente; wallet duplicada | `409` | não reenviar igual |
| **rejeição de negócio** (corpo traz `failureCode`) | `422` | decidir pelo `failureCode` |
| falha transitória de infraestrutura | `503` + `Retry-After` | reenviar com a **mesma** key |

Os `failureCode` estão em `ARCHITECTURE.md`, seção 4. Toda resposta traz `X-Correlation-Id` (envie o seu para correlacionar).

### Pela fila (SQS)

```bash
# envia a mensagem do enunciado do desafio para wager-transactions.fifo; --times=3 entrega a MESMA mensagem 3 vezes
bun run demo:sqs "$WID" "$PLAYER" BET 10.00 --times=3
```

O resultado é **um** débito de 10,00: as outras duas entregas são deduplicadas pela inbox (aparecem em `duplicates_detected_total{source="sqs"}`). O script usa `AWS_ENDPOINT_URL` (padrão `http://localhost:4566`) e `SQS_QUEUE_NAME`. Argumentos: `<walletId> <playerId> [kind=BET] [amount=10.00] [--times=N] [--ref=<externalId>]`.

Mensagens inválidas vão para `wager-transactions-dlq.fifo` com `failureClass` e `failureReason` nos atributos.

## 6. Observabilidade

| Endpoint | Para quê |
|---|---|
| `GET /health/live` | processo vivo (não toca dependências) |
| `GET /health/ready` | PostgreSQL e SQS alcançáveis (`503` com o detalhe do que caiu) |
| `GET /metrics` | Prometheus: transações por status, duplicatas, retries, DLQ, conflitos de lock, outbox lag, latências |

Logs: JSON, uma linha por evento, com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`; **nunca** valores monetários nem payloads (ver `ARCHITECTURE.md`, seção 11).

## 7. Testes

```bash
bun run typecheck         # tsc em modo estrito
bun test test/unit        # unidade: rápido, sem containers   (também: bun run test)
bun run test:integration  # PostgreSQL e LocalStack REAIS em containers
bun run test:concurrency  # paralelismo real, várias conexões/"instâncias"
bun run test:e2e          # processos do SO (bun src/main.ts) + kill -9 + reinício
bun run test:all          # tudo (~5 min)
```

- Os testes de integração/e2e **sobem os próprios containers** (PostgreSQL e LocalStack) pelo CLI do `podman` ou do `docker` (o primeiro que responder; force com `CONTAINER_ENGINE=docker`) e os removem ao final. A primeira execução baixa as imagens.
- Nenhum teste de integração substitui PostgreSQL ou SQS por mocks. Os poucos fakes (broker em memória em alguns testes da outbox, relógio falso) isolam lógica de tempo/retry e são sempre combinados com o banco real.
- O que cada suíte prova está em `ARCHITECTURE.md` (seções 8–12). Resumo dos cenários obrigatórios do desafio:

| Cenário do enunciado | Onde |
|---|---|
| mesma aposta ×50 em paralelo → um débito | `test/concurrency/wagering.test.ts`, `test/e2e/multiprocess.test.ts` |
| saldo 100, duas apostas de 80 → 1 processada, 1 rejeitada, saldo 20, 1 débito | idem |
| wallets distintas em paralelo | `test/concurrency/wagering.test.ts` |
| ≥ 3 processos/instâncias | `test/e2e/multiprocess.test.ts` (3 processos reais) |
| worker morto depois do commit e antes do ack | `test/integration/sqs-consumer.test.ts`, `test/e2e/crash-recovery.test.ts` |
| dois publishers na mesma outbox | `test/concurrency/outbox.test.ts`, `test/e2e/multiprocess.test.ts` |
| `ROLLBACK`/`REFUND` antes da referência | `test/integration/pending-reference.test.ts`, `test/concurrency/pending-reference.test.ts` |
| reinício do serviço com consistência final | `test/e2e/crash-recovery.test.ts` |
| invariante `wallet.balance == saldo do ledger` | verificada em todos os testes e por SQL global |

Notas de plataforma: no **Windows** o `SIGTERM` vira terminação forçada, então o teste de desligamento por sinal fica `skip` (o caminho gracioso é coberto em processo); em Linux/macOS ele roda. Em container o `podman stop` já encerra com código `0`.

## 8. Operação

- **Escalar:** suba quantas instâncias quiser contra o mesmo banco e as mesmas filas; elas se coordenam pelo banco (locks por wallet, `SKIP LOCKED`, inbox/idempotência). Migrations são serializadas por advisory lock.
- **Desligamento gracioso (`SIGTERM`):** para de consumir, conclui o que está em andamento, devolve à fila o que não começou, esvazia a outbox e fecha o pool. No compose: `stop_grace_period: 30s`.
- **Reiniciar/derrubar a qualquer momento é seguro:** nada financeiro é perdido nem duplicado (provado com `kill -9` nos testes e2e).

## 9. Problemas comuns

| Sintoma | Causa e correção |
|---|---|
| `port is already allocated` (5433, 4566 ou 3000) | outra instalação usando a porta; pare-a ou altere o mapeamento em `docker-compose.yml` |
| testes não encontram container engine | inicie o Docker Desktop ou `podman machine start`; ou defina `CONTAINER_ENGINE` |
| `dependency failed to start: ... localstack ... unhealthy` | o healthcheck precisa aceitar `"sqs": "available"` **e** `"running"` (o LocalStack muda para `running` após o primeiro uso); já tratado no compose |
| Podman no Windows: `Testcontainers` trava | por isso os testes usam o CLI direto (`test/integration/support/container.ts`) |
| `QueueNameExists` ao subir uma instância | já existe uma fila com o mesmo nome e **atributos diferentes** (ex.: `SQS_MAX_RECEIVE_COUNT`/`SQS_VISIBILITY_TIMEOUT_SECONDS` distintos entre instâncias): mantenha a configuração igual em todas, ou recrie a fila |

## 10. Mapa do repositório

```
src/
  domain/          Money, Wallet, WagerTransaction, WalletLedgerEntry, Inbox/Outbox, eventos (puro, sem framework)
  application/     casos de uso (CreateWallet, SubmitWagerTransaction, consultas, workers) e portas
  infrastructure/  MikroORM (entidades, migrations), SQS, observabilidade, composition root
  interface/http/  controllers NestJS/Fastify, mapeamento de erros, /health, /metrics
  main.ts          processo: HTTP + consumer SQS + publisher da outbox + worker de PENDING_REFERENCE
test/              unit · integration · concurrency · e2e (processos reais)
scripts/           send-wager-message.ts · build-diagram.ts
docs/              architecture.svg (imagem do ARCHITECTURE.md) · architecture.drawio (fonte editável)
README.md          este arquivo: setup, execução, API e testes
ARCHITECTURE.md    decisões, trade-offs, limitações
```
