import { Migration } from '@mikro-orm/migrations';

/**
 * Schema inicial. As garantias de unicidade, imutabilidade e não-negatividade vivem
 * aqui (restrição 9), não apenas no código de aplicação.
 */
export class Migration20260801000000_init extends Migration {
  override name = 'Migration20260801000000_init';

  override up(): void {
    // ───────────────────────────── wallets ─────────────────────────────
    this.addSql(`
      CREATE TABLE wallets (
        id         uuid          PRIMARY KEY,
        player_id  uuid          NOT NULL,
        currency   char(3)       NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance    numeric(20,2) NOT NULL CHECK (balance >= 0),
        version    integer       NOT NULL CHECK (version >= 1),
        created_at timestamptz   NOT NULL,
        updated_at timestamptz   NOT NULL,
        CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency),
        CONSTRAINT wallets_id_currency_key     UNIQUE (id, currency)
      )`);

    // ───────────────────────── wager_transactions ──────────────────────
    this.addSql(`
      CREATE TABLE wager_transactions (
        id                                uuid          PRIMARY KEY,
        provider_id                       text          NOT NULL,
        external_transaction_id           text          NOT NULL,
        idempotency_key                   text          NOT NULL,
        payload_hash                      char(64)      NOT NULL,
        wallet_id                         uuid          NOT NULL REFERENCES wallets (id),
        player_id                         uuid          NOT NULL,
        round_id                          text          NOT NULL,
        game_id                           text          NOT NULL,
        kind                              text          NOT NULL
          CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount                            numeric(20,2) NOT NULL CHECK (amount >= 0),
        currency                          char(3)       NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id text,
        reference_transaction_id          uuid          REFERENCES wager_transactions (id),
        status                            text          NOT NULL
          CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code                      text,
        -- saldo da wallet observado quando a transação chegou ao estado atual: base do replay idempotente
        balance_snapshot                  numeric(20,2) NOT NULL CHECK (balance_snapshot >= 0),
        reference_attempts                integer       NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
        next_attempt_at                   timestamptz,
        created_at                        timestamptz   NOT NULL,
        processed_at                      timestamptz,
        CONSTRAINT wager_transactions_idempotency_key_key UNIQUE (idempotency_key),
        CONSTRAINT wager_transactions_provider_external_key UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT wager_transactions_id_wallet_key UNIQUE (id, wallet_id),
        CONSTRAINT wager_transactions_positive_amount
          CHECK (kind = 'LOSS' OR amount > 0),
        CONSTRAINT wager_transactions_reference_required
          CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CONSTRAINT wager_transactions_failure_code_consistent
          CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CONSTRAINT wager_transactions_processed_at_consistent
          CHECK ((status = 'PROCESSED') = (processed_at IS NOT NULL)),
        CONSTRAINT wager_transactions_processed_reversal_has_reference
          CHECK (NOT (status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')) OR reference_transaction_id IS NOT NULL),
        CONSTRAINT wager_transactions_opening_internal
          CHECK ((kind = 'OPENING') = (provider_id = 'internal'))
      )`);

    // Uma referência é revertida no máximo uma vez, por REFUND ou ROLLBACK (interpretação mais estrita
    // que a regra 4: impede crédito duplicado quando REFUND e ROLLBACK disputam o mesmo alvo).
    this.addSql(`
      CREATE UNIQUE INDEX wager_transactions_single_reversal_idx
        ON wager_transactions (reference_transaction_id)
        WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`);

    // Worker de PENDING_REFERENCE: varre só o que está pendente e vencido.
    this.addSql(`
      CREATE INDEX wager_transactions_pending_reference_idx
        ON wager_transactions (next_attempt_at NULLS FIRST)
        WHERE status = 'PENDING_REFERENCE'`);

    this.addSql(`CREATE INDEX wager_transactions_wallet_idx ON wager_transactions (wallet_id, created_at)`);

    // ─────────────────────── wallet_ledger_entries ─────────────────────
    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        id             uuid          PRIMARY KEY,
        seq            bigint        GENERATED ALWAYS AS IDENTITY,
        wallet_id      uuid          NOT NULL,
        transaction_id uuid          NOT NULL,
        direction      text          NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount         numeric(20,2) NOT NULL CHECK (amount > 0),
        currency       char(3)       NOT NULL,
        balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
        balance_after  numeric(20,2) NOT NULL CHECK (balance_after >= 0),
        created_at     timestamptz   NOT NULL,
        CONSTRAINT wallet_ledger_entries_seq_key UNIQUE (seq),
        -- no máximo um lançamento por wallet para cada transação
        CONSTRAINT wallet_ledger_entries_wallet_tx_key UNIQUE (wallet_id, transaction_id),
        -- ordenação estável por wallet (paginação por cursor)
        CONSTRAINT wallet_ledger_entries_wallet_seq_key UNIQUE (wallet_id, seq),
        CONSTRAINT wallet_ledger_entries_wallet_fk
          FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
        CONSTRAINT wallet_ledger_entries_transaction_fk
          FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id),
        CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
          (direction = 'CREDIT' AND balance_before + amount = balance_after) OR
          (direction = 'DEBIT'  AND balance_before - amount = balance_after)
        )
      )`);

    // ───────────────────────────── inbox ───────────────────────────────
    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name text        NOT NULL,
        message_id    text        NOT NULL,
        payload_hash  char(64)    NOT NULL,
        received_at   timestamptz NOT NULL,
        processed_at  timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      )`);

    // ──────────────────────────── outbox ───────────────────────────────
    this.addSql(`
      CREATE TABLE outbox_messages (
        id              uuid        PRIMARY KEY,
        seq             bigint      GENERATED ALWAYS AS IDENTITY,
        aggregate_id    uuid        NOT NULL,
        event_type      text        NOT NULL,
        payload         jsonb       NOT NULL,
        occurred_at     timestamptz NOT NULL,
        attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz,
        published_at    timestamptz,
        CONSTRAINT outbox_messages_seq_key UNIQUE (seq)
      )`);
    this.addSql(`
      CREATE INDEX outbox_messages_pending_idx
        ON outbox_messages (seq)
        WHERE published_at IS NULL`);

    this.createGuards();
  }

  private createGuards(): void {
    // ── wallets: imutabilidade de identidade; version só sobe quando o saldo muda ──
    this.addSql(`
      CREATE FUNCTION wallets_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id
           OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at THEN
          RAISE EXCEPTION 'wallet identity columns are immutable' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.balance <> OLD.balance THEN
          IF NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'wallet version must increase by exactly 1 when balance changes'
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
        ELSIF NEW.version <> OLD.version THEN
          RAISE EXCEPTION 'wallet version must not change when balance is unchanged'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`
      CREATE TRIGGER wallets_guard_trg BEFORE UPDATE OR DELETE ON wallets
        FOR EACH ROW EXECUTE FUNCTION wallets_guard()`);

    // ── wager_transactions: terminais imutáveis, colunas de negócio imutáveis, sem delete ──
    this.addSql(`
      CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wager_transactions cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'wager transaction % is in terminal status %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
          RAISE EXCEPTION 'invalid status transition PENDING_REFERENCE -> PENDING'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.id <> OLD.id OR NEW.provider_id <> OLD.provider_id
           OR NEW.external_transaction_id <> OLD.external_transaction_id
           OR NEW.idempotency_key <> OLD.idempotency_key OR NEW.payload_hash <> OLD.payload_hash
           OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
           OR NEW.round_id <> OLD.round_id OR NEW.game_id <> OLD.game_id
           OR NEW.kind <> OLD.kind OR NEW.amount <> OLD.amount OR NEW.currency <> OLD.currency
           OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
           OR NEW.created_at <> OLD.created_at THEN
          RAISE EXCEPTION 'wager transaction business columns are immutable'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`
      CREATE TRIGGER wager_transactions_guard_trg BEFORE UPDATE OR DELETE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard()`);

    // ── ledger: append-only ──
    this.addSql(`
      CREATE FUNCTION wallet_ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is append-only (% not allowed)', TG_OP
          USING ERRCODE = 'integrity_constraint_violation';
      END $$`);
    this.addSql(`
      CREATE TRIGGER wallet_ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION wallet_ledger_append_only()`);
    this.addSql(`
      CREATE TRIGGER wallet_ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
        FOR EACH STATEMENT EXECUTE FUNCTION wallet_ledger_append_only()`);

    // ── ledger: encadeamento (balance_before == último balance_after da wallet) ──
    this.addSql(`
      CREATE FUNCTION wallet_ledger_chain() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE last_after numeric;
      BEGIN
        SELECT balance_after INTO last_after FROM wallet_ledger_entries
          WHERE wallet_id = NEW.wallet_id ORDER BY seq DESC LIMIT 1;
        IF NOT FOUND THEN last_after := 0; END IF;
        IF NEW.balance_before <> last_after THEN
          RAISE EXCEPTION 'ledger chain broken for wallet %: balance_before % <> last balance_after %',
            NEW.wallet_id, NEW.balance_before, last_after
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`
      CREATE TRIGGER wallet_ledger_chain_trg BEFORE INSERT ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION wallet_ledger_chain()`);

    // ── saldo materializado == ledger, verificado no COMMIT (toda mudança de saldo tem lançamento e vice-versa) ──
    this.addSql(`
      CREATE FUNCTION assert_wallet_matches_ledger(p_wallet uuid) RETURNS void LANGUAGE plpgsql AS $$
      DECLARE w numeric; l numeric;
      BEGIN
        SELECT balance INTO w FROM wallets WHERE id = p_wallet;
        IF NOT FOUND THEN RETURN; END IF;
        SELECT balance_after INTO l FROM wallet_ledger_entries
          WHERE wallet_id = p_wallet ORDER BY seq DESC LIMIT 1;
        IF NOT FOUND THEN l := 0; END IF;
        IF w <> l THEN
          RAISE EXCEPTION 'wallet % balance % does not match ledger %', p_wallet, w, l
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
      END $$`);
    this.addSql(`
      CREATE FUNCTION wallets_check_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM assert_wallet_matches_ledger(NEW.id); RETURN NULL; END $$`);
    this.addSql(`
      CREATE FUNCTION ledger_check_wallet() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM assert_wallet_matches_ledger(NEW.wallet_id); RETURN NULL; END $$`);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER wallets_ledger_consistency_trg
        AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION wallets_check_ledger()`);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER ledger_wallet_consistency_trg
        AFTER INSERT ON wallet_ledger_entries
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION ledger_check_wallet()`);
  }

  override down(): void {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions`);
    this.addSql(`DROP TABLE IF EXISTS wallets`);
    for (const fn of [
      'wallets_guard()',
      'wager_transactions_guard()',
      'wallet_ledger_append_only()',
      'wallet_ledger_chain()',
      'wallets_check_ledger()',
      'ledger_check_wallet()',
      'assert_wallet_matches_ledger(uuid)',
    ]) {
      this.addSql(`DROP FUNCTION IF EXISTS ${fn}`);
    }
  }
}
