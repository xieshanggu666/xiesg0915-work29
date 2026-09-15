-- 机房设备退役与数据擦除管理系统 · schema v1
-- PostgreSQL 13+

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE asset_status AS ENUM (
  'in_service','pending','disk_pulled','erasing','erasure_verified','scrapped','disposed'
);
CREATE TYPE disk_status AS ENUM (
  'pulled','erasing','verified','failed','scrapped'
);
CREATE TYPE job_status AS ENUM (
  'pending','running','verifying','verified','failed'
);

CREATE TABLE assets (
  id            TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  tag           TEXT NOT NULL UNIQUE,
  hostname      TEXT NOT NULL DEFAULT '',
  vendor        TEXT NOT NULL DEFAULT '',
  model         TEXT NOT NULL DEFAULT '',
  sn            TEXT NOT NULL DEFAULT '',
  room          TEXT NOT NULL DEFAULT '',
  rack          TEXT NOT NULL DEFAULT '',
  owner         TEXT NOT NULL DEFAULT '',
  status        asset_status NOT NULL DEFAULT 'in_service',
  registered_by TEXT NOT NULL DEFAULT '',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE disks (
  id          TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  asset_id    TEXT NOT NULL REFERENCES assets(id),
  serial      TEXT NOT NULL,
  model       TEXT NOT NULL DEFAULT '',
  kind        TEXT NOT NULL DEFAULT 'HDD',
  capacity_gb BIGINT NOT NULL DEFAULT 0,
  device_path TEXT NOT NULL,
  slot        TEXT NOT NULL DEFAULT '',
  status      disk_status NOT NULL DEFAULT 'pulled',
  pulled_by   TEXT NOT NULL DEFAULT '',
  pulled_at   TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (asset_id, serial)
);
CREATE INDEX idx_disks_asset ON disks(asset_id);
CREATE INDEX idx_disks_status ON disks(status);

CREATE TABLE erasure_jobs (
  id           TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  disk_id      TEXT NOT NULL REFERENCES disks(id),
  standard     TEXT NOT NULL,
  status       job_status NOT NULL DEFAULT 'pending',
  attempt      INT NOT NULL DEFAULT 0,
  current_pass INT NOT NULL DEFAULT 0,
  total_bytes  BIGINT NOT NULL DEFAULT 0,
  bytes_done   BIGINT NOT NULL DEFAULT 0,
  error        TEXT NOT NULL DEFAULT '',
  -- power-loss recovery: which worker owns it + heartbeat
  locked_by    TEXT NOT NULL DEFAULT '',
  locked_at    TIMESTAMPTZ,
  created_by   TEXT NOT NULL DEFAULT '',
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_jobs_disk ON erasure_jobs(disk_id);
-- partial index used by the claim query: one pending job per disk
CREATE INDEX idx_jobs_pending ON erasure_jobs(created_at) WHERE status = 'pending';
CREATE INDEX idx_jobs_running ON erasure_jobs(locked_at) WHERE status IN ('running','verifying');

CREATE TABLE checkpoints (
  job_id        TEXT PRIMARY KEY REFERENCES erasure_jobs(id),
  pass          INT NOT NULL,
  offset        BIGINT NOT NULL,
  heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- append-only: 每次执行尝试（含 interrupted / 重擦）一行，不可改不可删
CREATE TABLE erasure_runs (
  id           TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  job_id       TEXT NOT NULL REFERENCES erasure_jobs(id),
  attempt      INT NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  resumed      BOOLEAN NOT NULL DEFAULT false,
  wrote_bytes  BIGINT NOT NULL DEFAULT 0,
  verify_mode  TEXT NOT NULL DEFAULT '',
  verify_bytes BIGINT NOT NULL DEFAULT 0,
  mismatches   BIGINT NOT NULL DEFAULT 0,
  result       TEXT NOT NULL,
  detail       TEXT NOT NULL DEFAULT '',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_runs_job ON erasure_runs(job_id);

CREATE TABLE certificates (
  cert_no        TEXT PRIMARY KEY,
  disk_id        TEXT NOT NULL REFERENCES disks(id),
  asset_id       TEXT NOT NULL REFERENCES assets(id),
  job_id         TEXT NOT NULL REFERENCES erasure_jobs(id),
  standard       TEXT NOT NULL,
  standard_name  TEXT NOT NULL,
  disk_serial    TEXT NOT NULL,
  disk_model     TEXT NOT NULL DEFAULT '',
  capacity_gb   BIGINT NOT NULL DEFAULT 0,
  asset_tag     TEXT NOT NULL,
  passes         JSONB NOT NULL,
  started_at     TIMESTAMPTZ NOT NULL,
  finished_at    TIMESTAMPTZ NOT NULL,
  attempts       INT NOT NULL,
  verify_mode    TEXT NOT NULL,
  verify_bytes   BIGINT NOT NULL DEFAULT 0,
  report_key     TEXT NOT NULL,
  cert_key       TEXT NOT NULL,
  report_sha256  TEXT NOT NULL,
  operator       TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_certs_asset ON certificates(asset_id);
CREATE INDEX idx_certs_disk ON certificates(disk_id);

-- 处置签收：一台设备只有一行，UNIQUE 保证重复签收直接冲突（不可覆盖）
CREATE TABLE disposal_confirmations (
  id           TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  asset_id     TEXT NOT NULL UNIQUE REFERENCES assets(id),
  asset_tag    TEXT NOT NULL,
  method       TEXT NOT NULL CHECK (method IN ('reuse','resale','destroy')),
  receiver     TEXT NOT NULL,
  receiver_org TEXT NOT NULL DEFAULT '',
  note         TEXT NOT NULL DEFAULT '',
  operator     TEXT NOT NULL,
  signed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_logs (
  id          TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor       TEXT NOT NULL,
  action      TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id   TEXT NOT NULL,
  detail      TEXT NOT NULL DEFAULT '',
  request_id  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_audit_entity ON audit_logs(entity_type, entity_id, ts DESC);
CREATE INDEX idx_audit_actor ON audit_logs(actor, ts DESC);
CREATE INDEX idx_audit_time ON audit_logs(ts DESC);

-- ---------------------------------------------------------------------------
-- 不可变表：禁止 UPDATE / DELETE（审计表、擦除运行记录、擦除证明、处置签收）
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION forbid_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'table %.% is append-only; % is forbidden',
    TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_immutable
  BEFORE UPDATE OR DELETE ON audit_logs
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER trg_runs_immutable
  BEFORE UPDATE OR DELETE ON erasure_runs
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER trg_certs_immutable
  BEFORE UPDATE OR DELETE ON certificates
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER trg_disposal_immutable
  BEFORE UPDATE OR DELETE ON disposal_confirmations
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- ---------------------------------------------------------------------------
-- 资产状态机：非法跳转直接拒绝
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_asset_transition() RETURNS trigger AS $$
BEGIN
  IF NEW.status = OLD.status THEN
    RETURN NEW;
  END IF;
  IF (OLD.status, NEW.status) IN (
    ('in_service','pending'),
    ('pending','disk_pulled'),
    ('disk_pulled','erasing'),
    ('disk_pulled','scrapped'),
    ('erasing','erasure_verified'),
    ('erasing','scrapped'),
    ('erasure_verified','disposed'),
    ('scrapped','disposed')
  ) THEN
    NEW.updated_at = now();
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal asset transition % -> %', OLD.status, NEW.status;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_asset_transition
  BEFORE UPDATE OF status ON assets
  FOR EACH ROW EXECUTE FUNCTION check_asset_transition();
