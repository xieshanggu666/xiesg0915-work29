-- 退役审批单 · schema v2
-- 资产负责人提交退役申请（擦除标准 + 处置方式），安全员审核通过后
-- 才允许拆盘、擦除与处置签收；支持驳回重提与执行前撤回，禁止申请人自审。
-- PostgreSQL 13+（在 0001_init.sql 之后执行）

CREATE TYPE approval_status AS ENUM (
  'pending','approved','rejected','withdrawn'
);

CREATE TABLE decommission_approvals (
  id          TEXT PRIMARY KEY DEFAULT encode(gen_random_bytes(16),'hex'),
  asset_id    TEXT NOT NULL REFERENCES assets(id),
  asset_tag   TEXT NOT NULL,
  standard    TEXT NOT NULL,                                    -- 申请的擦除标准
  method      TEXT NOT NULL CHECK (method IN ('reuse','resale','destroy')), -- 申请的处置方式
  reason      TEXT NOT NULL DEFAULT '',                         -- 退役原因
  applicant   TEXT NOT NULL,                                    -- 申请人（资产负责人）
  status      approval_status NOT NULL DEFAULT 'pending',
  reviewer    TEXT NOT NULL DEFAULT '',                         -- 审核人（安全员）
  review_note TEXT NOT NULL DEFAULT '',                         -- 审核意见
  reviewed_at TIMESTAMPTZ,
  version     INT NOT NULL DEFAULT 1,                           -- 驳回/撤回后重提递增
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_approvals_asset ON decommission_approvals(asset_id, created_at DESC);

-- 同一资产最多一条活动（pending/approved）审批单；
-- 驳回/撤回为终态，重提即插入新行（version+1），历史完整保留。
CREATE UNIQUE INDEX uniq_active_approval ON decommission_approvals(asset_id)
  WHERE status IN ('pending','approved');

-- ---------------------------------------------------------------------------
-- 审批单状态机：非法跳转直接拒绝。
-- 附加业务护栏：approved -> withdrawn 仅当资产尚未开始执行（仍为 pending）。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_approval_transition() RETURNS trigger AS $$
BEGIN
  IF NEW.status = OLD.status THEN
    RETURN NEW;
  END IF;
  IF (OLD.status, NEW.status) IN (
    ('pending','approved'),
    ('pending','rejected'),
    ('pending','withdrawn')
  ) THEN
    NEW.updated_at = now();
    RETURN NEW;
  END IF;
  IF OLD.status = 'approved' AND NEW.status = 'withdrawn' THEN
    IF (SELECT status FROM assets WHERE id = OLD.asset_id) <> 'pending' THEN
      RAISE EXCEPTION 'cannot withdraw approval %: execution already started', OLD.id;
    END IF;
    NEW.updated_at = now();
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal approval transition % -> %', OLD.status, NEW.status;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_approval_transition
  BEFORE UPDATE OF status ON decommission_approvals
  FOR EACH ROW EXECUTE FUNCTION check_approval_transition();
