// Package store: PostgreSQL implementation of the Store interface.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"idc/decommission/internal/domain"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *Postgres) Close() { p.pool.Close() }

// ---- mapping helpers ----

func scanAsset(row pgx.Row) (domain.Asset, error) {
	var a domain.Asset
	err := row.Scan(&a.ID, &a.Tag, &a.Hostname, &a.Vendor, &a.Model, &a.SN,
		&a.Room, &a.Rack, &a.Owner, &a.Status, &a.RegisteredBy, &a.CreatedAt, &a.UpdatedAt)
	return a, err
}

const assetCols = `id,tag,hostname,vendor,model,sn,room,rack,owner,status,registered_by,created_at,updated_at`

func scanDisk(row pgx.Row) (domain.Disk, error) {
	var d domain.Disk
	err := row.Scan(&d.ID, &d.AssetID, &d.Serial, &d.Model, &d.Kind, &d.CapacityGB,
		&d.DevicePath, &d.Slot, &d.Status, &d.PulledBy, &d.PulledAt, &d.CreatedAt)
	return d, err
}

const diskCols = `id,asset_id,serial,model,kind,capacity_gb,device_path,slot,status,pulled_by,pulled_at,created_at`

func scanJob(row pgx.Row) (domain.ErasureJob, error) {
	var j domain.ErasureJob
	err := row.Scan(&j.ID, &j.DiskID, &j.Standard, &j.Status, &j.Attempt, &j.CurrentPass,
		&j.TotalBytes, &j.BytesDone, &j.Error, &j.CreatedBy, &j.LockedBy, &j.LockedAt, &j.StartedAt,
		&j.FinishedAt, &j.CreatedAt, &j.UpdatedAt)
	return j, err
}

const jobCols = `id,disk_id,standard,status,attempt,current_pass,total_bytes,bytes_done,error,created_by,locked_by,locked_at,started_at,finished_at,created_at,updated_at`

// ---- assets ----

func (p *Postgres) CreateAsset(ctx context.Context, a domain.Asset) error {
	if a.Status == "" {
		a.Status = domain.AssetInService
	}
	if a.RegisteredBy == "" {
		a.RegisteredBy = a.Owner
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
INSERT INTO assets(tag,hostname,vendor,model,sn,room,rack,owner,status,registered_by)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		a.Tag, a.Hostname, a.Vendor, a.Model, a.SN, a.Room, a.Rack, a.Owner,
		string(a.Status), a.RegisteredBy)
	if err != nil {
		return mapErr(err)
	}
	row := tx.QueryRow(ctx, `SELECT id FROM assets WHERE tag=$1`, a.Tag)
	var id string
	if err := row.Scan(&id); err != nil {
		return err
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: a.RegisteredBy, Action: "asset.register", EntityType: "asset", EntityID: id,
		Detail: fmt.Sprintf("登记退役设备 %s (%s %s SN:%s)", a.Tag, a.Vendor, a.Model, a.SN),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) GetAsset(ctx context.Context, id string) (domain.Asset, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+assetCols+` FROM assets WHERE id=$1`, id)
	a, err := scanAsset(row)
	return a, mapErr(err)
}

func (p *Postgres) GetAssetByTag(ctx context.Context, tag string) (domain.Asset, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+assetCols+` FROM assets WHERE tag=$1`, tag)
	a, err := scanAsset(row)
	return a, mapErr(err)
}

func (p *Postgres) ListAssets(ctx context.Context, f ListAssetsFilter) ([]domain.Asset, int, error) {
	var conds []string
	var args []any
	if f.Status != "" {
		conds = append(conds, fmt.Sprintf("status=$%d", len(args)+1))
		args = append(args, string(f.Status))
	}
	if f.Tag != "" {
		conds = append(conds, fmt.Sprintf("tag ILIKE $%d", len(args)+1))
		args = append(args, fmt.Sprintf("%%%s%%", f.Tag))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM assets `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	q := `SELECT ` + assetCols + ` FROM assets ` + where + ` ORDER BY created_at DESC`
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d OFFSET %d", f.Limit, f.Offset)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []domain.Asset{}
	for rows.Next() {
		a, err := scanAsset(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, a)
	}
	return out, total, rows.Err()
}

func (p *Postgres) TransitionAsset(ctx context.Context, id string, to domain.AssetStatus, actor, action, detail string) (domain.Asset, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return domain.Asset{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx,
		`UPDATE assets SET status=$2, updated_at=now() WHERE id=$1 RETURNING `+assetCols,
		id, string(to))
	a, err := scanAsset(row)
	if err != nil {
		return domain.Asset{}, mapErr(err)
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: actor, Action: action, EntityType: "asset", EntityID: id, Detail: detail,
	}); err != nil {
		return domain.Asset{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Asset{}, err
	}
	return a, nil
}

// ---- disks ----

func (p *Postgres) CreateDisk(ctx context.Context, d domain.Disk) error {
	if d.ID == "" {
		d.ID = domain.ID()
	}
	_, err := p.pool.Exec(ctx, `
INSERT INTO disks(id,asset_id,serial,model,kind,capacity_gb,device_path,slot,status,pulled_by,pulled_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		d.ID, d.AssetID, d.Serial, d.Model, d.Kind, d.CapacityGB, d.DevicePath, d.Slot,
		string(d.Status), d.PulledBy, d.PulledAt)
	return mapErr(err)
}

func (p *Postgres) GetDisk(ctx context.Context, id string) (domain.Disk, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+diskCols+` FROM disks WHERE id=$1`, id)
	d, err := scanDisk(row)
	return d, mapErr(err)
}

func (p *Postgres) ListDisks(ctx context.Context, assetID string) ([]domain.Disk, error) {
	q := `SELECT ` + diskCols + ` FROM disks`
	var args []any
	if assetID != "" {
		q += ` WHERE asset_id=$1`
		args = append(args, assetID)
	}
	q += ` ORDER BY created_at`
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Disk{}
	for rows.Next() {
		d, err := scanDisk(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (p *Postgres) UpdateDiskStatus(ctx context.Context, id string, status domain.DiskStatus, actor, detail string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ct, err := tx.Exec(ctx, `UPDATE disks SET status=$2 WHERE id=$1`, id, string(status))
	if err != nil {
		return mapErr(err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("%w: disk %s", ErrNotFound, id)
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: actor, Action: "disk.status:" + string(status),
		EntityType: "disk", EntityID: id, Detail: detail,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---- jobs ----

func (p *Postgres) CreateJob(ctx context.Context, j domain.ErasureJob) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id string
	err = tx.QueryRow(ctx, `
INSERT INTO erasure_jobs(disk_id,standard,status,attempt,created_by)
VALUES($1,$2,$3,$4,$5) RETURNING id`,
		j.DiskID, j.Standard, string(domain.JobPending), j.Attempt, j.CreatedBy).Scan(&id)
	if err != nil {
		return mapErr(err)
	}
	j.ID = id
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: j.CreatedBy, Action: "job.create", EntityType: "job", EntityID: id,
		Detail: fmt.Sprintf("创建擦除任务 disk=%s standard=%s attempt=%d", j.DiskID, j.Standard, j.Attempt),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) GetJob(ctx context.Context, id string) (domain.ErasureJob, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM erasure_jobs WHERE id=$1`, id)
	j, err := scanJob(row)
	return j, mapErr(err)
}

func (p *Postgres) ListJobs(ctx context.Context, diskID string) ([]domain.ErasureJob, error) {
	q := `SELECT ` + jobCols + ` FROM erasure_jobs`
	var args []any
	if diskID != "" {
		q += ` WHERE disk_id=$1`
		args = append(args, diskID)
	}
	q += ` ORDER BY created_at DESC`
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.ErasureJob{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// AcquireNextJob atomically claims one pending job or one stale running/verifying
// job whose heartbeat expired (power loss). SKIP LOCKED makes this safe with
// multiple scheduler instances.
func (p *Postgres) AcquireNextJob(ctx context.Context, workerID string, staleAfterSec int) (AcquireResult, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return AcquireResult{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
WITH picked AS (
  SELECT j.id FROM erasure_jobs j
  LEFT JOIN checkpoints c ON c.job_id = j.id
  WHERE j.status = 'pending'
     OR (j.status IN ('running','verifying')
         AND (
           -- $1=0 => process (re)start: reclaim jobs left by a previous owner,
           -- but never jobs this worker currently owns
           ($1::double precision = 0 AND COALESCE(j.locked_by,'') <> $2)
           OR ($1::double precision > 0
               AND GREATEST(COALESCE(j.locked_at, to_timestamp(0)),
                            COALESCE(c.heartbeat_at, to_timestamp(0)))
                   < now() - make_interval(secs => $1::double precision))
         ))
  ORDER BY CASE WHEN j.status='pending' THEN 0 ELSE 1 END, j.created_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE erasure_jobs j
SET locked_by=$2, locked_at=now(), updated_at=now(),
    -- claim pending jobs atomically so no other scheduler can double-pick them
    status=CASE WHEN j.status='pending' THEN 'running' ELSE j.status END
FROM picked WHERE j.id = picked.id
RETURNING `+jobPrefixCols("j."),
		float64(staleAfterSec), workerID)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AcquireResult{}, nil
		}
		return AcquireResult{}, err
	}
	drow := tx.QueryRow(ctx, `SELECT `+diskCols+` FROM disks WHERE id=$1`, j.DiskID)
	d, err := scanDisk(drow)
	if err != nil {
		return AcquireResult{}, err
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: workerID, Action: "job.acquire", EntityType: "job", EntityID: j.ID,
		Detail: fmt.Sprintf("worker=%s status=%s pass=%d offset=%d",
			workerID, j.Status, j.CurrentPass, j.BytesDone),
	}); err != nil {
		return AcquireResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AcquireResult{}, err
	}
	return AcquireResult{Job: j, Disk: d, OK: true}, nil
}

// jobPrefixCols prefixes each column with a table alias.
func jobPrefixCols(alias string) string {
	cols := strings.Split(jobCols, ",")
	for i, c := range cols {
		cols[i] = alias + c
	}
	return strings.Join(cols, ",")
}

func (p *Postgres) MarkJobRunning(ctx context.Context, jobID string, attempt int) error {
	_, err := p.pool.Exec(ctx, `
UPDATE erasure_jobs SET status='running', attempt=$2,
  started_at=COALESCE(started_at, now()), updated_at=now()
WHERE id=$1`, jobID, attempt)
	return mapErr(err)
}

func (p *Postgres) SaveProgress(ctx context.Context, jobID string, pass int, offset, total int64, status domain.JobStatus) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
UPDATE erasure_jobs SET current_pass=$2, bytes_done=$3, total_bytes=$4, status=$5,
  locked_at=now(), updated_at=now()
WHERE id=$1`,
		jobID, pass, offset, total, string(status)); err != nil {
		return mapErr(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO checkpoints(job_id,pass,offset,heartbeat_at,updated_at)
VALUES($1,$2,$3,now(),now())
ON CONFLICT (job_id) DO UPDATE
  SET pass=EXCLUDED.pass, offset=EXCLUDED.offset,
      heartbeat_at=now(), updated_at=now()`,
		jobID, pass, offset); err != nil {
		return mapErr(err)
	}
	return tx.Commit(ctx)
}

func (p *Postgres) CompleteJob(ctx context.Context, jobID string, status domain.JobStatus, errMsg string, run domain.ErasureRun) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
UPDATE erasure_jobs SET status=$2, error=$3, finished_at=now(), updated_at=now()
WHERE id=$1`, jobID, string(status), errMsg); err != nil {
		return mapErr(err)
	}
	var j domain.ErasureJob
	if err := tx.QueryRow(ctx, `SELECT attempt FROM erasure_jobs WHERE id=$1`, jobID).Scan(&j.Attempt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO erasure_runs(job_id,attempt,started_at,finished_at,resumed,wrote_bytes,verify_mode,verify_bytes,mismatches,result,detail)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		jobID, j.Attempt, run.StartedAt, run.FinishedAt, run.Resumed, run.WroteBytes,
		run.VerifyMode, run.VerifyBytes, run.Mismatches, run.Result, run.Detail); err != nil {
		return mapErr(err)
	}
	return tx.Commit(ctx)
}

func (p *Postgres) RecordRun(ctx context.Context, run domain.ErasureRun) error {
	err := p.pool.QueryRow(ctx, `
INSERT INTO erasure_runs(job_id,attempt,started_at,finished_at,resumed,wrote_bytes,verify_mode,verify_bytes,mismatches,result,detail)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
RETURNING id`,
		run.JobID, run.Attempt, run.StartedAt, run.FinishedAt, run.Resumed, run.WroteBytes,
		run.VerifyMode, run.VerifyBytes, run.Mismatches, run.Result, run.Detail,
	).Scan(&run.ID)
	return mapErr(err)
}

func (p *Postgres) ListRuns(ctx context.Context, jobID string) ([]domain.ErasureRun, error) {
	rows, err := p.pool.Query(ctx, `
SELECT id,job_id,attempt,started_at,finished_at,resumed,wrote_bytes,verify_mode,verify_bytes,mismatches,result,detail
FROM erasure_runs WHERE job_id=$1 ORDER BY started_at`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ErasureRun
	for rows.Next() {
		var r domain.ErasureRun
		if err := rows.Scan(&r.ID, &r.JobID, &r.Attempt, &r.StartedAt, &r.FinishedAt,
			&r.Resumed, &r.WroteBytes, &r.VerifyMode, &r.VerifyBytes,
			&r.Mismatches, &r.Result, &r.Detail); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) GetCheckpoint(ctx context.Context, jobID string) (domain.Checkpoint, error) {
	var cp domain.Checkpoint
	err := p.pool.QueryRow(ctx,
		`SELECT job_id,pass,offset,heartbeat_at,updated_at FROM checkpoints WHERE job_id=$1`,
		jobID).Scan(&cp.JobID, &cp.Pass, &cp.Offset, &cp.HeartbeatAt, &cp.UpdatedAt)
	return cp, mapErr(err)
}

// ---- certificates ----

func (p *Postgres) SaveCertificate(ctx context.Context, c domain.Certificate) error {
	passes, err := json.Marshal(c.Passes)
	if err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
INSERT INTO certificates(cert_no,disk_id,asset_id,job_id,standard,standard_name,disk_serial,disk_model,
 capacity_gb,asset_tag,passes,started_at,finished_at,attempts,verify_mode,verify_bytes,
 report_key,cert_key,report_sha256,operator)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		c.CertNo, c.DiskID, c.AssetID, c.JobID, c.Standard, c.StandardName,
		c.DiskSerial, c.DiskModel, c.CapacityGB, c.AssetTag, passes,
		c.StartedAt, c.FinishedAt, c.Attempts, c.VerifyMode, c.VerifyBytes,
		c.ReportKey, c.CertKey, c.ReportSHA256, c.Operator)
	if err != nil {
		return mapErr(err)
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: c.Operator, Action: "certificate.issue",
		EntityType: "certificate", EntityID: c.CertNo,
		Detail: fmt.Sprintf("签发擦除证明 disk=%s asset=%s report=%s", c.DiskID, c.AssetID, c.ReportKey),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func scanCert(row pgx.Row) (domain.Certificate, error) {
	var c domain.Certificate
	var passes []byte
	err := row.Scan(&c.CertNo, &c.DiskID, &c.AssetID, &c.JobID, &c.Standard,
		&c.StandardName, &c.DiskSerial, &c.DiskModel, &c.CapacityGB, &c.AssetTag,
		&passes, &c.StartedAt, &c.FinishedAt, &c.Attempts, &c.VerifyMode,
		&c.VerifyBytes, &c.ReportKey, &c.CertKey, &c.ReportSHA256, &c.Operator, &c.CreatedAt)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(passes, &c.Passes); err != nil {
		return c, err
	}
	return c, nil
}

const certCols = `cert_no,disk_id,asset_id,job_id,standard,standard_name,disk_serial,disk_model,capacity_gb,asset_tag,passes,started_at,finished_at,attempts,verify_mode,verify_bytes,report_key,cert_key,report_sha256,operator,created_at`

func (p *Postgres) GetCertificate(ctx context.Context, certNo string) (domain.Certificate, error) {
	c, err := scanCert(p.pool.QueryRow(ctx, `SELECT `+certCols+` FROM certificates WHERE cert_no=$1`, certNo))
	return c, mapErr(err)
}

func (p *Postgres) ListCertificates(ctx context.Context, assetID string) ([]domain.Certificate, error) {
	q := `SELECT ` + certCols + ` FROM certificates`
	var args []any
	if assetID != "" {
		q += ` WHERE asset_id=$1`
		args = append(args, assetID)
	}
	q += ` ORDER BY created_at DESC`
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Certificate{}
	for rows.Next() {
		c, err := scanCert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---- disposal ----

func (p *Postgres) SaveDisposal(ctx context.Context, d domain.DisposalConfirmation) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// UNIQUE(asset_id) + append-only trigger guarantee a signed confirmation
	// can never be replaced or deleted.
	err = tx.QueryRow(ctx, `
INSERT INTO disposal_confirmations(asset_id,asset_tag,method,receiver,receiver_org,note,operator)
VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,signed_at`,
		d.AssetID, d.AssetTag, d.Method, d.Receiver, d.ReceiverOrg, d.Note, d.Operator).
		Scan(&d.ID, &d.SignedAt)
	if err != nil {
		return mapErr(err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE assets SET status='disposed', updated_at=now() WHERE id=$1`, d.AssetID); err != nil {
		return mapErr(err)
	}
	if err := appendAuditTx(ctx, tx, domain.AuditLog{
		Actor: d.Operator, Action: "disposal.sign", EntityType: "asset", EntityID: d.AssetID,
		Detail: fmt.Sprintf("处置签收 method=%s receiver=%s/%s", d.Method, d.ReceiverOrg, d.Receiver),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) GetDisposal(ctx context.Context, assetID string) (domain.DisposalConfirmation, error) {
	var d domain.DisposalConfirmation
	err := p.pool.QueryRow(ctx, `
SELECT id,asset_id,asset_tag,method,receiver,receiver_org,note,operator,signed_at
FROM disposal_confirmations WHERE asset_id=$1`, assetID).
		Scan(&d.ID, &d.AssetID, &d.AssetTag, &d.Method, &d.Receiver,
			&d.ReceiverOrg, &d.Note, &d.Operator, &d.SignedAt)
	return d, mapErr(err)
}

// ---- audit ----

func appendAuditTx(ctx context.Context, tx pgx.Tx, l domain.AuditLog) error {
	if l.Time.IsZero() {
		l.Time = time.Now()
	}
	_, err := tx.Exec(ctx, `
INSERT INTO audit_logs(id,ts,actor,action,entity_type,entity_id,detail,request_id)
VALUES(COALESCE(NULLIF($1,''),encode(gen_random_bytes(16),'hex')),$2,$3,$4,$5,$6,$7,$8)`,
		l.ID, l.Time, l.Actor, l.Action, l.EntityType, l.EntityID, l.Detail, l.RequestID)
	return err
}

func (p *Postgres) AppendAudit(ctx context.Context, l domain.AuditLog) error {
	if l.Time.IsZero() {
		l.Time = time.Now()
	}
	_, err := p.pool.Exec(ctx, `
INSERT INTO audit_logs(id,ts,actor,action,entity_type,entity_id,detail,request_id)
VALUES(COALESCE(NULLIF($1,''),encode(gen_random_bytes(16),'hex')),$2,$3,$4,$5,$6,$7,$8)`,
		l.ID, l.Time, l.Actor, l.Action, l.EntityType, l.EntityID, l.Detail, l.RequestID)
	return mapErr(err)
}

func (p *Postgres) ListAudit(ctx context.Context, f AuditFilter) ([]domain.AuditLog, int, error) {
	var conds []string
	var args []any
	add := func(col, val string) {
		conds = append(conds, fmt.Sprintf("%s=$%d", col, len(args)+1))
		args = append(args, val)
	}
	if f.EntityType != "" {
		add("entity_type", f.EntityType)
	}
	if f.EntityID != "" {
		add("entity_id", f.EntityID)
	}
	if f.Actor != "" {
		add("actor", f.Actor)
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	q := `SELECT id,ts,actor,action,entity_type,entity_id,detail,request_id FROM audit_logs ` +
		where + ` ORDER BY ts DESC`
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d OFFSET %d", f.Limit, f.Offset)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []domain.AuditLog{}
	for rows.Next() {
		var l domain.AuditLog
		if err := rows.Scan(&l.ID, &l.Time, &l.Actor, &l.Action, &l.EntityType,
			&l.EntityID, &l.Detail, &l.RequestID); err != nil {
			return nil, 0, err
		}
		out = append(out, l)
	}
	return out, total, rows.Err()
}

// SeedDemoDisks is unsupported on Postgres; use the demo CLI against the
// in-memory store, or register real hardware through the API.
func (p *Postgres) SeedDemoDisks(ctx context.Context, dir string, operator string) (string, error) {
	return "", errors.New("SeedDemoDisks is only supported in memory/demo mode")
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w: %s", ErrConflict, pgErr.Message)
		case "23514", "P0001", "23503": // check violation / raise exception / FK
			msg := pgErr.Message
			if strings.Contains(msg, "append-only") || strings.Contains(msg, "illegal asset transition") {
				return fmt.Errorf("%w: %s", ErrConflict, msg)
			}
			return fmt.Errorf("%w: %s", ErrConflict, msg)
		}
	}
	return err
}
