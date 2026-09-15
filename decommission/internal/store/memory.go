package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"idc/decommission/internal/domain"
)

// Memory is an in-process Store used for local development, demos and unit
// tests. It mimics the transactional semantics that matter to the worker:
// atomic state transitions, unique constraints and append-only tables.
type Memory struct {
	mu          sync.Mutex
	assets      map[string]domain.Asset
	tagIndex    map[string]string
	disks       map[string]domain.Disk
	jobs        map[string]domain.ErasureJob
	checkpoints map[string]domain.Checkpoint
	runs        map[string][]domain.ErasureRun
	certs       map[string]domain.Certificate
	disposals   map[string]domain.DisposalConfirmation
	audit       []domain.AuditLog
}

func NewMemory() *Memory {
	return &Memory{
		assets:      map[string]domain.Asset{},
		tagIndex:    map[string]string{},
		disks:       map[string]domain.Disk{},
		jobs:        map[string]domain.ErasureJob{},
		checkpoints: map[string]domain.Checkpoint{},
		runs:        map[string][]domain.ErasureRun{},
		certs:       map[string]domain.Certificate{},
		disposals:   map[string]domain.DisposalConfirmation{},
	}
}

func (m *Memory) Ping(context.Context) error { return nil }

// ---- assets ----

func (m *Memory) CreateAsset(ctx context.Context, a domain.Asset) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tagIndex[a.Tag]; ok {
		return fmt.Errorf("%w: asset tag %q already exists", ErrConflict, a.Tag)
	}
	if a.ID == "" {
		a.ID = domain.ID()
	}
	now := time.Now()
	a.CreatedAt, a.UpdatedAt = now, now
	m.assets[a.ID] = a
	m.tagIndex[a.Tag] = a.ID
	m.appendAuditLocked(domain.AuditLog{
		Actor: a.RegisteredBy, Action: "asset.register",
		EntityType: "asset", EntityID: a.ID,
		Detail: fmt.Sprintf("登记退役设备 %s (%s %s SN:%s)", a.Tag, a.Vendor, a.Model, a.SN),
	})
	return nil
}

func (m *Memory) GetAsset(ctx context.Context, id string) (domain.Asset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.assets[id]
	if !ok {
		return domain.Asset{}, fmt.Errorf("%w: asset %s", ErrNotFound, id)
	}
	return a, nil
}

func (m *Memory) GetAssetByTag(ctx context.Context, tag string) (domain.Asset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.tagIndex[tag]
	if !ok {
		return domain.Asset{}, fmt.Errorf("%w: asset tag %s", ErrNotFound, tag)
	}
	return m.assets[id], nil
}

func (m *Memory) ListAssets(ctx context.Context, f ListAssetsFilter) ([]domain.Asset, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.Asset{}
	for _, a := range m.assets {
		if f.Status != "" && a.Status != f.Status {
			continue
		}
		if f.Tag != "" && !strings.Contains(a.Tag, f.Tag) {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	total := len(out)
	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	}
	if f.Limit > 0 && f.Limit < len(out) {
		out = out[:f.Limit]
	}
	return out, total, nil
}

func (m *Memory) TransitionAsset(ctx context.Context, id string, to domain.AssetStatus, actor, action, detail string) (domain.Asset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.assets[id]
	if !ok {
		return domain.Asset{}, fmt.Errorf("%w: asset %s", ErrNotFound, id)
	}
	if err := domain.CanTransitionAsset(a.Status, to); err != nil {
		return domain.Asset{}, fmt.Errorf("%w: %v", ErrConflict, err)
	}
	a.Status = to
	a.UpdatedAt = time.Now()
	m.assets[id] = a
	m.appendAuditLocked(domain.AuditLog{
		Actor: actor, Action: action, EntityType: "asset", EntityID: id, Detail: detail,
	})
	return a, nil
}

// ---- disks ----

func (m *Memory) CreateDisk(ctx context.Context, d domain.Disk) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d.ID == "" {
		d.ID = domain.ID()
	}
	if _, ok := m.assets[d.AssetID]; !ok {
		return fmt.Errorf("%w: asset %s", ErrNotFound, d.AssetID)
	}
	d.CreatedAt = time.Now()
	m.disks[d.ID] = d
	return nil
}

func (m *Memory) GetDisk(ctx context.Context, id string) (domain.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.disks[id]
	if !ok {
		return domain.Disk{}, fmt.Errorf("%w: disk %s", ErrNotFound, id)
	}
	return d, nil
}

func (m *Memory) ListDisks(ctx context.Context, assetID string) ([]domain.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.Disk{}
	for _, d := range m.disks {
		if assetID == "" || d.AssetID == assetID {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (m *Memory) UpdateDiskStatus(ctx context.Context, id string, status domain.DiskStatus, actor, detail string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.disks[id]
	if !ok {
		return fmt.Errorf("%w: disk %s", ErrNotFound, id)
	}
	d.Status = status
	m.disks[id] = d
	m.appendAuditLocked(domain.AuditLog{
		Actor: actor, Action: "disk.status:" + string(status),
		EntityType: "disk", EntityID: id, Detail: detail,
	})
	return nil
}

// ---- jobs ----

func (m *Memory) CreateJob(ctx context.Context, j domain.ErasureJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.ID == "" {
		j.ID = domain.ID()
	}
	if _, ok := m.disks[j.DiskID]; !ok {
		return fmt.Errorf("%w: disk %s", ErrNotFound, j.DiskID)
	}
	now := time.Now()
	j.CreatedAt, j.UpdatedAt = now, now
	m.jobs[j.ID] = j
	m.appendAuditLocked(domain.AuditLog{
		Actor: j.CreatedBy, Action: "job.create",
		EntityType: "job", EntityID: j.ID,
		Detail: fmt.Sprintf("创建擦除任务 disk=%s standard=%s attempt=%d", j.DiskID, j.Standard, j.Attempt),
	})
	return nil
}

func (m *Memory) GetJob(ctx context.Context, id string) (domain.ErasureJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return domain.ErasureJob{}, fmt.Errorf("%w: job %s", ErrNotFound, id)
	}
	return j, nil
}

func (m *Memory) ListJobs(ctx context.Context, diskID string) ([]domain.ErasureJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.ErasureJob{}
	for _, j := range m.jobs {
		if diskID == "" || j.DiskID == diskID {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// AcquireNextJob claims either a queued pending job or a stale running job
// whose heartbeat expired (power-loss recovery). One job is claimed at a
// time; the worker pool calls this under its own concurrency limiter.
func (m *Memory) AcquireNextJob(ctx context.Context, workerID string, staleAfterSec int) (AcquireResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	// staleAfterSec == 0 means "process (re)start": a previous owner is assumed
	// dead, so jobs locked by a *different* worker are reclaimed at once. Jobs
	// this worker already owns are never stolen (its slots hold them anyway).
	startup := staleAfterSec == 0
	var pick *domain.ErasureJob
	for _, j := range m.jobs {
		switch j.Status {
		case domain.JobPending:
			// prefer oldest pending
			if pick == nil || (pick.Status != domain.JobPending) || j.CreatedAt.Before(pick.CreatedAt) {
				jj := j
				pick = &jj
			}
		case domain.JobRunning, domain.JobVerifying:
			if j.LockedBy == workerID {
				continue // still owned by this worker
			}
			if startup {
				jj := j
				pick = &jj
				continue
			}
			cp, ok := m.checkpoints[j.ID]
			heartbeat := j.LockedAt
			if ok && cp.HeartbeatAt.After(heartbeat) {
				heartbeat = cp.HeartbeatAt
			}
			if heartbeat.IsZero() || now.Sub(heartbeat) > time.Duration(staleAfterSec)*time.Second {
				jj := j
				pick = &jj
			}
		}
	}
	if pick == nil {
		return AcquireResult{}, nil
	}
	// Atomically claim: a pending job becomes running in the same locked
	// section so a concurrent scheduler tick (or another process) can never
	// acquire the same job twice. Stale orphans keep status running/verifying.
	pickedID := pick.ID
	{
		j := m.jobs[pickedID]
		// pending jobs become running atomically here; stale orphans just have
		// their lock refreshed. In both cases LockedAt is the fallback
		// heartbeat until the first real checkpoint lands.
		if j.Status == domain.JobPending {
			j.Status = domain.JobRunning
		}
		j.LockedBy = workerID
		j.LockedAt = now
		j.UpdatedAt = now
		m.jobs[pickedID] = j
		pick = &j
	}
	d := m.disks[pick.DiskID]
	m.appendAuditLocked(domain.AuditLog{
		Actor: workerID, Action: "job.acquire", EntityType: "job", EntityID: pickedID,
		Detail: fmt.Sprintf("worker=%s status=%s pass=%d offset=%d",
			workerID, pick.Status, pick.CurrentPass, pick.BytesDone),
	})
	return AcquireResult{Job: *pick, Disk: d, OK: true}, nil
}

func (m *Memory) MarkJobRunning(ctx context.Context, jobID string, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	now := time.Now()
	j.Status = domain.JobRunning
	j.Attempt = attempt
	if j.StartedAt == nil {
		j.StartedAt = &now
	}
	j.LockedAt = now
	// LockedBy is set when the job is acquired; MarkJobRunning does not change it.
	j.UpdatedAt = now
	m.jobs[jobID] = j
	return nil
}

func (m *Memory) SaveProgress(ctx context.Context, jobID string, pass int, offset, total int64, status domain.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	now := time.Now()
	j.CurrentPass, j.BytesDone, j.TotalBytes = pass, offset, total
	j.Status = status
	j.UpdatedAt = now
	m.jobs[jobID] = j
	m.checkpoints[jobID] = domain.Checkpoint{
		JobID: jobID, Pass: pass, Offset: offset, HeartbeatAt: now, UpdatedAt: now,
	}
	return nil
}

func (m *Memory) CompleteJob(ctx context.Context, jobID string, status domain.JobStatus, errMsg string, run domain.ErasureRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}
	now := time.Now()
	j.Status, j.Error, j.FinishedAt, j.UpdatedAt = status, errMsg, &now, now
	m.jobs[jobID] = j
	run.ID, run.JobID, run.Attempt = domain.ID(), jobID, j.Attempt
	m.runs[jobID] = append(m.runs[jobID], run)
	return nil
}

func (m *Memory) RecordRun(ctx context.Context, run domain.ErasureRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if run.ID == "" {
		run.ID = domain.ID()
	}
	m.runs[run.JobID] = append(m.runs[run.JobID], run)
	return nil
}

func (m *Memory) ListRuns(ctx context.Context, jobID string) ([]domain.ErasureRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]domain.ErasureRun(nil), m.runs[jobID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

func (m *Memory) GetCheckpoint(ctx context.Context, jobID string) (domain.Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.checkpoints[jobID]
	if !ok {
		return domain.Checkpoint{}, fmt.Errorf("%w: checkpoint for %s", ErrNotFound, jobID)
	}
	return cp, nil
}

// ---- certificates ----

func (m *Memory) SaveCertificate(ctx context.Context, c domain.Certificate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.certs[c.CertNo]; ok {
		// certificates are append-only: never overwrite
		return fmt.Errorf("%w: certificate %s already exists and cannot be overwritten", ErrConflict, c.CertNo)
	}
	c.CreatedAt = time.Now()
	m.certs[c.CertNo] = c
	m.appendAuditLocked(domain.AuditLog{
		Actor: c.Operator, Action: "certificate.issue",
		EntityType: "certificate", EntityID: c.CertNo,
		Detail: fmt.Sprintf("签发擦除证明 disk=%s asset=%s report=%s", c.DiskID, c.AssetID, c.ReportKey),
	})
	return nil
}

func (m *Memory) GetCertificate(ctx context.Context, certNo string) (domain.Certificate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.certs[certNo]
	if !ok {
		return domain.Certificate{}, fmt.Errorf("%w: certificate %s", ErrNotFound, certNo)
	}
	return c, nil
}

func (m *Memory) ListCertificates(ctx context.Context, assetID string) ([]domain.Certificate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.Certificate{}
	for _, c := range m.certs {
		if assetID == "" || c.AssetID == assetID {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// ---- disposal ----

func (m *Memory) SaveDisposal(ctx context.Context, d domain.DisposalConfirmation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.disposals[d.AssetID]; ok {
		return fmt.Errorf("%w: disposal confirmation for asset %s is already signed and cannot be overwritten",
			ErrConflict, d.AssetID)
	}
	a, ok := m.assets[d.AssetID]
	if !ok {
		return fmt.Errorf("%w: asset %s", ErrNotFound, d.AssetID)
	}
	if err := domain.CanTransitionAsset(a.Status, domain.AssetDisposed); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	if d.ID == "" {
		d.ID = domain.ID()
	}
	d.SignedAt = time.Now()
	m.disposals[d.AssetID] = d
	a.Status = domain.AssetDisposed
	a.UpdatedAt = d.SignedAt
	m.assets[d.AssetID] = a
	m.appendAuditLocked(domain.AuditLog{
		Actor: d.Operator, Action: "disposal.sign",
		EntityType: "asset", EntityID: d.AssetID,
		Detail: fmt.Sprintf("处置签收 method=%s receiver=%s/%s", d.Method, d.ReceiverOrg, d.Receiver),
	})
	return nil
}

func (m *Memory) GetDisposal(ctx context.Context, assetID string) (domain.DisposalConfirmation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.disposals[assetID]
	if !ok {
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: disposal for %s", ErrNotFound, assetID)
	}
	return d, nil
}

// ---- audit ----

func (m *Memory) appendAuditLocked(l domain.AuditLog) {
	if l.ID == "" {
		l.ID = domain.ID()
	}
	if l.Time.IsZero() {
		l.Time = time.Now()
	}
	m.audit = append(m.audit, l)
}

func (m *Memory) AppendAudit(ctx context.Context, l domain.AuditLog) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendAuditLocked(l)
	return nil
}

func (m *Memory) ListAudit(ctx context.Context, f AuditFilter) ([]domain.AuditLog, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []domain.AuditLog{}
	for _, l := range m.audit {
		if f.EntityType != "" && l.EntityType != f.EntityType {
			continue
		}
		if f.EntityID != "" && l.EntityID != f.EntityID {
			continue
		}
		if f.Actor != "" && l.Actor != f.Actor {
			continue
		}
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	total := len(out)
	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	}
	if f.Limit > 0 && f.Limit < len(out) {
		out = out[:f.Limit]
	}
	return out, total, nil
}

// SeedDemoDisks creates sparse regular files standing in for block devices
// and registers them as disks on a freshly created asset. Only used by the
// `demo` command so the system is runnable without real hardware.
func (m *Memory) SeedDemoDisks(ctx context.Context, dir string, operator string) (string, error) {
	a := domain.Asset{
		Tag: fmt.Sprintf("AST-DEMO-%03d", time.Now().Unix()%1000), Hostname: "demo-db-01",
		Vendor: "DEMO", Model: "R740xd", SN: "DEMOSN" + domain.ID()[:8],
		Room: "DC-A", Rack: "R01-U10", Owner: operator,
		Status: domain.AssetPending, RegisteredBy: operator,
	}
	if err := m.CreateAsset(ctx, a); err != nil {
		return "", err
	}
	created, err := m.GetAssetByTag(ctx, a.Tag)
	if err != nil {
		return "", err
	}
	now := time.Now()
	specs := []struct {
		serial, model, kind string
		capGB               int64
		size                int64
	}{
		{"DEMO-DISK-0001", "ST-DEMO-8TB", "HDD", 8000, 260 << 20},
		{"DEMO-DISK-0002", "ST-DEMO-8TB", "HDD", 8000, 260 << 20},
	}
	for _, s := range specs {
		path := filepath.Join(dir, s.serial+".img")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			return "", err
		}
		if err := f.Truncate(s.size); err != nil {
			f.Close()
			return "", err
		}
		// stamp the first block with non-zero "data" so the demo verifies
		// something was actually overwritten
		if _, err := f.WriteAt([]byte("CONFIDENTIAL-DEMO-DATA-PATTERN"), 0); err != nil {
			f.Close()
			return "", err
		}
		f.Close()
		d := domain.Disk{
			AssetID:    created.ID,
			Serial:     s.serial,
			Model:      s.model,
			Kind:       s.kind,
			CapacityGB: s.capGB,
			DevicePath: path,
			Slot:       "0",
			Status:     domain.DiskPulled,
			PulledBy:   operator,
			PulledAt:   &now,
		}
		if err := m.CreateDisk(ctx, d); err != nil {
			return "", err
		}
	}
	if _, err := m.TransitionAsset(ctx, created.ID, domain.AssetDiskPulled, operator, "asset.disk_pull", "演示设备拆盘完成"); err != nil {
		return "", err
	}
	return created.ID, nil
}
