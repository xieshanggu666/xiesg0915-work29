// Package worker runs the erasure scheduler: it claims queued or stale
// (power-loss orphaned) jobs, opens the target device, resumes from the
// last checkpoint, verifies, and on success archives a report + certificate.
// Verification failures flip the disk to "failed" and wait for an operator
// decision: re-erase (new attempt) or scrap.
package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"idc/decommission/internal/config"
	"idc/decommission/internal/domain"
	"idc/decommission/internal/erasure"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/report"
	"idc/decommission/internal/store"
)

type Worker struct {
	id   string // human-readable base name (audit logs)
	inst string // unique per process instance: this is the lock owner
	cfg  config.Config
	st   store.Store
	obj  objectstore.Store
	log  *log.Logger
	sem  chan struct{} // limits concurrent device operations

	// reclaimImmediately is set on (re)start: jobs left in running/verifying
	// by a previous process lifetime (power loss / hard kill) are reclaimed
	// at once rather than waiting for the heartbeat timeout — at boot a dead
	// previous owner is a fact, not a suspicion.
	reclaimImmediately atomic.Bool

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New creates a worker. name labels audit entries; every process instance gets
// a unique lock-owner id so that after a real restart (same configured name)
// jobs held by the previous instance are still reclaimable as foreign.
func New(name string, cfg config.Config, st store.Store, obj objectstore.Store, logger *log.Logger) *Worker {
	if name == "" {
		name = "worker"
	}
	return &Worker{
		id: name, inst: name + "-" + domain.ID()[:8],
		cfg: cfg, st: st, obj: obj, log: logger,
		sem: make(chan struct{}, cfg.Workers),
	}
}

// ID returns the process-unique lock owner id.
func (w *Worker) ID() string { return w.inst }

// Name returns the stable human-readable worker name.
func (w *Worker) Name() string { return w.id }

// SetReclaimImmediately makes the next scheduler ticks reclaim jobs locked by
// a previous process immediately. CLI/demo restarts use it; long-running
// servers instead rely on the heartbeat timeout so transient slowness does not
// cause double execution.
func (w *Worker) SetReclaimImmediately(b bool) { w.reclaimImmediately.Store(b) }

// Start launches the scheduler loop.
func (w *Worker) Start(ctx context.Context) {
	// Every (re)start assumes the previous owner of running/verifying jobs is
	// dead (this is exactly what a power loss or hard kill looks like): reclaim
	// such jobs immediately for the first grace window, then fall back to
	// heartbeat-timeout detection for the rest of the process lifetime.
	w.reclaimImmediately.Store(true)
	ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		w.fillSlots(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.fillSlots(ctx)
			}
		}
	}()
	// after one grace window, stop aggressive reclaim so a briefly stalled
	// worker is not treated as dead
	go func() {
		select {
		case <-time.After(time.Duration(w.cfg.StaleAfterSec+2) * time.Second):
			w.reclaimImmediately.Store(false)
		case <-ctx.Done():
		}
	}()
}

// Stop waits for in-flight jobs to reach their next checkpoint and exits.
// Each running context is cancelled mid-chunk; progress already checkpointed
// stays durable and the job is reclaimed on next start (same as power loss).
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

// fillSlots is invoked every scheduler tick. It claims at most one job per
// free worker slot: each iteration occupies one slot, acquires exactly one
// job for that slot, and stops on the first empty acquisition. A job is
// therefore never acquired twice in the same tick, and the in-flight cap is
// always respected (the slot is held until the goroutine finishes).
// KillForDemo simulates sudden power loss: contexts are cancelled while jobs
// are mid-write, Stop is NOT called and no cleanup is performed. The durable
// checkpoint rows remain in the store; a later worker instance reclaims the
// stale running jobs and resumes them. Demo/test use only.
func (w *Worker) KillForDemo() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Worker) fillSlots(ctx context.Context) {
	for {
		// Occupy one worker slot before querying the store, so the number of
		// jobs acquired per tick can never exceed the configured concurrency.
		select {
		case w.sem <- struct{}{}:
		default:
			return // all slots busy
		}
		staleAfter := w.cfg.StaleAfterSec
		if w.reclaimImmediately.Load() {
			staleAfter = 0
		}
		res, err := w.st.AcquireNextJob(ctx, w.inst, staleAfter)
		if err != nil {
			w.log.Printf("acquire: %v", err)
			<-w.sem
			return
		}
		if !res.OK {
			<-w.sem
			return
		}
		w.wg.Add(1)
		go func(job domain.ErasureJob, disk domain.Disk) {
			defer w.wg.Done()
			defer func() { <-w.sem }() // release the slot only when fully done
			w.runJob(ctx, job, disk)
		}(res.Job, res.Disk)
	}
}

func (w *Worker) runJob(parent context.Context, job domain.ErasureJob, disk domain.Disk) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	std, err := domain.GetStandard(job.Standard)
	if err != nil {
		w.failJob(ctx, job, disk, "", err)
		return
	}
	if err := w.cfg.AllowedDevicePath(disk.DevicePath); err != nil {
		w.failJob(ctx, job, disk, "", err)
		return
	}

	// Resume cursor. A checkpoint exists for jobs already in flight; its
	// heartbeat being stale is exactly how a power-loss orphan got claimed.
	var resumed bool
	resumePass, resumeOffset := 0, int64(0)
	if cp, err := w.st.GetCheckpoint(ctx, job.ID); err == nil {
		if job.Status != domain.JobPending {
			resumePass, resumeOffset, resumed = cp.Pass, cp.Offset, true
		}
	}

	if err := w.st.MarkJobRunning(ctx, job.ID, job.Attempt); err != nil {
		w.log.Printf("mark running %s: %v", job.ID, err)
		return
	}
	if err := w.st.UpdateDiskStatus(ctx, disk.ID, domain.DiskErasing, w.id,
		fmt.Sprintf("开始擦除 job=%s attempt=%d resumed=%v", job.ID, job.Attempt, resumed)); err != nil {
		w.log.Printf("disk status %s: %v", disk.ID, err)
	}

	dev, err := erasure.OpenDevice(disk.DevicePath)
	if err != nil {
		w.failJob(ctx, job, disk, "", fmt.Errorf("open device: %w", err))
		return
	}
	defer dev.Close()

	size, err := dev.Size()
	if err != nil {
		w.failJob(ctx, job, disk, "", err)
		return
	}

	runStart := time.Now().UTC()
	var wrote int64
	onProgress := func(p erasure.Progress) error {
		status := domain.JobRunning
		if p.Phase == "verify" {
			status = domain.JobVerifying
		} else {
			// only overwrite progress counts as wiped bytes
			wrote = p.Offset
		}
		if err := w.st.SaveProgress(ctx, job.ID, p.Pass, p.Offset, p.Total, status); err != nil {
			return err
		}
		return ctx.Err()
	}

	verifyRes, runErr := erasure.Run(ctx, dev, erasure.Options{
		Standard:       std,
		JobID:          job.ID,
		ChunkSize:      w.cfg.ChunkBytes,
		CheckpointStep: w.cfg.CheckpointEvery,
		ResumePass:     resumePass,
		ResumeOffset:   resumeOffset,
	}, onProgress)

	finished := time.Now().UTC()
	switch {
	case runErr != nil:
		// Cancellation here means shutdown (power loss in production is a kill;
		// both leave the durable checkpoint in place). The next scheduler tick /
		// process restart reclaims the stale job and resumes.
		if ctx.Err() != nil || isInterrupted(runErr) {
			w.st.RecordRun(ctx, domain.ErasureRun{
				JobID: job.ID, Attempt: job.Attempt, StartedAt: runStart, FinishedAt: finished,
				Resumed: resumed, WroteBytes: wrote, Result: "interrupted",
				Detail: "任务中断（断电/停机），已保存断点，等待续做",
			})
			w.log.Printf("job %s interrupted at pass=%d offset=%d, will resume", job.ID, resumePass, wrote)
			return
		}
		w.failJob(ctx, job, disk, "", runErr)
		return
	case verifyRes.Mismatches > 0:
		detail := fmt.Sprintf("复验未通过，不一致字节数=%d（复验模式=%s,复验字节=%d）",
			verifyRes.Mismatches, verifyRes.Mode, verifyRes.Bytes)
		_ = w.st.CompleteJob(ctx, job.ID, domain.JobFailed, detail, domain.ErasureRun{
			StartedAt: runStart, FinishedAt: finished, Resumed: resumed,
			WroteBytes: wrote, VerifyMode: verifyRes.Mode, VerifyBytes: verifyRes.Bytes,
			Mismatches: verifyRes.Mismatches, Result: "failed", Detail: detail,
		})
		_ = w.st.UpdateDiskStatus(ctx, disk.ID, domain.DiskFailed, w.id, detail)
		w.log.Printf("job %s VERIFICATION FAILED mismatches=%d -> disk %s awaiting decision",
			job.ID, verifyRes.Mismatches, disk.ID)
		return
	}

	// ---- verification passed: finalize ----
	_ = w.st.SaveProgress(ctx, job.ID, len(std.Passes)-1, size, size, domain.JobVerified)
	// Flip the disk to its terminal state BEFORE issuing the certificate.
	// Disk status is the durable "wiped" fact; certificate issuance (render +
	// object upload + insert) can then be safely retried — the immutable
	// certificate row rejects duplicate issuance rather than leaving the disk
	// stuck in erasing when the worker dies in between.
	if err := w.st.UpdateDiskStatus(ctx, disk.ID, domain.DiskVerified, w.id,
		fmt.Sprintf("擦除复验通过 job=%s，准备签发证明", job.ID)); err != nil {
		w.log.Printf("mark disk verified %s: %v", disk.ID, err)
		return
	}
	_ = w.st.CompleteJob(ctx, job.ID, domain.JobVerified, "", domain.ErasureRun{
		StartedAt: runStart, FinishedAt: finished, Resumed: resumed,
		WroteBytes: size, VerifyMode: verifyRes.Mode, VerifyBytes: verifyRes.Bytes, Result: "verified",
		Detail: fmt.Sprintf("覆写+复验通过 (size=%d)", size),
	})
	if err := w.issueCertificate(ctx, job, disk, std, size, verifyRes, runStart, finished); err != nil {
		w.log.Printf("issue certificate for %s: %v (job remains verified; retry cert issuance)", disk.ID, err)
		return
	}
	w.log.Printf("job %s verified and certified (%s, %d bytes)", job.ID, std.Name, size)
}

// Reverify runs a read-only verification of an already processed disk without
// overwriting it. It shares the worker concurrency limiter so that N devices
// can be (re)verified simultaneously. Returns the verification result.
func (w *Worker) Reverify(ctx context.Context, diskID, operator string) (*domain.VerifyResult, error) {
	disk, err := w.st.GetDisk(ctx, diskID)
	if err != nil {
		return nil, err
	}
	if disk.Status != domain.DiskVerified && disk.Status != domain.DiskFailed {
		return nil, fmt.Errorf("%w: disk %s is %s; reverify only after wiping",
			store.ErrConflict, diskID, disk.Status)
	}
	if err := w.cfg.AllowedDevicePath(disk.DevicePath); err != nil {
		return nil, err
	}
	jobs, err := w.st.ListJobs(ctx, diskID)
	if err != nil || len(jobs) == 0 {
		return nil, fmt.Errorf("no erasure job found for disk %s", diskID)
	}
	last := jobs[0] // ListJobs is newest-first
	std, err := domain.GetStandard(last.Standard)
	if err != nil {
		return nil, err
	}

	select {
	case w.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-w.sem }()

	dev, err := erasure.OpenDevice(disk.DevicePath)
	if err != nil {
		return nil, fmt.Errorf("open device: %w", err)
	}
	defer dev.Close()

	res, err := erasure.Verify(ctx, dev, erasure.Options{
		Standard: std, JobID: last.ID,
		ChunkSize: w.cfg.ChunkBytes, CheckpointStep: w.cfg.CheckpointEvery,
	}, nil)
	if err != nil {
		return nil, err
	}
	detail := fmt.Sprintf("复验 mode=%s bytes=%d mismatches=%d", res.Mode, res.Bytes, res.Mismatches)
	if res.Mismatches > 0 {
		detail = "复验发现残留数据：" + detail
		_ = w.st.UpdateDiskStatus(ctx, diskID, domain.DiskFailed, operator, detail)
	}
	_ = w.st.AppendAudit(ctx, domain.AuditLog{
		Actor: operator, Action: "disk.reverify",
		EntityType: "disk", EntityID: diskID, Detail: detail,
	})
	return res, nil
}

// issueCertificate aggregates asset+disk+runs, renders JSON+HTML, uploads them,
// persists the certificate row and flips the asset to erasure_verified once
// all its disks are verified or scrapped.
func (w *Worker) issueCertificate(ctx context.Context, job domain.ErasureJob, disk domain.Disk, std domain.Standard, size int64, vr *domain.VerifyResult, started, finished time.Time) error {
	asset, err := w.st.GetAsset(ctx, disk.AssetID)
	if err != nil {
		return err
	}
	runs, err := w.st.ListRuns(ctx, job.ID)
	if err != nil {
		return err
	}

	certNo := certNumber(asset, disk, job, finished)
	cert := domain.Certificate{
		CertNo: certNo, DiskID: disk.ID, AssetID: asset.ID, JobID: job.ID,
		Standard: std.Code, StandardName: std.Name,
		DiskSerial: disk.Serial, DiskModel: disk.Model, CapacityGB: disk.CapacityGB, AssetTag: asset.Tag,
		Passes: std.Passes, StartedAt: started, FinishedAt: finished,
		Attempts: job.Attempt + 1, VerifyMode: vr.Mode, VerifyBytes: vr.Bytes,
		Operator: operatorOf(job),
	}

	jsonOut, htmlOut, reportKey, certKey, err := report.Build(cert, asset, disk, runs)
	if err != nil {
		return err
	}
	// upload report first; its digest is referenced by the certificate row
	obj, err := w.obj.Put(ctx, reportKey, "application/json", bytesReader(jsonOut))
	if err != nil {
		return fmt.Errorf("upload report: %w", err)
	}
	if _, err := w.obj.Put(ctx, certKey, "text/html; charset=utf-8", bytesReader(htmlOut)); err != nil {
		return fmt.Errorf("upload certificate: %w", err)
	}
	cert.ReportKey = reportKey
	cert.CertKey = certKey
	cert.ReportSHA256 = obj.SHA256
	if err := w.st.SaveCertificate(ctx, cert); err != nil {
		return err
	}

	// asset reaches erasure_verified only when every disk is done
	w.maybeAssetErasureDone(ctx, asset.ID)
	return nil
}

func (w *Worker) maybeAssetErasureDone(ctx context.Context, assetID string) {
	disks, err := w.st.ListDisks(ctx, assetID)
	if err != nil {
		return
	}
	for _, d := range disks {
		switch d.Status {
		case domain.DiskVerified, domain.DiskScrapped:
			// terminal
		default:
			return
		}
	}
	if _, err := w.st.TransitionAsset(ctx, assetID, domain.AssetErasureDone, w.id,
		"asset.erasure_verified", "全部磁盘处理完成（擦除复验通过或已报废）"); err != nil {
		w.log.Printf("asset transition: %v", err)
	}
}

func (w *Worker) failJob(ctx context.Context, job domain.ErasureJob, disk domain.Disk, _ string, cause error) {
	w.log.Printf("job %s failed: %v", job.ID, cause)
	_ = w.st.CompleteJob(ctx, job.ID, domain.JobFailed, cause.Error(), domain.ErasureRun{
		FinishedAt: time.Now().UTC(), Result: "failed", Detail: cause.Error(),
	})
}

// certNumber: human readable, e.g. CERT-20260915-AST001-DK9F3-003
func certNumber(asset domain.Asset, disk domain.Disk, job domain.ErasureJob, t time.Time) string {
	return fmt.Sprintf("CERT-%s-%s-%s-%03d",
		t.Format("20060102"),
		sanitize(asset.Tag),
		sanitize(shortSerial(disk.Serial)),
		job.Attempt+1)
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		}
	}
	if len(out) > 12 {
		out = out[:12]
	}
	if len(out) == 0 {
		return "X"
	}
	return string(out)
}

func shortSerial(s string) string {
	if len(s) > 8 {
		return s[len(s)-8:]
	}
	return s
}

func isInterrupted(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
