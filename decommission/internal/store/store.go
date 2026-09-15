// Package store defines persistence interfaces used by the worker and HTTP
// layers. Two implementations exist: an in-memory one (demo/tests) and a
// PostgreSQL one (migrations under /migrations).
package store

import (
	"context"
	"errors"

	"idc/decommission/internal/domain"
)

// ErrNotFound is returned when no row matches.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned on state-machine violations, immutable-row
// overwrite attempts and unique violations.
var ErrConflict = errors.New("conflict")

// ListAssetsFilter narrows asset listing.
type ListAssetsFilter struct {
	Status domain.AssetStatus
	Tag    string
	Limit  int
	Offset int
}

// AuditFilter narrows audit queries.
type AuditFilter struct {
	EntityType string
	EntityID   string
	Actor      string
	Limit      int
	Offset     int
}

// AcquireResult is returned by AcqueNextJob: either Job+Disk are populated
// or false when nothing is available.
type AcquireResult struct {
	Job  domain.ErasureJob
	Disk domain.Disk
	OK   bool
}

// Store is the full persistence surface of the system.
type Store interface {
	Ping(ctx context.Context) error

	// assets
	CreateAsset(ctx context.Context, a domain.Asset) error
	GetAsset(ctx context.Context, id string) (domain.Asset, error)
	GetAssetByTag(ctx context.Context, tag string) (domain.Asset, error)
	ListAssets(ctx context.Context, f ListAssetsFilter) ([]domain.Asset, int, error)
	// TransitionAsset validates the state machine, applies it and writes
	// an audit row in the same transaction.
	TransitionAsset(ctx context.Context, id string, to domain.AssetStatus, actor, action, detail string) (domain.Asset, error)

	// disks
	CreateDisk(ctx context.Context, d domain.Disk) error
	GetDisk(ctx context.Context, id string) (domain.Disk, error)
	ListDisks(ctx context.Context, assetID string) ([]domain.Disk, error)
	UpdateDiskStatus(ctx context.Context, id string, status domain.DiskStatus, actor, detail string) error

	// jobs
	CreateJob(ctx context.Context, j domain.ErasureJob) error
	GetJob(ctx context.Context, id string) (domain.ErasureJob, error)
	ListJobs(ctx context.Context, diskID string) ([]domain.ErasureJob, error)
	// AcquireNextJob atomically claims one pending/stale job for this worker.
	// Stale running jobs (dead heartbeat after power loss) are reclaimed too.
	AcquireNextJob(ctx context.Context, workerID string, staleAfterSec int) (AcquireResult, error)
	MarkJobRunning(ctx context.Context, jobID string, attempt int) error
	// SaveProgress updates pass/offset counters and heartbeat.
	SaveProgress(ctx context.Context, jobID string, pass int, offset, total int64, status domain.JobStatus) error
	// CompleteJob marks the terminal state and records the run.
	CompleteJob(ctx context.Context, jobID string, status domain.JobStatus, errMsg string, run domain.ErasureRun) error
	// RecordRun appends an immutable execution record (also used for the
	// interrupted run discovered during power-loss recovery).
	RecordRun(ctx context.Context, run domain.ErasureRun) error
	ListRuns(ctx context.Context, jobID string) ([]domain.ErasureRun, error)
	GetCheckpoint(ctx context.Context, jobID string) (domain.Checkpoint, error)

	// certificates
	SaveCertificate(ctx context.Context, c domain.Certificate) error
	GetCertificate(ctx context.Context, certNo string) (domain.Certificate, error)
	ListCertificates(ctx context.Context, assetID string) ([]domain.Certificate, error)

	// disposal
	// SaveDisposal fails with ErrConflict if the asset already has a
	// signed confirmation — signatures may never be overwritten.
	SaveDisposal(ctx context.Context, d domain.DisposalConfirmation) error
	GetDisposal(ctx context.Context, assetID string) (domain.DisposalConfirmation, error)

	// audit
	AppendAudit(ctx context.Context, l domain.AuditLog) error
	ListAudit(ctx context.Context, f AuditFilter) ([]domain.AuditLog, int, error)

	// demo / development helper
	SeedDemoDisks(ctx context.Context, dir string, operator string) (assetID string, err error)
}
