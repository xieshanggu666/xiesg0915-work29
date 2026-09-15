// Package service holds the business orchestration shared by the HTTP API and
// the CLI: it composes the Store and enforces lifecycle rules beyond the
// per-row constraints enforced in the database.
package service

import (
	"context"
	"fmt"
	"time"

	"idc/decommission/internal/domain"
	"idc/decommission/internal/store"
)

type Service struct{ St store.Store }

func New(st store.Store) *Service { return &Service{St: st} }

// RegisterAsset puts a newly retired device into the pipeline (pending).
type RegisterInput struct {
	Tag      string `json:"tag"`
	Hostname string `json:"hostname"`
	Vendor   string `json:"vendor"`
	Model    string `json:"model"`
	SN       string `json:"sn"`
	Room     string `json:"room"`
	Rack     string `json:"rack"`
	Owner    string `json:"owner"`
	Operator string `json:"-"` // from X-Operator header
}

func (s *Service) RegisterAsset(ctx context.Context, in RegisterInput) (domain.Asset, error) {
	if in.Tag == "" {
		return domain.Asset{}, fmt.Errorf("%w: tag is required", store.ErrConflict)
	}
	if in.Operator == "" {
		return domain.Asset{}, fmt.Errorf("%w: operator is required (X-Operator header)", store.ErrConflict)
	}
	a := domain.Asset{
		Tag: in.Tag, Hostname: in.Hostname, Vendor: in.Vendor, Model: in.Model, SN: in.SN,
		Room: in.Room, Rack: in.Rack, Owner: in.Owner,
		Status: domain.AssetPending, RegisteredBy: in.Operator,
	}
	if err := s.St.CreateAsset(ctx, a); err != nil {
		return domain.Asset{}, err
	}
	return s.St.GetAssetByTag(ctx, in.Tag)
}

// DiskInput describes one pulled disk.
type DiskInput struct {
	Serial     string `json:"serial"`
	Model      string `json:"model"`
	Kind       string `json:"kind"`
	CapacityGB int64  `json:"capacity_gb"`
	DevicePath string `json:"device_path"`
	Slot       string `json:"slot"`
}

// PullDisks records the disks removed from a device and moves the asset from
// pending to disk_pulled. The operator pulling is recorded on every disk.
func (s *Service) PullDisks(ctx context.Context, assetID, operator string, disks []DiskInput) (domain.Asset, []domain.Disk, error) {
	if operator == "" {
		return domain.Asset{}, nil, fmt.Errorf("%w: operator required", store.ErrConflict)
	}
	if len(disks) == 0 {
		return domain.Asset{}, nil, fmt.Errorf("%w: at least one disk is required", store.ErrConflict)
	}
	a, err := s.St.GetAsset(ctx, assetID)
	if err != nil {
		return domain.Asset{}, nil, err
	}
	if a.Status != domain.AssetPending {
		return domain.Asset{}, nil, fmt.Errorf("%w: can only pull disks while asset is pending (current=%s)",
			store.ErrConflict, a.Status)
	}
	now := time.Now()
	var created []domain.Disk
	for _, in := range disks {
		if in.Serial == "" || in.DevicePath == "" {
			return a, nil, fmt.Errorf("%w: disk serial and device_path are required", store.ErrConflict)
		}
		d := domain.Disk{
			ID:      domain.ID(),
			AssetID: assetID, Serial: in.Serial, Model: in.Model,
			Kind: orDefault(in.Kind, "HDD"), CapacityGB: in.CapacityGB,
			DevicePath: in.DevicePath, Slot: in.Slot,
			Status: domain.DiskPulled, PulledBy: operator, PulledAt: &now,
		}
		if err := s.St.CreateDisk(ctx, d); err != nil {
			return a, nil, err
		}
		created = append(created, d)
	}
	a, err = s.St.TransitionAsset(ctx, assetID, domain.AssetDiskPulled, operator,
		"asset.disk_pull", fmt.Sprintf("拆盘 %d 块", len(created)))
	if err != nil {
		return a, nil, err
	}
	return a, created, nil
}

// CreateErasureJob queues a disk for wiping. The asset moves to erasing when
// its first job starts (lazy transition in the worker); here the disk must be
// in pulled or failed state.
func (s *Service) CreateErasureJob(ctx context.Context, diskID, standard, operator string) (domain.ErasureJob, error) {
	if operator == "" {
		return domain.ErasureJob{}, fmt.Errorf("%w: operator required", store.ErrConflict)
	}
	std, err := domain.GetStandard(standard)
	if err != nil {
		return domain.ErasureJob{}, fmt.Errorf("%w: %v", store.ErrConflict, err)
	}
	d, err := s.St.GetDisk(ctx, diskID)
	if err != nil {
		return domain.ErasureJob{}, err
	}
	switch d.Status {
	case domain.DiskPulled, domain.DiskFailed:
	default:
		return domain.ErasureJob{}, fmt.Errorf("%w: disk %s is %s, cannot queue job",
			store.ErrConflict, diskID, d.Status)
	}
	// attempt = number of prior jobs for this disk
	prior, err := s.St.ListJobs(ctx, diskID)
	if err != nil {
		return domain.ErasureJob{}, err
	}
	j := domain.ErasureJob{
		DiskID: diskID, Standard: std.Code,
		Status: domain.JobPending, Attempt: len(prior), CreatedBy: operator,
	}
	if err := s.St.CreateJob(ctx, j); err != nil {
		return domain.ErasureJob{}, err
	}
	// best-effort asset -> erasing
	if d.Status == domain.DiskPulled {
		_, _ = s.St.TransitionAsset(ctx, d.AssetID, domain.AssetErasing, operator,
			"asset.erasing", "创建首个擦除任务，进入擦除")
	}
	all, err := s.St.ListJobs(ctx, diskID)
	if err != nil || len(all) == 0 {
		return j, nil
	}
	return all[0], nil
}

// ScrapDisk handles the failed-verification decision "报废": the disk is
// physically destroyed/quarantined and never re-wiped. The containing asset
// may still move forward as scrapped.
func (s *Service) ScrapDisk(ctx context.Context, diskID, operator, reason string) error {
	if operator == "" {
		return fmt.Errorf("%w: operator required", store.ErrConflict)
	}
	d, err := s.St.GetDisk(ctx, diskID)
	if err != nil {
		return err
	}
	if d.Status != domain.DiskFailed && d.Status != domain.DiskPulled {
		return fmt.Errorf("%w: only failed/pulled disks can be scrapped (current=%s)",
			store.ErrConflict, d.Status)
	}
	if err := s.St.UpdateDiskStatus(ctx, diskID, domain.DiskScrapped, operator,
		"报废："+reason); err != nil {
		return err
	}
	// mark any open job failed
	jobs, _ := s.St.ListJobs(ctx, diskID)
	for _, j := range jobs {
		if j.Status == domain.JobPending || j.Status == domain.JobRunning || j.Status == domain.JobFailed {
			if j.Status != domain.JobFailed {
				_ = s.St.CompleteJob(ctx, j.ID, domain.JobFailed, "disk scrapped: "+reason, domain.ErasureRun{
					StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
					Result: "failed", Detail: "磁盘报废，任务终止：" + reason,
				})
			}
		}
	}
	// asset transitions to scrapped only if not already beyond this point
	a, err := s.St.GetAsset(ctx, d.AssetID)
	if err != nil {
		return err
	}
	if a.Status == domain.AssetDiskPulled || a.Status == domain.AssetErasing {
		_, _ = s.St.TransitionAsset(ctx, a.ID, domain.AssetScrapped, operator,
			"asset.scrap", "存在报废磁盘："+reason)
	}
	return nil
}

// ConfirmDisposal signs off the final disposition. The signed row is unique
// per asset and append-only, so a second call can never overwrite the first.
type DisposalInput struct {
	Method      string `json:"method"`
	Receiver    string `json:"receiver"`
	ReceiverOrg string `json:"receiver_org"`
	Note        string `json:"note"`
	Operator    string `json:"-"` // from X-Operator header
}

func (s *Service) ConfirmDisposal(ctx context.Context, assetID string, in DisposalInput) (domain.DisposalConfirmation, error) {
	if in.Operator == "" {
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: operator required", store.ErrConflict)
	}
	switch in.Method {
	case "reuse", "resale", "destroy":
	default:
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: method must be reuse|resale|destroy", store.ErrConflict)
	}
	if in.Receiver == "" {
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: receiver is required", store.ErrConflict)
	}
	a, err := s.St.GetAsset(ctx, assetID)
	if err != nil {
		return domain.DisposalConfirmation{}, err
	}
	switch a.Status {
	case domain.AssetErasureDone, domain.AssetScrapped:
	case domain.AssetDisposed:
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: asset already disposed; signature cannot be overwritten", store.ErrConflict)
	default:
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: asset not ready for disposal (status=%s)",
			store.ErrConflict, a.Status)
	}
	conf := domain.DisposalConfirmation{
		AssetID: assetID, AssetTag: a.Tag, Method: in.Method,
		Receiver: in.Receiver, ReceiverOrg: in.ReceiverOrg, Note: in.Note, Operator: in.Operator,
	}
	if err := s.St.SaveDisposal(ctx, conf); err != nil {
		return domain.DisposalConfirmation{}, err
	}
	return s.St.GetDisposal(ctx, assetID)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
