// Package service holds the business orchestration shared by the HTTP API and
// the CLI: it composes the Store and enforces lifecycle rules beyond the
// per-row constraints enforced in the database.
package service

import (
	"context"
	"errors"
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

// ---- 退役审批单 ----

// ApprovalInput is a decommission request submitted by the asset owner.
type ApprovalInput struct {
	Standard string `json:"standard"` // 申请的擦除标准
	Method   string `json:"method"`   // 申请的处置方式 reuse/resale/destroy
	Reason   string `json:"reason"`   // 退役原因
	Operator string `json:"-"`        // from X-Operator header
}

// SubmitApproval files a decommission request for an asset. The applicant
// must be the asset owner (资产负责人提交); the asset must not have started
// execution and must have no other open request. Re-submission after a
// rejection/withdrawal creates a new row with version+1 (驳回重提).
func (s *Service) SubmitApproval(ctx context.Context, assetID string, in ApprovalInput) (domain.DecommissionApproval, error) {
	if in.Operator == "" {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: operator is required (X-Operator header)", store.ErrConflict)
	}
	a, err := s.St.GetAsset(ctx, assetID)
	if err != nil {
		return domain.DecommissionApproval{}, err
	}
	if a.Owner != "" && in.Operator != a.Owner {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: only the asset owner (%s) may submit the decommission request", store.ErrConflict, a.Owner)
	}
	if a.Status != domain.AssetPending {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: asset %s already in execution (status=%s); cannot submit approval",
			store.ErrConflict, assetID, a.Status)
	}
	std, err := domain.GetStandard(in.Standard)
	if err != nil {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: %v", store.ErrConflict, err)
	}
	switch in.Method {
	case "reuse", "resale", "destroy":
	default:
		return domain.DecommissionApproval{}, fmt.Errorf("%w: method must be reuse|resale|destroy", store.ErrConflict)
	}
	if _, err := s.St.GetActiveApproval(ctx, assetID); err == nil {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: asset already has an active approval; withdraw it before resubmitting", store.ErrConflict)
	} else if !errors.Is(err, store.ErrNotFound) {
		return domain.DecommissionApproval{}, err
	}
	version := 1
	if prev, err := s.St.ListApprovals(ctx, assetID); err == nil {
		for _, p := range prev {
			if p.Version >= version {
				version = p.Version + 1
			}
		}
	}
	ap := domain.DecommissionApproval{
		ID: domain.ID(), AssetID: assetID, AssetTag: a.Tag,
		Standard: std.Code, Method: in.Method, Reason: in.Reason,
		Applicant: in.Operator, Status: domain.ApprovalPending, Version: version,
	}
	if err := s.St.CreateApproval(ctx, ap); err != nil {
		return domain.DecommissionApproval{}, err
	}
	return s.St.GetApproval(ctx, ap.ID)
}

// ReviewApproval is the security officer's decision on a pending request.
// 禁止申请人自审: the reviewer must differ from the applicant. Rejection
// requires a note so the owner knows what to fix before resubmitting.
func (s *Service) ReviewApproval(ctx context.Context, approvalID, reviewer string, approve bool, note string) (domain.DecommissionApproval, error) {
	if reviewer == "" {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: operator is required (X-Operator header)", store.ErrConflict)
	}
	ap, err := s.St.GetApproval(ctx, approvalID)
	if err != nil {
		return domain.DecommissionApproval{}, err
	}
	if ap.Status != domain.ApprovalPending {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: approval is %s, only pending requests can be reviewed", store.ErrConflict, ap.Status)
	}
	if reviewer == ap.Applicant {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: applicant cannot review their own request (禁止自审)", store.ErrConflict)
	}
	to := domain.ApprovalApproved
	if !approve {
		to = domain.ApprovalRejected
		if note == "" {
			return domain.DecommissionApproval{}, fmt.Errorf("%w: review note is required when rejecting", store.ErrConflict)
		}
	}
	return s.St.ReviewApproval(ctx, approvalID, to, reviewer, note)
}

// WithdrawApproval cancels an open request. Only the applicant may withdraw,
// and only before execution has started (asset still pending — 执行前撤回).
func (s *Service) WithdrawApproval(ctx context.Context, approvalID, operator string) (domain.DecommissionApproval, error) {
	if operator == "" {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: operator is required (X-Operator header)", store.ErrConflict)
	}
	ap, err := s.St.GetApproval(ctx, approvalID)
	if err != nil {
		return domain.DecommissionApproval{}, err
	}
	if operator != ap.Applicant {
		return domain.DecommissionApproval{}, fmt.Errorf("%w: only the applicant (%s) may withdraw the request", store.ErrConflict, ap.Applicant)
	}
	return s.St.WithdrawApproval(ctx, approvalID, operator)
}

// approvedApproval returns the asset's approved decommission approval, or a
// conflict error blocking execution: 审批通过前禁止拆盘、擦除与签收。
func (s *Service) approvedApproval(ctx context.Context, assetID string) (domain.DecommissionApproval, error) {
	ap, err := s.St.GetActiveApproval(ctx, assetID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ap, fmt.Errorf("%w: asset has no approved decommission request; submit one and pass security review first (未提交退役审批)", store.ErrConflict)
		}
		return ap, err
	}
	if ap.Status != domain.ApprovalApproved {
		return ap, fmt.Errorf("%w: decommission approval is %s; execution is allowed only after security review passes", store.ErrConflict, ap.Status)
	}
	return ap, nil
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
	// 审批通过后才允许拆盘
	if _, err := s.approvedApproval(ctx, assetID); err != nil {
		return domain.Asset{}, nil, err
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
	// 审批通过后才允许擦除，且擦除标准必须与审批通过的标准一致
	ap, err := s.approvedApproval(ctx, d.AssetID)
	if err != nil {
		return domain.ErasureJob{}, err
	}
	if ap.Standard != std.Code {
		return domain.ErasureJob{}, fmt.Errorf("%w: erasure standard %s does not match the approved standard %s",
			store.ErrConflict, std.Code, ap.Standard)
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
	// 审批通过后才允许签收，且处置方式须与审批一致；
	// 例外：设备已报废（scrapped）时允许按 destroy 签收——物理销毁是
	// 报废资产唯一可行的终态，原计划（如转售）已因擦除失败失效。
	ap, err := s.approvedApproval(ctx, assetID)
	if err != nil {
		return domain.DisposalConfirmation{}, err
	}
	scrappedDestroy := a.Status == domain.AssetScrapped && in.Method == "destroy"
	if in.Method != ap.Method && !scrappedDestroy {
		return domain.DisposalConfirmation{}, fmt.Errorf("%w: disposal method %s does not match the approved method %s",
			store.ErrConflict, in.Method, ap.Method)
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
