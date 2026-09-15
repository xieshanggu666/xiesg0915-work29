// Package domain holds the core entities and the state machines of the
// equipment decommission & data erasure system.
package domain

import "time"

// ID generates a random 32-char hex identifier.
func ID() string { return newID() }

// ---- Asset (资产 / 设备) ----

// AssetStatus is the asset lifecycle state. The allowed forward transitions
// are declared in AssetTransitions below.
type AssetStatus string

const (
	AssetInService   AssetStatus = "in_service"       // 在用
	AssetPending     AssetStatus = "pending"          // 退役登记完成，待拆盘
	AssetDiskPulled  AssetStatus = "disk_pulled"      // 已拆盘
	AssetErasing     AssetStatus = "erasing"          // 擦除进行中
	AssetErasureDone AssetStatus = "erasure_verified" // 擦除并校验通过
	AssetScrapped    AssetStatus = "scrapped"         // 报废（擦除失败/物理损坏）
	AssetDisposed    AssetStatus = "disposed"         // 处置签收，终态
)

// AssetTransitions is the allowed state machine for an asset.
var AssetTransitions = map[AssetStatus]map[AssetStatus]bool{
	AssetInService:   {AssetPending: true},
	AssetPending:     {AssetDiskPulled: true},
	AssetDiskPulled:  {AssetErasing: true, AssetScrapped: true},
	AssetErasing:     {AssetErasureDone: true, AssetScrapped: true},
	AssetErasureDone: {AssetDisposed: true},
	AssetScrapped:    {AssetDisposed: true},
}

type Asset struct {
	ID           string      `json:"id"`
	Tag          string      `json:"tag"`      // 资产编号
	Hostname     string      `json:"hostname"` // 原主机名
	Vendor       string      `json:"vendor"`   // 厂商
	Model        string      `json:"model"`    // 型号
	SN           string      `json:"sn"`       // 设备序列号
	Room         string      `json:"room"`     // 机房
	Rack         string      `json:"rack"`     // 机柜
	Owner        string      `json:"owner"`    // 责任人
	Status       AssetStatus `json:"status"`
	RegisteredBy string      `json:"registered_by"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

// ---- Disk (磁盘) ----

type DiskStatus string

const (
	DiskPulled   DiskStatus = "pulled" // 已拆下待擦
	DiskErasing  DiskStatus = "erasing"
	DiskVerified DiskStatus = "verified" // 擦除+校验通过
	DiskFailed   DiskStatus = "failed"   // 校验失败，等待 重擦/报废 决策
	DiskScrapped DiskStatus = "scrapped" // 报废
)

type Disk struct {
	ID         string     `json:"id"`
	AssetID    string     `json:"asset_id"`
	Serial     string     `json:"serial"`
	Model      string     `json:"model"`
	Kind       string     `json:"kind"` // HDD / SSD / NVME
	CapacityGB int64      `json:"capacity_gb"`
	DevicePath string     `json:"device_path"` // 例如 /dev/sdb 或演示用文件路径
	Slot       string     `json:"slot"`
	Status     DiskStatus `json:"status"`
	PulledBy   string     `json:"pulled_by"`
	PulledAt   *time.Time `json:"pulled_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

// ---- Erasure standard (覆写标准) ----

// PatternType defines how one overwrite pass writes bytes.
type PatternType string

const (
	PatternZeros  PatternType = "zeros"  // 全 0x00
	PatternOnes   PatternType = "ones"   // 全 0xFF
	PatternRandom PatternType = "random" // 密码学随机流（按块可重现，见 erasure 包）
)

type PassSpec struct {
	Index   int         `json:"index"`
	Pattern PatternType `json:"pattern"`
}

// Standard describes an overwrite standard.
type Standard struct {
	Code        string     `json:"code"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Passes      []PassSpec `json:"passes"`
	// VerifyMode: "full" 全盘逐字节复验, "sample" 抽样复验
	VerifyMode string `json:"verify_mode"`
}

// ---- Erasure job (擦除任务) ----

type JobStatus string

const (
	JobPending   JobStatus = "pending"   // 排队
	JobRunning   JobStatus = "running"   // 覆写中（含断电中断）
	JobVerifying JobStatus = "verifying" // 覆写完成，复验中
	JobVerified  JobStatus = "verified"  // 复验通过
	JobFailed    JobStatus = "failed"    // 复验失败，等待决策
)

type ErasureJob struct {
	ID          string     `json:"id"`
	DiskID      string     `json:"disk_id"`
	Standard    string     `json:"standard"`
	Status      JobStatus  `json:"status"`
	Attempt     int        `json:"attempt"`      // 第几次擦除（重擦递增）
	CurrentPass int        `json:"current_pass"` // 从 0 开始
	TotalBytes  int64      `json:"total_bytes"`
	BytesDone   int64      `json:"bytes_done"` // 当前 pass 已写字节
	Error       string     `json:"error,omitempty"`
	CreatedBy   string     `json:"created_by"`
	LockedBy    string     `json:"locked_by,omitempty"` // worker currently owning the run
	LockedAt    time.Time  `json:"locked_at,omitempty"` // acquisition lock / fallback heartbeat
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Checkpoint is the resume cursor persisted periodically.
// After power loss the worker resumes exactly at (Pass, Offset).
type Checkpoint struct {
	JobID       string    `json:"job_id"`
	Pass        int       `json:"pass"`
	Offset      int64     `json:"offset"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ErasureRun is one immutable execution attempt record (append-only history).
type ErasureRun struct {
	ID          string    `json:"id"`
	JobID       string    `json:"job_id"`
	Attempt     int       `json:"attempt"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Resumed     bool      `json:"resumed"`
	WroteBytes  int64     `json:"wrote_bytes"`
	VerifyMode  string    `json:"verify_mode"`
	VerifyBytes int64     `json:"verify_bytes"`
	Mismatches  int64     `json:"mismatches"`
	Result      string    `json:"result"` // verified / failed / interrupted
	Detail      string    `json:"detail,omitempty"`
}

// VerifySamplePoint is a checked range during sampled verification.
type VerifySamplePoint struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

type VerifyResult struct {
	Mode       string              `json:"mode"`
	Bytes      int64               `json:"bytes"`
	Mismatches int64               `json:"mismatches"`
	Samples    []VerifySamplePoint `json:"samples,omitempty"`
}

// ---- Certificate (擦除证明, append-only) ----

type Certificate struct {
	CertNo       string     `json:"cert_no"`
	DiskID       string     `json:"disk_id"`
	AssetID      string     `json:"asset_id"`
	JobID        string     `json:"job_id"`
	Standard     string     `json:"standard"`
	StandardName string     `json:"standard_name"`
	DiskSerial   string     `json:"disk_serial"`
	DiskModel    string     `json:"disk_model"`
	CapacityGB   int64      `json:"capacity_gb"`
	AssetTag     string     `json:"asset_tag"`
	Passes       []PassSpec `json:"passes"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   time.Time  `json:"finished_at"`
	Attempts     int        `json:"attempts"`
	VerifyMode   string     `json:"verify_mode"`
	VerifyBytes  int64      `json:"verify_bytes"`
	ReportKey    string     `json:"report_key"` // MinIO object key
	CertKey      string     `json:"cert_key"`
	ReportSHA256 string     `json:"report_sha256"`
	Operator     string     `json:"operator"`
	CreatedAt    time.Time  `json:"created_at"`
}

// ---- Disposal confirmation (处置签收, 不可覆盖) ----

type DisposalConfirmation struct {
	ID          string    `json:"id"`
	AssetID     string    `json:"asset_id"`
	AssetTag    string    `json:"asset_tag"`
	Method      string    `json:"method"`       // reuse 再利用 / resale 转售 / destroy 物理销毁
	Receiver    string    `json:"receiver"`     // 签收人
	ReceiverOrg string    `json:"receiver_org"` // 签收单位
	Note        string    `json:"note"`
	Operator    string    `json:"operator"`
	SignedAt    time.Time `json:"signed_at"`
}

// ---- Decommission approval (退役审批单) ----

// ApprovalStatus is the lifecycle of a decommission approval request.
// The allowed transitions are declared in ApprovalTransitions below.
type ApprovalStatus string

const (
	ApprovalPending   ApprovalStatus = "pending"   // 待安全员审核
	ApprovalApproved  ApprovalStatus = "approved"  // 审核通过，允许拆盘/擦除/签收
	ApprovalRejected  ApprovalStatus = "rejected"  // 已驳回（终态，可重新提交新单）
	ApprovalWithdrawn ApprovalStatus = "withdrawn" // 执行前申请人撤回（终态）
)

// ApprovalTransitions is the allowed state machine for an approval request.
// 驳回重提 / 撤回重提都通过新建审批单完成（旧单保留为历史），因此这里
// rejected/withdrawn 没有出边。
var ApprovalTransitions = map[ApprovalStatus]map[ApprovalStatus]bool{
	ApprovalPending:  {ApprovalApproved: true, ApprovalRejected: true, ApprovalWithdrawn: true},
	ApprovalApproved: {ApprovalWithdrawn: true},
}

// DecommissionApproval is one submitted 退役审批单. An asset may resubmit
// after a rejection/withdrawal; every submission is a new row with Version
// incremented, so the full review history stays queryable.
type DecommissionApproval struct {
	ID         string         `json:"id"`
	AssetID    string         `json:"asset_id"`
	AssetTag   string         `json:"asset_tag"`
	Standard   string         `json:"standard"`  // 申请的擦除标准
	Method     string         `json:"method"`    // 申请的处置方式 reuse/resale/destroy
	Reason     string         `json:"reason"`    // 退役原因
	Applicant  string         `json:"applicant"` // 申请人（资产负责人）
	Status     ApprovalStatus `json:"status"`
	Reviewer   string         `json:"reviewer,omitempty"`    // 审核人（安全员）
	ReviewNote string         `json:"review_note,omitempty"` // 审核意见
	ReviewedAt *time.Time     `json:"reviewed_at,omitempty"`
	Version    int            `json:"version"` // 第几次提交（驳回/撤回后重提递增）
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// Active reports whether the approval still gates execution: pending (awaiting
// review) or approved (execution unlocked). Rejected/withdrawn are history.
func (a DecommissionApproval) Active() bool {
	return a.Status == ApprovalPending || a.Status == ApprovalApproved
}

// ---- Audit log (审计, append-only) ----

type AuditLog struct {
	ID         string    `json:"id"`
	Time       time.Time `json:"time"`
	Actor      string    `json:"actor"`
	Action     string    `json:"action"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Detail     string    `json:"detail,omitempty"`
	RequestID  string    `json:"request_id,omitempty"`
}
