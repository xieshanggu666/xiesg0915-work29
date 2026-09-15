package worker

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"idc/decommission/internal/config"
	"idc/decommission/internal/domain"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/service"
	"idc/decommission/internal/store"
)

type harness struct {
	st  store.Store
	obj objectstore.Store
	wk  *Worker
	svc *service.Service
	dir string
	cfg config.Config
}

func newHarness(t *testing.T, workers int) *harness {
	t.Helper()
	dir := t.TempDir()
	diskDir := filepath.Join(dir, "disks")
	reportDir := filepath.Join(dir, "reports")
	_ = os.MkdirAll(diskDir, 0o750)
	cfg := config.Config{
		Workers: workers, ChunkBytes: 1 << 20, CheckpointEvery: 4 << 20,
		StaleAfterSec: 2, DevicePathPrefix: diskDir + ",", ReportDir: reportDir,
	}
	st := store.NewMemory()
	obj := objectstore.NewFilesystem(reportDir, "erasure-reports")
	_ = obj.EnsureBucket(context.Background())
	wk := New("test-worker", cfg, st, obj, log.New(os.Stderr, "", 0))
	return &harness{st: st, obj: obj, wk: wk, svc: service.New(st), dir: dir, cfg: cfg}
}

// makeDiskFile creates a disk image with a marker in the first bytes.
func makeDiskFile(t *testing.T, h *harness, serial string, size int64) string {
	t.Helper()
	p := filepath.Join(filepath.Join(h.dir, "disks"), serial+".img")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(strings.Repeat("CONFIDENTIAL", 100)), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return p
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// registerAssetWithDisks builds an asset in disk_pulled state with n disks.
// It walks the real gate: owner submits the decommission approval, the
// security officer approves, then disks are pulled.
func (h *harness) registerAssetWithDisks(t *testing.T, n int, size int64) domain.Asset {
	t.Helper()
	ctx := context.Background()
	a, err := h.svc.RegisterAsset(ctx, service.RegisterInput{
		Tag: "AST-" + domain.ID()[:8], Vendor: "Dell", Model: "R740",
		SN: domain.ID()[:10], Room: "DC-A", Rack: "R1", Owner: "ops", Operator: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	ap, err := h.svc.SubmitApproval(ctx, a.ID, service.ApprovalInput{
		Standard: "nist_clear", Method: "destroy", Reason: "到达退役周期", Operator: "ops",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.ReviewApproval(ctx, ap.ID, "sec-officer", true, "同意退役"); err != nil {
		t.Fatal(err)
	}
	var inputs []service.DiskInput
	for i := 0; i < n; i++ {
		serial := "DK" + domain.ID()[:8]
		path := makeDiskFile(t, h, serial, size)
		inputs = append(inputs, service.DiskInput{
			Serial: serial, Model: "ST8T", Kind: "HDD",
			CapacityGB: 8000, DevicePath: path, Slot: string(rune('0' + i)),
		})
	}
	a, disks, err := h.svc.PullDisks(ctx, a.ID, "alice", inputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(disks) != n {
		t.Fatalf("disks = %d", len(disks))
	}
	return a
}

// TestConcurrentErasureManyDevices wipes 6 disks with worker pool size 3 and
// confirms every disk ends verified with a certificate.
func TestConcurrentErasureManyDevices(t *testing.T) {
	h := newHarness(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.wk.Start(ctx)

	a := h.registerAssetWithDisks(t, 6, 24<<20)
	disks, _ := h.st.ListDisks(ctx, a.ID)
	var wg sync.WaitGroup
	for _, d := range disks {
		wg.Add(1)
		go func(diskID string) {
			defer wg.Done()
			if _, err := h.svc.CreateErasureJob(ctx, diskID, "nist_clear", "alice"); err != nil {
				t.Error(err)
			}
		}(d.ID)
	}
	wg.Wait()

	waitFor(t, 30*time.Second, func() bool {
		for _, d := range disks {
			got, _ := h.st.GetDisk(ctx, d.ID)
			if got.Status != domain.DiskVerified {
				return false
			}
		}
		certs, _ := h.st.ListCertificates(ctx, a.ID)
		return len(certs) == len(disks)
	})

	certs, err := h.st.ListCertificates(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(certs) != len(disks) {
		t.Fatalf("certs = %d, want %d", len(certs), len(disks))
	}
	// report objects must exist in the object store
	for _, c := range certs {
		rc, _, err := h.obj.Get(ctx, c.ReportKey)
		if err != nil {
			t.Fatalf("report %s: %v", c.ReportKey, err)
		}
		rc.Close()
		if len(c.ReportSHA256) != 64 {
			t.Fatalf("bad sha256 on cert %s", c.CertNo)
		}
	}
	asset, _ := h.st.GetAsset(ctx, a.ID)
	if asset.Status != domain.AssetErasureDone {
		t.Fatalf("asset status = %s, want erasure_verified", asset.Status)
	}
}

// TestFailedVerifyThenReerase: corrupt a chunk after writing so that verify
// fails; then queue a fresh job (重擦) and confirm eventual verification + cert.
func TestFailedVerifyThenReerase(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.wk.Start(ctx)

	a := h.registerAssetWithDisks(t, 1, 16<<20)
	disks, _ := h.st.ListDisks(ctx, a.ID)
	d := disks[0]

	// Run one successful wipe first.
	j, err := h.svc.CreateErasureJob(ctx, d.ID, "nist_clear", "alice")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, func() bool {
		g, _ := h.st.GetDisk(ctx, d.ID)
		return g.Status == domain.DiskVerified
	})
	_ = j

	// Tamper with the image, then request a standalone reverify — must fail.
	// To exercise the failed-job path directly, tamper *during* a wipe is
	// nondeterministic, so instead we tamper and force reverify to report bad.
	f, err := os.OpenFile(d.DevicePath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 10<<20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	res, err := h.wk.Reverify(ctx, d.ID, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if res.Mismatches < 4 {
		t.Fatalf("expected mismatches, got %d", res.Mismatches)
	}

	// Decision: re-erase (重擦). A new job attempt wipes again and verifies.
	if _, err := h.svc.CreateErasureJob(ctx, d.ID, "nist_clear", "bob"); err != nil {
		t.Fatal(err)
	}
	// the second certificate is issued only after the new job verifies;
	// wait on the cert row, not on the job status (issuance follows it)
	waitFor(t, 15*time.Second, func() bool {
		cs, _ := h.st.ListCertificates(ctx, a.ID)
		return len(cs) == 2
	})
	certs, _ := h.st.ListCertificates(ctx, a.ID)
	if len(certs) != 2 {
		t.Fatalf("expected 2 certificates (one per verified attempt), got %d", len(certs))
	}
}

// TestScrapAfterFailure: disk that cannot be wiped goes to scrap; asset ends
// scrapped and cannot be disposed as resale-ready until the scrapped route.
func TestScrapAfterFailure(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := h.registerAssetWithDisks(t, 1, 8<<20)
	disks, _ := h.st.ListDisks(ctx, a.ID)
	d := disks[0]

	if err := h.svc.ScrapDisk(ctx, d.ID, "carol", "盘面划伤，SMART 报错，复验失败"); err != nil {
		t.Fatal(err)
	}
	g, _ := h.st.GetDisk(ctx, d.ID)
	if g.Status != domain.DiskScrapped {
		t.Fatalf("disk = %s", g.Status)
	}
	aa, _ := h.st.GetAsset(ctx, a.ID)
	if aa.Status != domain.AssetScrapped {
		t.Fatalf("asset = %s, want scrapped", aa.Status)
	}

	// scrapped device can be signed off via destroy disposal
	conf, err := h.svc.ConfirmDisposal(ctx, a.ID, service.DisposalInput{
		Method: "destroy", Receiver: "zhao", ReceiverOrg: "第三方销毁公司",
		Note: "物理粉碎", Operator: "carol",
	})
	if err != nil {
		t.Fatal(err)
	}
	if conf.AssetID != a.ID {
		t.Fatal("wrong asset on confirmation")
	}

	// the signed confirmation must be immutable: a second sign is rejected
	_, err = h.svc.ConfirmDisposal(ctx, a.ID, service.DisposalInput{
		Method: "resale", Receiver: "li", Operator: "mallory",
	})
	if err == nil {
		t.Fatal("expected overwrite to be rejected")
	}
	got, _ := h.st.GetDisposal(ctx, a.ID)
	if got.Receiver != "zhao" {
		t.Fatalf("confirmation was overwritten: receiver=%s", got.Receiver)
	}
}

// TestCertificateIsImmutable: attempting to save a second certificate with the
// same cert number fails.
func TestCertificateIsImmutable(t *testing.T) {
	h := newHarness(t, 1)
	ctx := context.Background()
	a := h.registerAssetWithDisks(t, 1, 4<<20)
	disks, _ := h.st.ListDisks(ctx, a.ID)
	c := domain.Certificate{CertNo: "CERT-DUP", DiskID: disks[0].ID, AssetID: a.ID,
		JobID: domain.ID(), Standard: "nist_clear", Operator: "x"}
	if err := h.st.SaveCertificate(ctx, c); err != nil {
		t.Fatal(err)
	}
	c.Operator = "mallory"
	if err := h.st.SaveCertificate(ctx, c); err == nil {
		t.Fatal("duplicate certificate must be rejected")
	}
}

// TestAuditTrailListsOperatorAndTime: every state change appears with actor.
func TestAuditTrailListsOperatorAndTime(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.wk.Start(ctx)

	a := h.registerAssetWithDisks(t, 1, 8<<20)
	disks, _ := h.st.ListDisks(ctx, a.ID)
	if _, err := h.svc.CreateErasureJob(ctx, disks[0].ID, "nist_clear", "alice"); err != nil {
		t.Fatal(err)
	}
	// certificate issuance follows the disk-verified flip; wait for the
	// worker audit rows rather than for an intermediate state
	waitFor(t, 15*time.Second, func() bool {
		logs, total, _ := h.st.ListAudit(ctx, store.AuditFilter{EntityType: "asset", EntityID: a.ID, Limit: 100})
		if total < 3 {
			return false
		}
		for _, l := range logs {
			if l.Actor == "test-worker" {
				return true
			}
		}
		return false
	})
	logs, total, err := h.st.ListAudit(ctx, store.AuditFilter{EntityType: "asset", EntityID: a.ID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if total < 3 {
		t.Fatalf("audit rows for asset = %d", total)
	}
	actors := map[string]bool{}
	for _, l := range logs {
		if l.Time.IsZero() {
			t.Fatal("audit row without timestamp")
		}
		actors[l.Actor] = true
	}
	if !actors["alice"] {
		t.Fatalf("alice missing from audit actors: %v", actors)
	}
	if !actors["test-worker"] {
		t.Fatalf("worker missing from audit actors: %v", actors)
	}
}
