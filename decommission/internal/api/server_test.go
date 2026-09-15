package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"idc/decommission/internal/config"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/service"
	"idc/decommission/internal/store"
	"idc/decommission/internal/worker"
)

type apiEnv struct {
	srv *httptest.Server
	st  store.Store
	svc *service.Service
	wk  *worker.Worker
	dir string
}

func newAPIEnv(t *testing.T, workers int) *apiEnv {
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
	obj := objectstore.NewFilesystem(reportDir, "bucket")
	_ = obj.EnsureBucket(context.Background())
	wk := worker.New("api-worker", cfg, st, obj, log.New(io.Discard, "", 0))
	svc := service.New(st)
	srv := httptest.NewServer(NewServer(svc, st, obj, wk, log.New(io.Discard, "", 0)).Routes())
	t.Cleanup(srv.Close)
	return &apiEnv{srv: srv, st: st, svc: svc, wk: wk, dir: dir}
}

func (e *apiEnv) req(t *testing.T, method, path, operator string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if operator != "" {
		req.Header.Set("X-Operator", operator)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out
}

func dataID(m map[string]any) string { return m["data"].(map[string]any)["id"].(string) }

func makeDiskImage(t *testing.T, e *apiEnv, serial string, size int64) string {
	t.Helper()
	p := filepath.Join(e.dir, "disks", serial+".img")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Truncate(size)
	_, _ = f.WriteAt([]byte(strings.Repeat("X", 4096)), 0)
	f.Close()
	return p
}

// TestFullLifecycleOverHTTP exercises register -> approval -> pull -> wipe ->
// verify -> certificate download -> dispose -> immutable re-sign over real HTTP.
func TestFullLifecycleOverHTTP(t *testing.T) {
	e := newAPIEnv(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.wk.Start(ctx)

	// operator header is required
	status, _ := e.req(t, "POST", "/api/v1/assets", "", map[string]any{"tag": "AST-1"})
	if status != http.StatusConflict {
		t.Fatalf("missing operator: status=%d", status)
	}

	status, body := e.req(t, "POST", "/api/v1/assets", "alice", map[string]any{
		"tag": "AST-001", "vendor": "Dell", "model": "R740", "sn": "SN1",
		"room": "DC-A", "rack": "R1", "owner": "alice",
	})
	if status != http.StatusCreated {
		t.Fatalf("register: %d %v", status, body)
	}
	assetID := dataID(body)

	// duplicate tag -> conflict
	status, _ = e.req(t, "POST", "/api/v1/assets", "alice", map[string]any{"tag": "AST-001"})
	if status != http.StatusConflict {
		t.Fatalf("duplicate tag: %d", status)
	}

	diskInputs := func() []map[string]any {
		var out []map[string]any
		for _, serial := range []string{"D1", "D2"} {
			path := makeDiskImage(t, e, serial, 12<<20)
			out = append(out, map[string]any{
				"serial": serial, "model": "ST", "kind": "HDD",
				"capacity_gb": 1000, "device_path": path, "slot": "0",
			})
		}
		return out
	}

	// 审批通过前禁止拆盘
	status, _ = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disks", "bob",
		map[string]any{"disks": diskInputs()[:1]})
	if status != http.StatusConflict {
		t.Fatalf("pull before approval must be blocked: %d", status)
	}

	// 资产负责人提交退役申请；禁止自审；安全员通过
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/approvals", "alice",
		map[string]any{"standard": "nist_clear", "method": "resale", "reason": "服役期满"})
	if status != http.StatusCreated {
		t.Fatalf("submit approval: %d %v", status, body)
	}
	approvalID := dataID(body)
	status, body = e.req(t, "POST", "/api/v1/approvals/"+approvalID+"/review", "alice",
		map[string]any{"approve": true})
	if status != http.StatusConflict {
		t.Fatalf("self-review must be forbidden: %d %v", status, body)
	}
	status, body = e.req(t, "POST", "/api/v1/approvals/"+approvalID+"/review", "sec-admin",
		map[string]any{"approve": true, "note": "标准与密级匹配，同意"})
	if status != http.StatusOK {
		t.Fatalf("approve: %d %v", status, body)
	}
	if body["data"].(map[string]any)["status"] != "approved" {
		t.Fatalf("approval status = %v", body["data"])
	}

	// pull two disks (now gated open)
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disks", "bob",
		map[string]any{"disks": diskInputs()})
	if status != http.StatusCreated {
		t.Fatalf("pull disks: %d %v", status, body)
	}
	disks := body["data"].(map[string]any)["disks"].([]any)
	if len(disks) != 2 {
		t.Fatalf("disks = %d", len(disks))
	}

	// 擦除标准必须与审批一致
	firstDisk := disks[0].(map[string]any)["id"].(string)
	status, body = e.req(t, "POST", "/api/v1/disks/"+firstDisk+"/jobs", "alice",
		map[string]any{"standard": "dod_3pass"})
	if status != http.StatusConflict {
		t.Fatalf("unapproved standard must be blocked: %d %v", status, body)
	}

	// queue jobs concurrently
	for _, di := range disks {
		id := di.(map[string]any)["id"].(string)
		status, body = e.req(t, "POST", "/api/v1/disks/"+id+"/jobs", "alice",
			map[string]any{"standard": "nist_clear"})
		if status != http.StatusCreated {
			t.Fatalf("create job: %d %v", status, body)
		}
	}

	// wait for verified
	deadline := time.Now().Add(20 * time.Second)
	var certs []any
	for time.Now().Before(deadline) {
		status, body = e.req(t, "GET", "/api/v1/assets/"+assetID+"/certificates", "", nil)
		certs = body["data"].([]any)
		if len(certs) == 2 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if len(certs) != 2 {
		t.Fatalf("certs = %d", len(certs))
	}

	// both report objects downloadable
	for _, ci := range certs {
		certNo := ci.(map[string]any)["cert_no"].(string)
		req, _ := http.NewRequest("GET", e.srv.URL+"/api/v1/reports/"+certNo, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !bytes.Contains(b, []byte(`idc-erasure-report/v1`)) {
			t.Fatalf("report download status=%d len=%d", resp.StatusCode, len(b))
		}
		req, _ = http.NewRequest("GET", e.srv.URL+"/api/v1/reports/"+certNo+"/certificate", nil)
		resp2, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if resp2.StatusCode != http.StatusOK || !bytes.Contains(b2, []byte("数据擦除证明")) {
			t.Fatalf("html cert status=%d", resp2.StatusCode)
		}
	}

	// 处置方式必须与审批一致（审批的是 resale）
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disposal", "carol", map[string]any{
		"method": "destroy", "receiver": "zhao",
	})
	if status != http.StatusConflict {
		t.Fatalf("unapproved disposal method must be blocked: %d %v", status, body)
	}

	// dispose
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disposal", "carol", map[string]any{
		"method": "resale", "receiver": "zhao", "receiver_org": "dept-2",
	})
	if status != http.StatusCreated {
		t.Fatalf("dispose: %d %v", status, body)
	}

	// signature cannot be overwritten
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disposal", "mallory", map[string]any{
		"method": "destroy", "receiver": "li",
	})
	if status != http.StatusConflict {
		t.Fatalf("re-sign should conflict: %d %v", status, body)
	}
	status, body = e.req(t, "GET", "/api/v1/assets/"+assetID+"/disposal", "", nil)
	if body["data"].(map[string]any)["receiver"] != "zhao" {
		t.Fatal("signature was mutated")
	}

	// audit per device carries operators and timestamps
	status, body = e.req(t, "GET", "/api/v1/audit?entity_type=asset&entity_id="+assetID, "", nil)
	items := body["data"].(map[string]any)["items"].([]any)
	actors := map[string]bool{}
	for _, it := range items {
		row := it.(map[string]any)
		if row["time"] == "" || row["time"] == nil {
			t.Fatal("audit row without time")
		}
		actors[row["actor"].(string)] = true
	}
	for _, want := range []string{"alice", "bob", "carol", "api-worker"} {
		if !actors[want] {
			t.Fatalf("actor %s missing from audit trail: %v", want, actors)
		}
	}

	// 审批单自身也有完整审计：提交人 alice、审核人 sec-admin
	status, body = e.req(t, "GET", "/api/v1/audit?entity_type=approval&entity_id="+approvalID, "", nil)
	items = body["data"].(map[string]any)["items"].([]any)
	approvalActors := map[string]bool{}
	for _, it := range items {
		approvalActors[it.(map[string]any)["actor"].(string)] = true
	}
	if !approvalActors["alice"] || !approvalActors["sec-admin"] {
		t.Fatalf("approval audit actors = %v", approvalActors)
	}

	// timeline carries the approval history
	status, body = e.req(t, "GET", "/api/v1/assets/"+assetID+"/timeline", "", nil)
	if status != http.StatusOK {
		t.Fatalf("timeline: %d", status)
	}
	approvals := body["data"].(map[string]any)["approvals"].([]any)
	if len(approvals) != 1 {
		t.Fatalf("timeline approvals = %d", len(approvals))
	}
}

// approveAsset is the happy-path approval flow used by tests that focus on
// later stages: owner submits, security officer approves.
func approveAsset(t *testing.T, e *apiEnv, assetID, owner, standard, method string) {
	t.Helper()
	status, body := e.req(t, "POST", "/api/v1/assets/"+assetID+"/approvals", owner,
		map[string]any{"standard": standard, "method": method, "reason": "退役"})
	if status != http.StatusCreated {
		t.Fatalf("submit approval: %d %v", status, body)
	}
	status, body = e.req(t, "POST", "/api/v1/approvals/"+dataID(body)+"/review", "sec-admin",
		map[string]any{"approve": true, "note": "同意"})
	if status != http.StatusOK {
		t.Fatalf("approve: %d %v", status, body)
	}
}

// TestReverifyTamperedDisk verifies the failed-verification decision path
// through HTTP: tamper -> reverify reports mismatches -> re-erase -> verified.
func TestReverifyTamperedDisk(t *testing.T) {
	e := newAPIEnv(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.wk.Start(ctx)

	status, body := e.req(t, "POST", "/api/v1/assets", "alice",
		map[string]any{"tag": "AST-FAIL", "owner": "alice"})
	assetID := dataID(body)
	approveAsset(t, e, assetID, "alice", "nist_clear", "reuse")
	path := makeDiskImage(t, e, "DF", 10<<20)
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disks", "bob",
		map[string]any{"disks": []map[string]any{{
			"serial": "DF", "kind": "HDD", "capacity_gb": 100, "device_path": path,
		}}})
	diskID := body["data"].(map[string]any)["disks"].([]any)[0].(map[string]any)["id"].(string)

	status, _ = e.req(t, "POST", "/api/v1/disks/"+diskID+"/jobs", "alice",
		map[string]any{"standard": "nist_clear"})
	waitDisk(t, e, diskID, "verified")

	// physically tamper with the wiped image
	f, _ := os.OpenFile(path, os.O_RDWR, 0o600)
	_, _ = f.WriteAt([]byte{0xAB, 0xCD, 0xEF}, 6<<20)
	f.Close()

	status, body = e.req(t, "POST", "/api/v1/disks/"+diskID+"/reverify", "auditor", nil)
	if status != http.StatusOK {
		t.Fatalf("reverify: %d %v", status, body)
	}
	mismatches := body["data"].(map[string]any)["mismatches"].(float64)
	if mismatches < 3 {
		t.Fatalf("mismatches = %v", mismatches)
	}

	// disk flipped to failed and needs an explicit decision
	status, body = e.req(t, "GET", "/api/v1/disks/"+diskID, "", nil)
	if body["data"].(map[string]any)["status"] != "failed" {
		t.Fatal("disk should be failed after failed reverify")
	}

	// decision: re-erase
	status, _ = e.req(t, "POST", "/api/v1/disks/"+diskID+"/jobs", "bob",
		map[string]any{"standard": "nist_clear"})
	if status != http.StatusCreated {
		t.Fatalf("re-erase job: %d", status)
	}
	waitDisk(t, e, diskID, "verified")

	// alternative decision path: scrap a failed disk on another asset
	status, body = e.req(t, "POST", "/api/v1/assets", "alice",
		map[string]any{"tag": "AST-SCRAP", "owner": "alice"})
	a2 := dataID(body)
	approveAsset(t, e, a2, "alice", "nist_clear", "destroy")
	path2 := makeDiskImage(t, e, "DS", 4<<20)
	status, body = e.req(t, "POST", "/api/v1/assets/"+a2+"/disks", "bob",
		map[string]any{"disks": []map[string]any{{
			"serial": "DS", "kind": "HDD", "capacity_gb": 100, "device_path": path2,
		}}})
	d2 := body["data"].(map[string]any)["disks"].([]any)[0].(map[string]any)["id"].(string)
	status, _ = e.req(t, "POST", "/api/v1/disks/"+d2+"/scrap", "carol",
		map[string]any{"reason": "复验失败，坏道"})
	if status != http.StatusOK {
		t.Fatalf("scrap: %d", status)
	}
	// scrapped asset can still be disposed via destroy
	status, body = e.req(t, "POST", "/api/v1/assets/"+a2+"/disposal", "carol", map[string]any{
		"method": "destroy", "receiver": "incinerator-01",
	})
	if status != http.StatusCreated {
		t.Fatalf("dispose scrapped: %d %v", status, body)
	}
}

func waitDisk(t *testing.T, e *apiEnv, id, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, body := e.req(t, "GET", "/api/v1/disks/"+id, "", nil)
		if body["data"] != nil && body["data"].(map[string]any)["status"] == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("disk %s did not reach %s", id, want)
}
