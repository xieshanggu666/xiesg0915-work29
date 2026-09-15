package api

import (
	"net/http"
	"testing"
)

// TestApprovalWorkflow covers the full 退役审批单 state machine over HTTP:
// submit (owner only) -> reject -> resubmit -> withdraw -> resubmit ->
// approve -> withdraw-before-execution -> resubmit -> approve -> execution
// locks withdrawal. It also checks the validation errors and the
// scrapped-asset destroy exception at disposal.
func TestApprovalWorkflow(t *testing.T) {
	e := newAPIEnv(t, 1)

	status, body := e.req(t, "POST", "/api/v1/assets", "alice", map[string]any{
		"tag": "AST-APPR", "vendor": "Dell", "model": "R740", "owner": "alice",
	})
	if status != http.StatusCreated {
		t.Fatalf("register: %d %v", status, body)
	}
	assetID := dataID(body)
	submit := func(operator string, standard, method string) (int, map[string]any) {
		return e.req(t, "POST", "/api/v1/assets/"+assetID+"/approvals", operator,
			map[string]any{"standard": standard, "method": method, "reason": "服役期满"})
	}
	review := func(id, operator string, approve bool, note string) (int, map[string]any) {
		return e.req(t, "POST", "/api/v1/approvals/"+id+"/review", operator,
			map[string]any{"approve": approve, "note": note})
	}
	withdraw := func(id, operator string) (int, map[string]any) {
		return e.req(t, "POST", "/api/v1/approvals/"+id+"/withdraw", operator, nil)
	}

	// ---- validation ----
	if status, _ = submit("", "nist_clear", "resale"); status != http.StatusConflict {
		t.Fatalf("submit without operator: %d", status)
	}
	if status, body = submit("bob", "nist_clear", "resale"); status != http.StatusConflict {
		t.Fatalf("non-owner submit must be rejected: %d %v", status, body)
	}
	if status, _ = submit("alice", "zeros_1pass", "resale"); status != http.StatusConflict {
		t.Fatalf("unknown standard: %d", status)
	}
	if status, _ = submit("alice", "nist_clear", "landfill"); status != http.StatusConflict {
		t.Fatalf("unknown method: %d", status)
	}

	// ---- v1: submit -> reject ----
	status, body = submit("alice", "nist_clear", "resale")
	if status != http.StatusCreated {
		t.Fatalf("submit v1: %d %v", status, body)
	}
	v1 := dataID(body)
	if body["data"].(map[string]any)["status"] != "pending" {
		t.Fatalf("v1 status = %v", body["data"])
	}
	if v := body["data"].(map[string]any)["version"].(float64); v != 1 {
		t.Fatalf("v1 version = %v", v)
	}
	// one active request per asset
	if status, _ = submit("alice", "nist_clear", "resale"); status != http.StatusConflict {
		t.Fatalf("duplicate active approval: %d", status)
	}
	// reviewer identity required; self-review forbidden; reject needs a note
	if status, _ = review(v1, "", true, ""); status != http.StatusConflict {
		t.Fatalf("review without operator: %d", status)
	}
	if status, body = review(v1, "alice", true, ""); status != http.StatusConflict {
		t.Fatalf("self-review must be forbidden: %d %v", status, body)
	}
	if status, _ = review(v1, "sec-admin", false, ""); status != http.StatusConflict {
		t.Fatalf("reject without note: %d", status)
	}
	status, body = review(v1, "sec-admin", false, "标准偏低，介质将离网，需 purge")
	if status != http.StatusOK || body["data"].(map[string]any)["status"] != "rejected" {
		t.Fatalf("reject: %d %v", status, body)
	}
	// rejected is terminal for this row
	if status, _ = review(v1, "sec-admin", true, ""); status != http.StatusConflict {
		t.Fatalf("re-review of rejected: %d", status)
	}
	if status, _ = withdraw(v1, "alice"); status != http.StatusConflict {
		t.Fatalf("withdraw rejected: %d", status)
	}

	// ---- v2: 驳回重提 -> 执行前撤回 ----
	status, body = submit("alice", "nist_purge", "resale")
	if status != http.StatusCreated {
		t.Fatalf("resubmit v2: %d %v", status, body)
	}
	v2 := dataID(body)
	if v := body["data"].(map[string]any)["version"].(float64); v != 2 {
		t.Fatalf("v2 version = %v", v)
	}
	if std := body["data"].(map[string]any)["standard"].(string); std != "nist_purge" {
		t.Fatalf("v2 standard = %s", std)
	}
	if status, _ = withdraw(v2, "mallory"); status != http.StatusConflict {
		t.Fatalf("non-applicant withdraw: %d", status)
	}
	status, body = withdraw(v2, "alice")
	if status != http.StatusOK || body["data"].(map[string]any)["status"] != "withdrawn" {
		t.Fatalf("withdraw v2: %d %v", status, body)
	}

	// ---- v3: 撤回后重提 -> 通过 -> 执行前仍可撤回 ----
	status, body = submit("alice", "nist_purge", "resale")
	if status != http.StatusCreated {
		t.Fatalf("resubmit v3: %d %v", status, body)
	}
	v3 := dataID(body)
	status, body = review(v3, "sec-admin", true, "同意")
	if status != http.StatusOK || body["data"].(map[string]any)["status"] != "approved" {
		t.Fatalf("approve v3: %d %v", status, body)
	}
	if body["data"].(map[string]any)["reviewer"].(string) != "sec-admin" {
		t.Fatalf("reviewer = %v", body["data"])
	}
	// approved but execution not started: withdrawal still allowed
	status, body = withdraw(v3, "alice")
	if status != http.StatusOK || body["data"].(map[string]any)["status"] != "withdrawn" {
		t.Fatalf("withdraw approved v3: %d %v", status, body)
	}

	// ---- v4: approve -> execution starts -> withdrawal locked ----
	status, body = submit("alice", "nist_purge", "resale")
	if status != http.StatusCreated {
		t.Fatalf("resubmit v4: %d %v", status, body)
	}
	v4 := dataID(body)
	if v := body["data"].(map[string]any)["version"].(float64); v != 4 {
		t.Fatalf("v4 version = %v", v)
	}
	if status, _ = review(v4, "sec-admin", true, "同意"); status != http.StatusOK {
		t.Fatalf("approve v4: %d", status)
	}
	path := makeDiskImage(t, e, "APPR-D1", 4<<20)
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disks", "bob",
		map[string]any{"disks": []map[string]any{{
			"serial": "APPR-D1", "kind": "HDD", "capacity_gb": 100, "device_path": path,
		}}})
	if status != http.StatusCreated {
		t.Fatalf("pull after approval: %d %v", status, body)
	}
	if status, _ = withdraw(v4, "alice"); status != http.StatusConflict {
		t.Fatalf("withdraw after execution started must be blocked: %d", status)
	}

	// history: 4 submissions, newest first
	status, body = e.req(t, "GET", "/api/v1/assets/"+assetID+"/approvals", "", nil)
	if status != http.StatusOK {
		t.Fatalf("list approvals: %d", status)
	}
	items := body["data"].([]any)
	if len(items) != 4 {
		t.Fatalf("approval history = %d, want 4", len(items))
	}
	if items[0].(map[string]any)["version"].(float64) != 4 {
		t.Fatalf("history not newest-first: %v", items[0])
	}
	// detail endpoint
	status, body = e.req(t, "GET", "/api/v1/approvals/"+v1, "", nil)
	if status != http.StatusOK || body["data"].(map[string]any)["status"] != "rejected" {
		t.Fatalf("get approval v1: %d %v", status, body)
	}
	status, _ = e.req(t, "GET", "/api/v1/approvals/nonexistent", "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("get missing approval: %d", status)
	}
}

// TestScrappedAssetDestroyException: an asset approved for reuse that ends up
// scrapped may still be signed off as destroy (physical destruction is the
// only viable end state), while other mismatched methods stay blocked.
func TestScrappedAssetDestroyException(t *testing.T) {
	e := newAPIEnv(t, 1)

	status, body := e.req(t, "POST", "/api/v1/assets", "alice",
		map[string]any{"tag": "AST-EXC", "owner": "alice"})
	assetID := dataID(body)
	approveAsset(t, e, assetID, "alice", "nist_clear", "reuse")

	path := makeDiskImage(t, e, "EXC-D1", 4<<20)
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disks", "bob",
		map[string]any{"disks": []map[string]any{{
			"serial": "EXC-D1", "kind": "HDD", "capacity_gb": 100, "device_path": path,
		}}})
	if status != http.StatusCreated {
		t.Fatalf("pull: %d %v", status, body)
	}
	diskID := body["data"].(map[string]any)["disks"].([]any)[0].(map[string]any)["id"].(string)
	status, _ = e.req(t, "POST", "/api/v1/disks/"+diskID+"/scrap", "carol",
		map[string]any{"reason": "盘体物理损坏"})
	if status != http.StatusOK {
		t.Fatalf("scrap: %d", status)
	}

	// resale was never approved -> blocked
	status, _ = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disposal", "carol",
		map[string]any{"method": "resale", "receiver": "x"})
	if status != http.StatusConflict {
		t.Fatalf("resale must stay blocked: %d", status)
	}
	// destroy is allowed for a scrapped asset even though reuse was approved
	status, body = e.req(t, "POST", "/api/v1/assets/"+assetID+"/disposal", "carol",
		map[string]any{"method": "destroy", "receiver": "incinerator-01"})
	if status != http.StatusCreated {
		t.Fatalf("destroy on scrapped asset: %d %v", status, body)
	}
}
