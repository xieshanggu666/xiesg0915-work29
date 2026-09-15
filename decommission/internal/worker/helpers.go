package worker

import (
	"bytes"

	"idc/decommission/internal/domain"
)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// operatorOf returns the human accountable for a job — the operator who
// queued it — falling back to the automated worker name when absent.
func operatorOf(j domain.ErasureJob) string {
	if j.CreatedBy != "" {
		return j.CreatedBy
	}
	return "system"
}
