package domain

import "fmt"

// Built-in overwrite standards. These mirror the common NIST SP 800-88 /
// DoD-style patterns used in data-center decommissioning.
var standards = map[string]Standard{
	"nist_clear": {
		Code: "nist_clear", Name: "NIST 800-88 Clear",
		Description: "全盘单次零覆写 + 全盘复验（适用于介质在本组织内复用）",
		Passes:      []PassSpec{{Index: 0, Pattern: PatternZeros}},
		VerifyMode:  "full",
	},
	"nist_purge": {
		Code: "nist_purge", Name: "NIST 800-88 Purge",
		Description: "0xFF 覆写、随机覆写、0x00 覆写三道 + 全盘复验（用于介质离开组织）",
		Passes: []PassSpec{
			{Index: 0, Pattern: PatternOnes},
			{Index: 1, Pattern: PatternRandom},
			{Index: 2, Pattern: PatternZeros},
		},
		VerifyMode: "full",
	},
	"dod_3pass": {
		Code: "dod_3pass", Name: "DoD 5220.22-M (3-pass)",
		Description: "零/一/随机三道覆写 + 全盘复验（常见合同要求）",
		Passes: []PassSpec{
			{Index: 0, Pattern: PatternZeros},
			{Index: 1, Pattern: PatternOnes},
			{Index: 2, Pattern: PatternRandom},
		},
		VerifyMode: "full",
	},
	"sample_quick": {
		Code: "sample_quick", Name: "快速擦除(单零+抽样复验)",
		Description: "全盘单次零覆写 + 抽样复验（仅用于低密级演示/演练）",
		Passes:      []PassSpec{{Index: 0, Pattern: PatternZeros}},
		VerifyMode:  "sample",
	},
}

// GetStandard returns a built-in standard by code.
func GetStandard(code string) (Standard, error) {
	if s, ok := standards[code]; ok {
		return s, nil
	}
	return Standard{}, fmt.Errorf("unknown erasure standard %q", code)
}

// ListStandards returns all built-in standards.
func ListStandards() []Standard {
	out := make([]Standard, 0, len(standards))
	for _, s := range standards {
		out = append(out, s)
	}
	return out
}

// CanTransitionAsset validates an asset status change.
func CanTransitionAsset(from, to AssetStatus) error {
	if from == to {
		return nil
	}
	if allowed, ok := AssetTransitions[from]; ok && allowed[to] {
		return nil
	}
	return fmt.Errorf("illegal asset transition %s -> %s", from, to)
}

// CanTransitionApproval validates an approval status change.
func CanTransitionApproval(from, to ApprovalStatus) error {
	if from == to {
		return nil
	}
	if allowed, ok := ApprovalTransitions[from]; ok && allowed[to] {
		return nil
	}
	return fmt.Errorf("illegal approval transition %s -> %s", from, to)
}
