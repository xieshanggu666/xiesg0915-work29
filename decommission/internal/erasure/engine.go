package erasure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"

	"idc/decommission/internal/domain"
)

// Progress is reported after each checkpoint-worthy chunk.
type Progress struct {
	Pass   int
	Offset int64
	Total  int64
	Phase  string // "overwrite" / "verify"
}

// Options controls a run.
type Options struct {
	Standard       domain.Standard
	JobID          string // seeds deterministic random passes
	ChunkSize      int
	CheckpointStep int64 // persist a checkpoint every N bytes
	// Resume point: pass+offset of the overwrite phase. Zero = fresh start.
	ResumePass   int
	ResumeOffset int64
	// SampleFraction for "sample" verification (0..1); 0 => default 1%.
	SampleFraction float64
}

// ProgressFn is called after every checkpoint flush; returning an error cancels
// the run (used to propagate worker shutdown / context cancellation).
type ProgressFn func(p Progress) error

// Verify performs only the verification phase against the final-pass pattern.
// It is used for standalone re-verification (复验) of an already wiped disk.
func Verify(ctx context.Context, dev Device, opt Options, onProgress ProgressFn) (*domain.VerifyResult, error) {
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 4 << 20
	}
	if opt.CheckpointStep <= 0 {
		opt.CheckpointStep = 64 << 20
	}
	size, err := dev.Size()
	if err != nil {
		return nil, fmt.Errorf("determine device size: %w", err)
	}
	if size == 0 {
		return nil, fmt.Errorf("device reports zero size")
	}
	return verify(ctx, dev, size, opt, onProgress)
}

// Run executes all overwrite passes (resuming at the checkpoint) and then
// verifies the final pass. It returns a VerifyResult describing what was checked.
//
// If ctx is cancelled mid-pass (power loss is simulated by process kill,
// but graceful stop / cancellation behaves identically), the last checkpoint
// already persisted is the resume point on the next attempt.
func Run(ctx context.Context, dev Device, opt Options, onProgress ProgressFn) (*domain.VerifyResult, error) {
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 4 << 20
	}
	if opt.CheckpointStep <= 0 {
		opt.CheckpointStep = 64 << 20
	}
	size, err := dev.Size()
	if err != nil {
		return nil, fmt.Errorf("determine device size: %w", err)
	}
	if size == 0 {
		return nil, fmt.Errorf("device reports zero size")
	}

	// ---- overwrite passes ----
	if err := overwrite(ctx, dev, size, opt, onProgress); err != nil {
		return nil, err
	}

	// ---- verification against the final pass pattern ----
	res, err := verify(ctx, dev, size, opt, onProgress)
	if err != nil {
		return res, err
	}
	return res, nil
}

func overwrite(ctx context.Context, dev Device, size int64, opt Options, onProgress ProgressFn) error {
	finalPass := len(opt.Standard.Passes) - 1
	for passIdx := opt.ResumePass; passIdx <= finalPass; passIdx++ {
		spec := opt.Standard.Passes[passIdx]
		start := int64(0)
		if passIdx == opt.ResumePass && opt.ResumeOffset > 0 {
			start = opt.ResumeOffset
			if start >= size {
				continue
			}
		}
		buf := make([]byte, opt.ChunkSize)
		offset := start
		lastCheckpoint := start
		for offset < size {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := int64(len(buf))
			if offset+n > size {
				n = size - offset
			}
			chunk := buf[:n]
			if err := streamAt(string(spec.Pattern), opt.JobID, spec.Index, chunk, offset); err != nil {
				return err
			}
			if _, err := dev.WriteAt(chunk, offset); err != nil {
				return fmt.Errorf("write pass %d offset %d: %w", spec.Index, offset, err)
			}
			offset += n

			if offset-lastCheckpoint >= opt.CheckpointStep || offset >= size {
				if err := dev.Sync(); err != nil {
					return fmt.Errorf("fsync pass %d: %w", spec.Index, err)
				}
				if onProgress != nil {
					if err := onProgress(Progress{Pass: spec.Index, Offset: offset, Total: size, Phase: "overwrite"}); err != nil {
						return err
					}
				}
				lastCheckpoint = offset
			}
		}
		if err := dev.Sync(); err != nil {
			return fmt.Errorf("fsync end of pass %d: %w", spec.Index, err)
		}
	}
	return nil
}

func verify(ctx context.Context, dev Device, size int64, opt Options, onProgress ProgressFn) (*domain.VerifyResult, error) {
	final := opt.Standard.Passes[len(opt.Standard.Passes)-1]
	readBuf := make([]byte, opt.ChunkSize)
	wantBuf := make([]byte, opt.ChunkSize)

	ranges := []span{{start: 0, end: size}}
	res := &domain.VerifyResult{Mode: opt.Standard.VerifyMode}
	if opt.Standard.VerifyMode == "sample" {
		ranges = sampleRanges(size, opt.SampleFraction, opt.JobID)
		res.Samples = make([]domain.VerifySamplePoint, 0, len(ranges))
	}

	var sinceReport int64
	for _, rn := range ranges {
		offset := rn.start
		for offset < rn.end {
			if err := ctx.Err(); err != nil {
				return res, err
			}
			n := int64(len(readBuf))
			if offset+n > rn.end {
				n = rn.end - offset
			}
			if _, err := dev.ReadAt(readBuf[:n], offset); err != nil && err != io.EOF {
				return res, fmt.Errorf("read verify offset %d: %w", offset, err)
			}
			if err := streamAt(string(final.Pattern), opt.JobID, final.Index, wantBuf[:n], offset); err != nil {
				return res, err
			}
			if !bytes.Equal(readBuf[:n], wantBuf[:n]) {
				res.Mismatches += countMismatches(readBuf[:n], wantBuf[:n])
			}
			res.Bytes += n
			sinceReport += n
			offset += n
			if onProgress != nil && (sinceReport >= opt.CheckpointStep*4 || offset >= rn.end) {
				sinceReport = 0
				if err := onProgress(Progress{Pass: final.Index, Offset: offset, Total: size, Phase: "verify"}); err != nil {
					return res, err
				}
			}
		}
		if opt.Standard.VerifyMode == "sample" {
			res.Samples = append(res.Samples, domain.VerifySamplePoint{Offset: rn.start, Length: rn.end - rn.start})
		}
	}
	return res, nil
}

type span struct{ start, end int64 }

// sampleRanges picks up to ~fraction*capacity in fixed 4 MiB windows spread
// pseudorandomly but deterministically over the disk (seeded from job id),
// always including head and tail.
func sampleRanges(size int64, fraction float64, jobID string) []span {
	if fraction <= 0 {
		fraction = 0.01
	}
	const window = 4 << 20
	target := int64(float64(size) * fraction)
	if target < window {
		target = window
	}
	if target > size {
		target = size
	}
	var seed int64
	for _, c := range []byte("verify-sample") {
		seed = seed*131 + int64(c)
	}
	// job id makes the sample layout per-disk unique but reproducible on re-verify
	for i := 0; i < len(jobID) && i < 16; i++ {
		seed = seed*131 + int64(jobID[i])
	}
	rng := rand.New(rand.NewSource(seed))
	spans := []span{
		{0, min64(window, size)},
		{max64(0, size-window), size},
	}
	covered := spans[0].end - spans[0].start
	if size > window {
		covered += spans[1].end - spans[1].start
	}
	for covered < target {
		start := rng.Int63n(max64(1, size-window))
		end := start + window
		if end > size {
			end = size
		}
		spans = append(spans, span{start, end})
		covered += end - start
		if len(spans) > 4096 {
			break
		}
	}
	return spans
}

func countMismatches(a, b []byte) int64 {
	var n int64
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
