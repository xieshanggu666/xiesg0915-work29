package erasure

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"idc/decommission/internal/domain"
)

// newImage creates a temp file filled with pseudo-random "old data".
func newImage(t *testing.T, size int64) (*FileDevice, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "disk.img")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1<<20)
	var off int64
	for off < size {
		n := int64(len(buf))
		if off+n > size {
			n = size - off
		}
		if _, err := rand.Read(buf[:n]); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(buf[:n], off); err != nil {
			t.Fatal(err)
		}
		off += n
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := OpenDevice(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dev.Close() })
	return dev, path
}

func zerosStandard() domain.Standard {
	return domain.Standard{
		Code: "t-zero", Passes: []domain.PassSpec{{Index: 0, Pattern: domain.PatternZeros}},
		VerifyMode: "full",
	}
}

func TestOverwriteAndVerify_FullZero(t *testing.T) {
	const size = 32 << 20
	dev, _ := newImage(t, size)
	res, err := Run(context.Background(), dev, Options{
		Standard: zerosStandard(), JobID: "job-aaaa",
		ChunkSize: 1 << 20, CheckpointStep: 4 << 20,
	}, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Mismatches != 0 {
		t.Fatalf("mismatches = %d, want 0", res.Mismatches)
	}
	if res.Bytes != size {
		t.Fatalf("verified bytes = %d, want %d", res.Bytes, size)
	}
	// independent readback check
	got := make([]byte, size)
	if _, err := dev.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, size)) {
		t.Fatal("disk not fully zeroed")
	}
}

func TestThreePassStandardVerifiesFinalPattern(t *testing.T) {
	std, _ := domain.GetStandard("nist_purge") // final pass = zeros, full verify
	const size = 16 << 20
	dev, _ := newImage(t, size)
	res, err := Run(context.Background(), dev, Options{
		Standard: std, JobID: "job-bbbb",
		ChunkSize: 1 << 20, CheckpointStep: 2 << 20,
	}, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Mismatches != 0 {
		t.Fatalf("mismatches = %d", res.Mismatches)
	}
	got := make([]byte, 4096)
	if _, err := dev.ReadAt(got, 10<<20); err != nil {
		t.Fatal(err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("final pass byte %d = %#x, want 0x00", i, b)
		}
	}
}

// TestResumeAfterCrash simulates power loss: cancel mid overwrite, then resume
// from the checkpoint and verify the whole disk. The region written before the
// crash must already contain the correct pattern, and resume must complete it.
func TestResumeAfterCrash(t *testing.T) {
	const size = 40 << 20
	dev, path := newImage(t, size)

	ctx, cancel := context.WithCancel(context.Background())
	var crashedAt atomic.Int64
	var crashedPass int
	var calls atomic.Int32
	onProgress := func(p Progress) error {
		calls.Add(1)
		if p.Phase == "overwrite" && p.Offset >= 20<<20 {
			crashedAt.Store(p.Offset)
			crashedPass = p.Pass
			cancel() // "power loss" right after this durable checkpoint
		}
		return nil
	}
	_, err := Run(ctx, dev, Options{
		Standard: zerosStandard(), JobID: "job-crash",
		ChunkSize: 1 << 20, CheckpointStep: 1 << 20,
	}, onProgress)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	dev.Close()
	if crashedAt.Load() == 0 {
		t.Fatal("never reached crash point")
	}

	// reopen (as a rebooted process would) and resume from the checkpoint
	dev2, err := OpenDevice(path)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), dev2, Options{
		Standard: zerosStandard(), JobID: "job-crash",
		ChunkSize: 1 << 20, CheckpointStep: 4 << 20,
		ResumePass: crashedPass, ResumeOffset: crashedAt.Load(),
	}, nil)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if res.Mismatches != 0 {
		t.Fatalf("post-resume mismatches = %d", res.Mismatches)
	}
	if res.Bytes != size {
		t.Fatalf("verified bytes = %d, want %d", res.Bytes, size)
	}
}

// TestResumeWithMultiPass: crash during pass 2 of a 3-pass standard; earlier
// passes must stay intact because resume restarts the *current* pass, not from
// zero. Final verification still passes (final pass overwrites everything).
func TestResumeMidMultiPass(t *testing.T) {
	std, _ := domain.GetStandard("dod_3pass") // zero, ones, random -> final random
	const size = 24 << 20
	dev, path := newImage(t, size)

	ctx, cancel := context.WithCancel(context.Background())
	var crashedAt atomic.Int64
	var crashedPass atomic.Int32
	onProgress := func(p Progress) error {
		if p.Phase == "overwrite" && p.Pass == 1 && p.Offset >= 12<<20 {
			crashedAt.Store(p.Offset)
			crashedPass.Store(int32(p.Pass))
			cancel()
		}
		return nil
	}
	if _, err := Run(ctx, dev, Options{
		Standard: std, JobID: "job-multi", ChunkSize: 1 << 20, CheckpointStep: 1 << 20,
	}, onProgress); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	dev.Close()

	dev2, err := OpenDevice(path)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), dev2, Options{
		Standard: std, JobID: "job-multi",
		ChunkSize: 1 << 20, CheckpointStep: 4 << 20,
		ResumePass: int(crashedPass.Load()), ResumeOffset: crashedAt.Load(),
	}, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Mismatches != 0 {
		t.Fatalf("mismatches after resume = %d", res.Mismatches)
	}
}

// TestDetectsTampering proves verification catches a non-conforming byte.
func TestDetectsTampering(t *testing.T) {
	const size = 8 << 20
	dev, path := newImage(t, size)
	if _, err := Run(context.Background(), dev, Options{
		Standard: zerosStandard(), JobID: "job-tamper",
		ChunkSize: 1 << 20, CheckpointStep: 4 << 20,
	}, nil); err != nil {
		t.Fatal(err)
	}
	dev.Close()

	// physically poke residual data into the "erased" disk
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xAB, 0xCD}, 5<<20); err != nil {
		t.Fatal(err)
	}
	f.Close()

	dev2, err := OpenDevice(path)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Verify(context.Background(), dev2, Options{
		Standard: zerosStandard(), JobID: "job-tamper",
		ChunkSize: 1 << 20, CheckpointStep: 4 << 20,
	}, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Mismatches < 2 {
		t.Fatalf("expected mismatches, got %d", res.Mismatches)
	}
}

// TestRandomPatternIsReproducible: the random keystream at a given offset must
// be stable across cipher instances (this is what makes random passes resumable).
func TestRandomPatternIsReproducible(t *testing.T) {
	a := make([]byte, 1024)
	b := make([]byte, 1024)
	if err := streamAt("random", "job-x", 2, a, 1234567); err != nil {
		t.Fatal(err)
	}
	if err := streamAt("random", "job-x", 2, b, 1234567); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("random stream not reproducible at same offset")
	}
	c := make([]byte, 1024)
	if err := streamAt("random", "job-x", 2, c, 1234568); err != nil {
		t.Fatal(err)
	}
	// adjacent offsets overlap by len-1 bytes: verify shift consistency
	if !bytes.Equal(a[1:], c[:1023]) {
		t.Fatal("keystream not offset-consistent")
	}
}

func TestSampleVerification(t *testing.T) {
	std, _ := domain.GetStandard("sample_quick")
	const size = 64 << 20
	dev, _ := newImage(t, size)
	res, err := Run(context.Background(), dev, Options{
		Standard: std, JobID: "job-sample",
		ChunkSize: 1 << 20, CheckpointStep: 16 << 20, SampleFraction: 0.1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "sample" || res.Bytes >= size {
		t.Fatalf("sample verify should cover less than full disk, mode=%s bytes=%d", res.Mode, res.Bytes)
	}
	if len(res.Samples) < 2 {
		t.Fatalf("expected head+tail samples, got %d", len(res.Samples))
	}
	if res.Mismatches != 0 {
		t.Fatalf("mismatches = %d", res.Mismatches)
	}
}
