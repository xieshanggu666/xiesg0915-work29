// Package erasure implements the overwrite engine: multi-pass block writing,
// byte-level verification, and power-loss safe checkpoints.
//
// Resume design
//
//	Every chunk written is identified by (passIndex, offset). Random passes
//	use a deterministic keystream (ChaCha8 keyed by jobID+pass), so the
//	bytes expected at any offset after a crash are reproducible without buffering
//	the whole pass. Checkpoints are persisted every N bytes; on startup (or
//	after a crash) the engine seeks to the saved offset and continues the same
//	pass. Completed passes are never rewritten.
package erasure

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/chacha20"
)

// Device is a seekable, writable/readable raw target. Production uses block
// devices (os.File on /dev/sdX); tests and demos use regular files.
type Device interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.Closer
	Size() (int64, error) // addressable byte count
	Sync() error          // fsync to stable storage
}

// FileDevice wraps *os.File (block device or regular file).
type FileDevice struct{ f *os.File }

// OpenDevice opens a block device / disk image. Callers must pre-validate
// the path against the allowed device prefixes.
func OpenDevice(path string) (*FileDevice, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open device %s: %w", path, err)
	}
	return &FileDevice{f: f}, nil
}

func (d *FileDevice) ReadAt(p []byte, off int64) (int, error)   { return d.f.ReadAt(p, off) }
func (d *FileDevice) WriteAt(p []byte, off int64) (int, error)  { return d.f.WriteAt(p, off) }
func (d *FileDevice) Seek(off int64, whence int) (int64, error) { return d.f.Seek(off, whence) }
func (d *FileDevice) Close() error                              { return d.f.Close() }
func (d *FileDevice) Sync() error                               { return d.f.Sync() }

func (d *FileDevice) Size() (int64, error) {
	pos, err := d.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := d.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := d.f.Seek(pos, io.SeekStart); err != nil {
		return 0, err
	}
	return end, nil
}

// streamAt fills buf with the bytes the pass expects at absolute offset.
// zeros/ones are trivial; random derives a keystream keyed by (jobID, passIndex),
// so resume after power loss can re-derive any offset independently.
func streamAt(pattern, jobID string, passIndex int, buf []byte, offset int64) error {
	switch pattern {
	case "zeros":
		for i := range buf {
			buf[i] = 0x00
		}
		return nil
	case "ones":
		for i := range buf {
			buf[i] = 0xFF
		}
		return nil
	case "random":
		var key [32]byte
		copy(key[:16], pad16(jobID))
		binary.BigEndian.PutUint64(key[16:24], uint64(passIndex))
		copy(key[24:], []byte("IDC-ERASURE-V1!!"))
		c, err := chacha20.NewUnauthenticatedCipher(key[:], make([]byte, chacha20.NonceSize))
		if err != nil {
			return err
		}
		// ChaCha keystream is 64-byte blocks; align to the absolute offset.
		block := offset / 64
		c.SetCounter(uint32(block))
		var blockBuf [64]byte
		skip := int(offset % 64)
		pos := 0
		if skip > 0 {
			c.XORKeyStream(blockBuf[:], blockBuf[:])
			pos = copy(buf, blockBuf[skip:])
		}
		for pos < len(buf) {
			c.XORKeyStream(blockBuf[:], blockBuf[:])
			pos += copy(buf[pos:], blockBuf[:])
		}
		return nil
	default:
		return fmt.Errorf("unknown pattern %q", pattern)
	}
}

// pad16 folds the job id into a deterministic 16-byte key prefix.
func pad16(s string) []byte {
	out := make([]byte, 16)
	copy(out, []byte(s))
	if len(s) > 16 {
		for i := 16; i < len(s); i++ {
			out[i%16] ^= s[i]
		}
	}
	return out
}
