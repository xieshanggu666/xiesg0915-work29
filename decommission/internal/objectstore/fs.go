package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"idc/decommission/internal/domain"
)

// Filesystem stores objects under root/bucket/key. Useful when MinIO is not
// deployed; the report-download endpoint streams files straight back.
type Filesystem struct {
	root   string
	bucket string
}

func NewFilesystem(root, bucket string) *Filesystem {
	return &Filesystem{root: root, bucket: bucket}
}

func (fs *Filesystem) dir() string { return filepath.Join(fs.root, safe(fs.bucket)) }

func (fs *Filesystem) EnsureBucket(context.Context) error {
	return os.MkdirAll(fs.dir(), 0o750)
}

func (fs *Filesystem) Backend() string { return "filesystem" }

func (fs *Filesystem) path(key string) (string, error) {
	clean := filepath.Clean("/" + key) // prevent ../ escape
	full := filepath.Join(fs.dir(), clean)
	if !strings.HasPrefix(full, fs.dir()) {
		return "", fmt.Errorf("invalid key %q", key)
	}
	return full, nil
}

func (fs *Filesystem) Put(ctx context.Context, key, contentType string, r io.Reader) (Object, error) {
	full, err := fs.path(key)
	if err != nil {
		return Object{}, err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		return Object{}, err
	}
	tmp := full + ".tmp." + domain.UniqueSuffix()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return Object{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return Object{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return Object{}, err
	}
	f.Close()
	if err := os.Rename(tmp, full); err != nil {
		return Object{}, err
	}
	return Object{
		Key:         key,
		Size:        n,
		SHA256:      hex.EncodeToString(h.Sum(nil)),
		ContentType: contentType,
	}, nil
}

func (fs *Filesystem) Get(ctx context.Context, key string) (io.ReadCloser, Object, error) {
	full, err := fs.path(key)
	if err != nil {
		return nil, Object{}, err
	}
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, Object{}, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return nil, Object{}, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, Object{}, err
	}
	return f, Object{Key: key, Size: st.Size()}, nil
}

func safe(s string) string {
	return strings.NewReplacer("/", "_", "..", "_").Replace(s)
}
