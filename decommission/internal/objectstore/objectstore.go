// Package objectstore stores generated reports and certificates. Two backends are
// provided behind one interface:
//
//   - S3/MinIO backend using an AWS Signature V4 client implemented directly
//     on net/http (no SDK dependency).
//   - filesystem backend for local development and tests.
package objectstore

import (
	"context"
	"fmt"
	"io"
)

type Object struct {
	Key          string
	Size         int64
	SHA256       string
	ContentType  string
	LastModified string
}

type Store interface {
	// Put uploads data and returns the object key + sha256 hex digest.
	Put(ctx context.Context, key, contentType string, r io.Reader) (Object, error)
	// Get downloads an object.
	Get(ctx context.Context, key string) (io.ReadCloser, Object, error)
	// EnsureBucket creates the bucket if missing (best effort on FS backend).
	EnsureBucket(ctx context.Context) error
	// Backend reports "minio" or "filesystem" for reporting.
	Backend() string
}

// New picks the backend from configuration. If endpoint is empty the FS backend
// rooted at dir is used.
func New(endpoint, key, secret, bucket, region string, useTLS bool, dir string) (Store, error) {
	if endpoint == "" {
		return NewFilesystem(dir, bucket), nil
	}
	s, err := newS3(endpoint, key, secret, bucket, region, useTLS)
	if err != nil {
		return nil, fmt.Errorf("init minio backend: %w", err)
	}
	return s, nil
}
