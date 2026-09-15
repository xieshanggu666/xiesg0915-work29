package objectstore

import "errors"

// ErrObjectNotFound mirrors the S3 NoSuchKey error.
var ErrObjectNotFound = errors.New("object not found")
