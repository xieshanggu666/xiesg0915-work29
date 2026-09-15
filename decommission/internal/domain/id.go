package domain

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a 128-bit random hex string.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// UniqueSuffix returns a short random hex suffix for temp file names.
func UniqueSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
