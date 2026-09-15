package objectstore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// fakeS3 emulates just enough of path-style S3 to verify the SigV4 client:
// it recomputes the signature from the request and rejects mismatches, and
// stores objects in memory for a Put/Get round trip.
type fakeS3 struct {
	t       *testing.T
	key     string
	secret  string
	region  string
	bucket  string
	objects map[string][]byte
}

func (f *fakeS3) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !f.verify(r) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("<Error><Code>SignatureDoesNotMatch</Code></Error>"))
			return
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		isBucketReq := len(parts) == 1 || (len(parts) == 2 && parts[1] == "")
		switch {
		case r.Method == http.MethodHead && isBucketReq:
			if _, ok := f.objects["__bucket__"]; ok {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodPut && isBucketReq:
			f.objects["__bucket__"] = []byte{1}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			f.objects[parts[1]] = body
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			body, ok := f.objects[parts[1]]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

// verify recomputes the AWS SigV4 signature exactly as S3 would.
func (f *fakeS3) verify(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	var signedHeaders, cred, sig string
	for _, part := range strings.Split(strings.TrimPrefix(strings.SplitN(auth, " ", 2)[1], ""), ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			cred = strings.TrimPrefix(part, "Credential=")
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			sig = strings.TrimPrefix(part, "Signature=")
		}
	}
	amzDate := r.Header.Get("X-Amz-Date")
	dateStamp := amzDate[:8]
	contentSHA := r.Header.Get("X-Amz-Content-Sha256")
	scope := dateStamp + "/" + f.region + "/s3/aws4_request"
	if !strings.HasPrefix(cred, f.key+"/"+scope) {
		f.t.Logf("bad credential scope: %s", cred)
		return false
	}
	var ch strings.Builder
	hs := strings.Split(signedHeaders, ";")
	sort.Strings(hs)
	for _, h := range hs {
		ch.WriteString(h)
		ch.WriteString(":")
		val := r.Header.Get(http.CanonicalHeaderKey(h))
		if h == "host" {
			val = r.Host
		}
		ch.WriteString(strings.TrimSpace(val))
		ch.WriteString("\n")
	}
	canonReq := strings.Join([]string{r.Method, r.URL.EscapedPath(), "", ch.String(), signedHeaders, contentSHA}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, shaHex([]byte(canonReq)),
	}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+f.secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(f.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	want := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
	if !hmac.Equal([]byte(want), []byte(sig)) {
		f.t.Logf("signature mismatch:\nwant %s\ngot  %s", want, sig)
		return false
	}
	return true
}

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func newFakeS3(t *testing.T) (*httptest.Server, *S3) {
	t.Helper()
	f := &fakeS3{
		t: t, key: "testkey", secret: "testsecret", region: "us-east-1",
		bucket: "bkt", objects: map[string][]byte{},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := newS3(strings.TrimPrefix(srv.URL, "http://"), f.key, f.secret, f.bucket, f.region, false)
	if err != nil {
		t.Fatal(err)
	}
	return srv, c
}

func TestS3PutGetRoundTrip(t *testing.T) {
	_, c := newFakeS3(t)
	ctx := context.Background()
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	// second ensure must be idempotent (bucket exists -> 200)
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("idempotent ensure: %v", err)
	}
	payload := bytes.Repeat([]byte("erasure-report-"), 100)
	obj, err := c.Put(ctx, "reports/CERT-1.json", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Fatalf("size = %d", obj.Size)
	}
	if len(obj.SHA256) != 64 {
		t.Fatalf("sha = %q", obj.SHA256)
	}
	rc, got, err := c.Get(ctx, "reports/CERT-1.json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if !bytes.Equal(body, payload) {
		t.Fatal("payload mismatch")
	}
	if got.Size != int64(len(payload)) {
		t.Fatalf("get size = %d", got.Size)
	}
	if _, _, err := c.Get(ctx, "missing"); err == nil {
		t.Fatal("expected not found")
	}
}

func TestS3RejectsBadCredentials(t *testing.T) {
	srv, _ := newFakeS3(t)
	bad, err := newS3(strings.TrimPrefix(srv.URL, "http://"), "testkey", "WRONGSECRET", "bkt", "us-east-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Put(context.Background(), "k", "text/plain", strings.NewReader("x")); err == nil {
		t.Fatal("expected signature rejection")
	}
}

func TestFilesystemPutGet(t *testing.T) {
	dir := t.TempDir()
	fs := NewFilesystem(dir, "bucket")
	ctx := context.Background()
	if err := fs.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	obj, err := fs.Put(ctx, "reports/a/b.json", "application/json", strings.NewReader(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if obj.SHA256 != shaHex([]byte(`{"ok":true}`)) {
		t.Fatalf("sha = %s", obj.SHA256)
	}
	rc, _, err := fs.Get(ctx, "reports/a/b.json")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != `{"ok":true}` {
		t.Fatalf("body = %s", b)
	}
	// path traversal is neutralized by the clean-based sandbox: "../" keys
	// resolve inside the bucket directory instead of escaping it
	if _, err := fs.Put(ctx, "../../../../tmp/escape.txt", "text/plain", strings.NewReader("x")); err != nil {
		t.Fatalf("sandboxed put: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bucket", "tmp", "escape.txt")); err != nil {
		t.Fatalf("escaped or misplaced object: %v", err)
	}
}
