package objectstore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Only PutObject/GetObject/HeadBucket/CreateBucket are needed here.
// S3 is a minimal AWS Signature V4 S3 client, sufficient for MinIO.
// Only PutObject/GetObject/HeadBucket/CreateBucket are needed here.
type S3 struct {
	endpoint string
	key      string
	secret   string
	bucket   string
	region   string
	useTLS   bool
	hc       *http.Client
}

func newS3(endpoint, key, secret, bucket, region string, useTLS bool) (*S3, error) {
	endpoint = strings.TrimRight(endpoint, "/")
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		scheme := "http"
		if useTLS {
			scheme = "https"
		}
		endpoint = scheme + "://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	return &S3{
		endpoint: strings.TrimSuffix(u.String(), "/"),
		key:      key, secret: secret, bucket: bucket, region: region,
		useTLS: u.Scheme == "https",
		hc:     &http.Client{Timeout: 10 * time.Minute},
	}, nil
}

func (s *S3) Backend() string { return "minio" }

func (s *S3) url(key string) string {
	return fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, key)
}

func (s *S3) EnsureBucket(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, s.endpoint+"/"+s.bucket, nil)
	if err != nil {
		return err
	}
	s.sign(req, "", nil)
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("head bucket: unexpected status %d", resp.StatusCode)
	}
	// create bucket (path style; MinIO default)
	body := []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>%s</LocationConstraint></CreateBucketConfiguration>`, s.region))
	req, err = http.NewRequestWithContext(ctx, http.MethodPut, s.endpoint+"/"+s.bucket, bytes.NewReader(body))
	if err != nil {
		return err
	}
	s.sign(req, "", bytes.NewReader(body))
	resp, err = s.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("create bucket: status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (s *S3) Put(ctx context.Context, key, contentType string, r io.Reader) (Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Object{}, err
	}
	sum := sha256.Sum256(data)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.url(escapeKey(key)), bytes.NewReader(data))
	if err != nil {
		return Object{}, err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	sumHex := hex.EncodeToString(sum[:])
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = int64(len(data))
	s.sign(req, sumHex, bytes.NewReader(data))
	resp, err := s.hc.Do(req)
	if err != nil {
		return Object{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return Object{}, fmt.Errorf("put %s: status %d: %s", key, resp.StatusCode, b)
	}
	return Object{Key: key, Size: int64(len(data)), SHA256: sumHex, ContentType: contentType}, nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url(escapeKey(key)), nil)
	if err != nil {
		return nil, Object{}, err
	}
	s.sign(req, "", nil)
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, Object{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, Object{}, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, Object{}, fmt.Errorf("get %s: status %d: %s", key, resp.StatusCode, b)
	}
	return resp.Body, Object{Key: key, Size: resp.ContentLength, ContentType: resp.Header.Get("Content-Type")}, nil
}

// ---- AWS Sig V4 ----

func (s *S3) sign(req *http.Request, contentSHA string, payload io.Reader) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	if contentSHA == "" {
		contentSHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	req.Header.Set("X-Amz-Content-Sha256", contentSHA)
	if req.Header.Get("Content-Type") == "" && payload != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	u := req.URL
	canonicalURI := "/" + s.bucket + strings.TrimPrefix(u.Path, "/"+s.bucket)
	canonicalURI = (&url.URL{Path: canonicalURI}).EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	headersToSign := []string{"content-type", "host", "x-amz-content-sha256", "x-amz-date"}
	headerVals := map[string]string{
		"content-type":         req.Header.Get("Content-Type"),
		"host":                 u.Host,
		"x-amz-content-sha256": contentSHA,
		"x-amz-date":           amzDate,
	}
	sort.Strings(headersToSign)
	var signedHeaders, canonHeaders strings.Builder
	for _, h := range headersToSign {
		signedHeaders.WriteString(h)
		signedHeaders.WriteString(";")
		canonHeaders.WriteString(h)
		canonHeaders.WriteString(":")
		canonHeaders.WriteString(strings.TrimSpace(headerVals[h]))
		canonHeaders.WriteString("\n")
	}
	sh := strings.TrimSuffix(signedHeaders.String(), ";")
	canonicalReq := strings.Join([]string{
		req.Method,
		canonicalURI,
		"", // query
		canonHeaders.String(),
		sh,
		contentSHA,
	}, "\n")
	scope := dateStamp + "/" + s.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalReq)),
	}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+s.secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(s.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		url.PathEscape(s.key), scope, sh, signature))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// escapeKey URL-escapes an object key without escaping slashes.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}
