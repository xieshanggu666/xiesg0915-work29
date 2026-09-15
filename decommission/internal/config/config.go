// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr      string
	DatabaseURL   string // postgres://... ; empty => in-memory demo store
	MinIOEndpoint string
	MinIOKey      string
	MinIOSecret   string
	MinIOBucket   string
	MinIORegion   string
	MinIOUseTLS   bool
	// ReportDir: when MinIO is not configured, reports are written here
	// and served through the same object-store interface (filesystem mode).
	ReportDir string

	Workers          int    // 并发擦除的设备(磁盘)数
	ChunkBytes       int    // 单次写入/校验块大小
	CheckpointEvery  int64  // 每写 N 字节落一次断点
	StaleAfterSec    int    // 超过该秒数无心跳的 running 任务视为断电残留
	DevicePathPrefix string // 允许打开的设备/文件路径前缀，逗号分隔；安全护栏
}

// MinIOBucketOr returns the configured bucket name or the given default.
func (c Config) MinIOBucketOr(def string) string {
	if c.MinIOBucket == "" {
		return def
	}
	return c.MinIOBucket
}

func Load() Config {
	c := Config{
		HTTPAddr:         getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		MinIOEndpoint:    os.Getenv("MINIO_ENDPOINT"),
		MinIOKey:         os.Getenv("MINIO_ACCESS_KEY"),
		MinIOSecret:      os.Getenv("MINIO_SECRET_KEY"),
		MinIOBucket:      getenv("MINIO_BUCKET", "erasure-reports"),
		MinIORegion:      getenv("MINIO_REGION", "us-east-1"),
		MinIOUseTLS:      getenv("MINIO_USE_TLS", "false") == "true",
		ReportDir:        getenv("REPORT_DIR", "./reports"),
		Workers:          getenvInt("WORKERS", 4),
		ChunkBytes:       getenvInt("CHUNK_BYTES", 4*1024*1024),
		CheckpointEvery:  int64(getenvInt("CHECKPOINT_EVERY_BYTES", 64*1024*1024)),
		StaleAfterSec:    getenvInt("STALE_AFTER_SEC", 120),
		DevicePathPrefix: getenv("DEVICE_PATH_PREFIX", "/dev/"),
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.ChunkBytes < 4096 {
		c.ChunkBytes = 4096
	}
	return c
}

// AllowedDevicePath checks that the wipe target lives under an explicitly
// allowed prefix. This prevents an operator from wiping e.g. the OS disk by
// accident: prefixes default to /dev/ but demo files live under DEMO_DISK_DIR.
func (c Config) AllowedDevicePath(p string) error {
	for _, pref := range strings.Split(c.DevicePathPrefix, ",") {
		pref = strings.TrimSpace(pref)
		if pref != "" && strings.HasPrefix(p, pref) {
			return nil
		}
	}
	return fmt.Errorf("device path %q is not under allowed prefixes [%s]", p, c.DevicePathPrefix)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
