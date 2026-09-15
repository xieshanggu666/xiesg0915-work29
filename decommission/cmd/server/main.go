// Command decommission-server runs the erasure management API and worker pool.
//
// Without DATABASE_URL it uses an in-memory store (demo/evaluation mode) and
// stores reports on the local filesystem; with DATABASE_URL it uses PostgreSQL
// and, when MINIO_ENDPOINT is set, MinIO/S3 for reports.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"idc/decommission/internal/api"
	"idc/decommission/internal/config"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/service"
	"idc/decommission/internal/store"
	"idc/decommission/internal/worker"
)

func main() {
	logger := log.New(os.Stdout, "decommission: ", log.LstdFlags|log.Lmsgprefix)
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var st store.Store
	if cfg.DatabaseURL != "" {
		pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			logger.Fatalf("connect postgres: %v", err)
		}
		if err := pg.Ping(ctx); err != nil {
			logger.Fatalf("ping postgres: %v (run migrations/0001_init.sql first)", err)
		}
		logger.Print("store: postgres")
		st = pg
	} else {
		logger.Print("store: in-memory (set DATABASE_URL for postgres)")
		st = store.NewMemory()
	}

	obj, err := objectstore.New(cfg.MinIOEndpoint, cfg.MinIOKey, cfg.MinIOSecret,
		cfg.MinIOBucket, cfg.MinIORegion, cfg.MinIOUseTLS, cfg.ReportDir)
	if err != nil {
		logger.Fatalf("object store: %v", err)
	}
	if err := obj.EnsureBucket(ctx); err != nil {
		logger.Fatalf("ensure bucket: %v", err)
	}
	logger.Printf("object store: %s (bucket=%s)", obj.Backend(), cfg.MinIOBucket)

	// Demo mode: fabricate a pulled device with two disk images so the full
	// pipeline can be observed without real hardware.
	if os.Getenv("DEMO") == "true" {
		if mem, ok := st.(*store.Memory); ok {
			demoDir := filepath.Join(cfg.ReportDir, "..", "demo-disks")
			_ = os.MkdirAll(demoDir, 0o750)
			// make demo disk images eligible as wipe targets
			cfg.DevicePathPrefix = demoDir + "," + cfg.DevicePathPrefix
			if _, err := mem.SeedDemoDisks(ctx, demoDir, "demo-operator"); err != nil {
				logger.Fatalf("seed demo: %v", err)
			}
			logger.Printf("demo data ready under %s", demoDir)
		}
	}

	wk := worker.New("worker-1", cfg, st, obj, logger)
	wk.Start(ctx)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(service.New(st), st, obj, wk, logger).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		logger.Printf("listening on %s (workers=%d, chunk=%d, checkpoint=%d)",
			cfg.HTTPAddr, cfg.Workers, cfg.ChunkBytes, cfg.CheckpointEvery)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Print("shutting down: in-flight jobs stop at the next checkpoint and will resume on restart")
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	wk.Stop()
	logger.Print("stopped")
}
