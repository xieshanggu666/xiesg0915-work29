// Command decommission-cli is a small operator CLI over the same service layer.
// It also embeds "migrate" to apply migrations/0001_init.sql to PostgreSQL.
//
// Examples (demo mode, no postgres/minio needed):
//
//	decommission-cli migrate --database-url $DATABASE_URL
//	decommission-cli demo --wipe --standard nist_purge
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"idc/decommission/internal/config"
	"idc/decommission/internal/domain"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/service"
	"idc/decommission/internal/store"
	"idc/decommission/internal/worker"
)

var _ = config.Config{}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	logger := log.New(os.Stderr, "cli: ", log.LstdFlags)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "migrate":
		runMigrate(ctx, os.Args[2:])
	case "demo":
		runDemo(ctx, os.Args[2:], logger)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: decommission-cli <command> [flags]

commands:
  migrate   apply migrations/0001_init.sql to PostgreSQL
  demo      run an end-to-end flow against in-memory store + local report dir
            --wipe                 actually wipe the demo disk images
            --standard <code>      nist_clear|nist_purge|dod_3pass|sample_quick
            --crash-at-mb <n>      simulate power loss after n MiB, then restart & resume
            --workers <n>          concurrency (default 2)
            --dir <path>           workspace for disk images & reports (default ./.demo)`)
}

func runMigrate(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dbURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "postgres DSN")
	file := fs.String("file", "migrations/0001_init.sql", "migration SQL file")
	_ = fs.Parse(args)
	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "--database-url or DATABASE_URL required")
		os.Exit(2)
	}
	pg, err := store.NewPostgres(ctx, *dbURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := store.ApplyMigrationFile(ctx, pg, *file); err != nil {
		fmt.Fprintln(os.Stderr, "migration failed:", err)
		os.Exit(1)
	}
	fmt.Println("migration applied:", *file)
}

func runDemo(ctx context.Context, args []string, logger *log.Logger) {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	wipe := fs.Bool("wipe", false, "actually queue+run erasure jobs")
	standard := fs.String("standard", "nist_clear", "erasure standard")
	crashAt := fs.Int("crash-at-mb", 0, "simulate power loss after N MiB of pass 0, then restart and resume")
	workers := fs.Int("workers", 2, "worker concurrency")
	dir := fs.String("dir", "./.demo", "workspace")
	_ = fs.Parse(args)

	cfg := config.Load()
	cfg.Workers = *workers
	cfg.ChunkBytes = 1 << 20
	cfg.CheckpointEvery = 4 << 20
	cfg.StaleAfterSec = 5
	cfg.ReportDir = filepath.Join(*dir, "reports")
	cfg.MinIOBucket = "erasure-reports"
	diskDir := filepath.Join(*dir, "disks")
	_ = os.MkdirAll(diskDir, 0o750)
	cfg.DevicePathPrefix = diskDir + ","

	st := store.NewMemory()
	obj, err := objectstore.New("", "", "", cfg.MinIOBucket, "", false, cfg.ReportDir)
	if err != nil {
		logger.Fatal(err)
	}
	_ = obj.EnsureBucket(ctx)
	if _, err := domain.GetStandard(*standard); err != nil {
		logger.Fatal(err)
	}
	assetID, err := st.SeedDemoDisks(ctx, diskDir, "demo-operator", *standard)
	if err != nil {
		logger.Fatal(err)
	}
	a, _ := st.GetAsset(ctx, assetID)
	disks, _ := st.ListDisks(ctx, a.ID)
	dump("asset", a)
	for _, d := range disks {
		dump("disk", map[string]any{"id": d.ID, "serial": d.Serial, "path": d.DevicePath, "status": d.Status})
	}
	if !*wipe {
		fmt.Println("seeded; re-run with --wipe to execute erasure")
		return
	}

	wk := worker.New("cli-worker", cfg, st, obj, logger)

	if *crashAt > 0 {
		// first process lifetime: start, queue jobs, kill after crash point
		runWithCrash(ctx, wk, st, disks, *standard, *crashAt, logger)
		// second process lifetime: brand-new worker instance resumes the
		// already-existing running jobs from their checkpoints (no new jobs)
		fmt.Println(">>> simulated process restart; stale job will be reclaimed and resumed")
		time.Sleep(200 * time.Millisecond)
		wk2 := worker.New("cli-worker-restarted", cfg, st, obj, logger)
		runAndWait(ctx, wk2, st, disks, false, *standard, 45*time.Second, logger)
	} else {
		runAndWait(ctx, wk, st, disks, true, *standard, 45*time.Second, logger)
	}

	certs, _ := st.ListCertificates(ctx, a.ID)
	dump("certificates", certs)
	aa, _ := st.GetAsset(ctx, a.ID)
	fmt.Println("asset final status:", aa.Status)
	logs, _, _ := st.ListAudit(ctx, store.AuditFilter{EntityType: "asset", EntityID: a.ID, Limit: 50})
	fmt.Println("---- asset audit trail ----")
	for i := len(logs) - 1; i >= 0; i-- {
		l := logs[i]
		fmt.Printf("%s  %-12s  %s  %s\n", l.Time.Format(time.RFC3339), l.Actor, l.Action, l.Detail)
	}
}

func runAndWait(ctx context.Context, wk *worker.Worker, st store.Store, disks []domain.Disk, createJobs bool, standard string, timeout time.Duration, logger *log.Logger) {
	wk.Start(ctx)
	if createJobs {
		svc := service.New(st)
		for _, d := range disks {
			if _, err := svc.CreateErasureJob(ctx, d.ID, standard, "demo-operator"); err != nil {
				logger.Fatal(err)
			}
		}
	}
	waitVerified(st, disks, timeout)
	wk.Stop()
}

func runWithCrash(ctx context.Context, wk *worker.Worker, st store.Store, disks []domain.Disk, standard string, crashAt int, logger *log.Logger) {
	fmt.Printf(">>> first lifetime: queuing %d jobs with standard %s, crash after %d MiB\n", len(disks), standard, crashAt)
	wk.Start(ctx)
	svc := service.New(st)
	for _, d := range disks {
		if _, err := svc.CreateErasureJob(ctx, d.ID, standard, "demo-operator"); err != nil {
			logger.Fatal(err)
		}
	}
	// watch first disk's progress, then forcibly stop the process (= power loss)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		jobs, _ := st.ListJobs(ctx, disks[0].ID)
		if len(jobs) > 0 && jobs[0].BytesDone >= int64(crashAt)<<20 {
			fmt.Printf(">>> POWER LOSS at pass=%d offset=%d MiB; killing worker without cleanup\n",
				jobs[0].CurrentPass, jobs[0].BytesDone>>20)
			wk.KillForDemo()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	logger.Fatal("crash point never reached")
}

func waitVerified(st store.Store, disks []domain.Disk, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, d := range disks {
			g, _ := st.GetDisk(context.Background(), d.ID)
			if g.Status != domain.DiskVerified {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	log.Fatal("timeout waiting for verified disks")
}

func dump(label string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("---- %s ----\n%s\n", label, b)
}
