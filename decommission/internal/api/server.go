package api

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"idc/decommission/internal/domain"
	"idc/decommission/internal/objectstore"
	"idc/decommission/internal/service"
	"idc/decommission/internal/store"
)

type Server struct {
	svc *service.Service
	st  store.Store
	obj objectstore.Store
	wk  ReverifyRunner
	log *log.Logger
}

// ReverifyRunner is the subset of the worker API used by HTTP handlers.
type ReverifyRunner interface {
	Reverify(ctx context.Context, diskID, operator string) (*domain.VerifyResult, error)
}

func NewServer(svc *service.Service, st store.Store, obj objectstore.Store, wk ReverifyRunner, logger *log.Logger) *Server {
	return &Server{svc: svc, st: st, obj: obj, wk: wk, log: logger}
}

// Routes builds the application mux.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/v1/standards", s.listStandards)

	mux.HandleFunc("POST /api/v1/assets", s.registerAsset)
	mux.HandleFunc("GET /api/v1/assets", s.listAssets)
	mux.HandleFunc("GET /api/v1/assets/{id}", s.getAsset)
	mux.HandleFunc("POST /api/v1/assets/{id}/disks", s.pullDisks)
	mux.HandleFunc("GET /api/v1/assets/{id}/disks", s.listAssetDisks)
	mux.HandleFunc("GET /api/v1/assets/{id}/certificates", s.listAssetCerts)
	mux.HandleFunc("GET /api/v1/assets/{id}/disposal", s.getDisposal)
	mux.HandleFunc("POST /api/v1/assets/{id}/disposal", s.confirmDisposal)
	mux.HandleFunc("GET /api/v1/assets/{id}/timeline", s.assetTimeline)

	mux.HandleFunc("GET /api/v1/disks/{id}", s.getDisk)
	mux.HandleFunc("POST /api/v1/disks/{id}/jobs", s.createJob)
	mux.HandleFunc("POST /api/v1/disks/{id}/reverify", s.reverifyDisk)
	mux.HandleFunc("POST /api/v1/disks/{id}/scrap", s.scrapDisk)
	mux.HandleFunc("GET /api/v1/disks/{id}/jobs", s.listDiskJobs)

	mux.HandleFunc("GET /api/v1/jobs/{id}", s.getJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/runs", s.listRuns)

	mux.HandleFunc("GET /api/v1/certificates", s.listCertificates)
	mux.HandleFunc("GET /api/v1/certificates/{certNo}", s.getCertificate)
	mux.HandleFunc("GET /api/v1/reports/{certNo}", s.downloadReport)
	mux.HandleFunc("GET /api/v1/reports/{certNo}/certificate", s.downloadCertificate)

	mux.HandleFunc("GET /api/v1/audit", s.listAudit)

	return s.recoverAndLog(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	storeStatus := "ok"
	if err := s.st.Ping(ctx); err != nil {
		storeStatus = "error: " + err.Error()
	}
	ok(w, map[string]string{"store": storeStatus, "object_backend": s.obj.Backend(), "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *Server) listStandards(w http.ResponseWriter, r *http.Request) {
	ok(w, domain.ListStandards())
}

// ---- assets ----

func (s *Server) registerAsset(w http.ResponseWriter, r *http.Request) {
	var in service.RegisterInput
	if !decode(w, r, &in) {
		return
	}
	in.Operator = operator(r)
	a, err := s.svc.RegisterAsset(r.Context(), in)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	created(w, a)
}

func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListAssetsFilter{
		Status: domain.AssetStatus(q.Get("status")),
		Tag:    q.Get("tag"),
		Limit:  atoiDefault(q.Get("limit"), 100),
		Offset: atoiDefault(q.Get("offset"), 0),
	}
	assets, total, err := s.st.ListAssets(r.Context(), f)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, map[string]any{"items": assets, "total": total, "limit": f.Limit, "offset": f.Offset})
}

func (s *Server) getAsset(w http.ResponseWriter, r *http.Request) {
	a, err := s.st.GetAsset(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, a)
}

func (s *Server) pullDisks(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Disks []service.DiskInput `json:"disks"`
	}
	if !decode(w, r, &body) {
		return
	}
	asset, disks, err := s.svc.PullDisks(r.Context(), r.PathValue("id"), operator(r), body.Disks)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	created(w, map[string]any{"asset": asset, "disks": disks})
}

func (s *Server) listAssetDisks(w http.ResponseWriter, r *http.Request) {
	disks, err := s.st.ListDisks(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, disks)
}

func (s *Server) listAssetCerts(w http.ResponseWriter, r *http.Request) {
	certs, err := s.st.ListCertificates(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, certs)
}

func (s *Server) getDisposal(w http.ResponseWriter, r *http.Request) {
	d, err := s.st.GetDisposal(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, d)
}

func (s *Server) confirmDisposal(w http.ResponseWriter, r *http.Request) {
	var in service.DisposalInput
	if !decode(w, r, &in) {
		return
	}
	in.Operator = operator(r)
	d, err := s.svc.ConfirmDisposal(r.Context(), r.PathValue("id"), in)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	created(w, d)
}

// assetTimeline pulls every audit row for the device — who did what, and when.
func (s *Server) assetTimeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	logs, _, err := s.st.ListAudit(r.Context(), store.AuditFilter{EntityType: "asset", EntityID: id, Limit: 500})
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	disks, err := s.st.ListDisks(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	type item struct {
		ID     string              `json:"id"`
		Serial string              `json:"serial"`
		Status domain.DiskStatus   `json:"status"`
		Events []domain.AuditLog   `json:"events"`
		Jobs   []domain.ErasureJob `json:"jobs"`
	}
	diskItems := make([]item, 0, len(disks))
	for _, d := range disks {
		ev, _, _ := s.st.ListAudit(r.Context(), store.AuditFilter{EntityType: "disk", EntityID: d.ID, Limit: 200})
		jobs, _ := s.st.ListJobs(r.Context(), d.ID)
		diskItems = append(diskItems, item{ID: d.ID, Serial: d.Serial, Status: d.Status, Events: ev, Jobs: jobs})
	}
	ok(w, map[string]any{"asset_events": logs, "disks": diskItems})
}

// ---- disks / jobs ----

func (s *Server) getDisk(w http.ResponseWriter, r *http.Request) {
	d, err := s.st.GetDisk(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, d)
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Standard string `json:"standard"`
	}
	if !decode(w, r, &body) {
		return
	}
	j, err := s.svc.CreateErasureJob(r.Context(), r.PathValue("id"), body.Standard, operator(r))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	created(w, j)
}

func (s *Server) reverifyDisk(w http.ResponseWriter, r *http.Request) {
	if s.wk == nil {
		fail(w, http.StatusServiceUnavailable, "no_worker", "worker not attached")
		return
	}
	res, err := s.wk.Reverify(r.Context(), r.PathValue("id"), operator(r))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, res)
}

func (s *Server) scrapDisk(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	if r.ContentLength != 0 {
		if !decode(w, r, &body) {
			return
		}
	}
	if err := s.svc.ScrapDisk(r.Context(), r.PathValue("id"), operator(r), body.Reason); err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, map[string]string{"status": "scrapped"})
}

func (s *Server) listDiskJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.st.ListJobs(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, jobs)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.st.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, j)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.st.ListRuns(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, runs)
}

// ---- certificates & reports ----

func (s *Server) listCertificates(w http.ResponseWriter, r *http.Request) {
	certs, err := s.st.ListCertificates(r.Context(), r.URL.Query().Get("asset_id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, certs)
}

func (s *Server) getCertificate(w http.ResponseWriter, r *http.Request) {
	c, err := s.st.GetCertificate(r.Context(), r.PathValue("certNo"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, c)
}

func (s *Server) downloadReport(w http.ResponseWriter, r *http.Request) {
	c, err := s.st.GetCertificate(r.Context(), r.PathValue("certNo"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.streamObject(w, r, c.ReportKey, "application/json")
}

func (s *Server) downloadCertificate(w http.ResponseWriter, r *http.Request) {
	c, err := s.st.GetCertificate(r.Context(), r.PathValue("certNo"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	s.streamObject(w, r, c.CertKey, "text/html; charset=utf-8")
}

func (s *Server) streamObject(w http.ResponseWriter, r *http.Request, key, contentType string) {
	rc, obj, err := s.obj.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, objectstore.ErrObjectNotFound) {
			fail(w, http.StatusNotFound, "not_found", "report object missing: "+key)
			return
		}
		fail(w, http.StatusBadGateway, "object_error", err.Error())
		return
	}
	defer rc.Close()
	if obj.ContentType != "" {
		contentType = obj.ContentType
	}
	w.Header().Set("Content-Type", contentType)
	if obj.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	if !strings.HasPrefix(contentType, "text/html") {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+key[strings.LastIndex(key, "/")+1:]+"\"")
	}
	_, _ = io.Copy(w, rc)
}

// ---- audit ----

func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.AuditFilter{
		EntityType: q.Get("entity_type"),
		EntityID:   q.Get("entity_id"),
		Actor:      q.Get("actor"),
		Limit:      atoiDefault(q.Get("limit"), 200),
		Offset:     atoiDefault(q.Get("offset"), 0),
	}
	logs, total, err := s.st.ListAudit(r.Context(), f)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ok(w, map[string]any{"items": logs, "total": total, "limit": f.Limit, "offset": f.Offset})
}

// ---- middleware ----

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *Server) recoverAndLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		s.log.Printf("%s %s -> %d (%s) operator=%s", r.Method, r.URL.Path, sw.code, time.Since(start), operator(r))
	})
}

func atoiDefault(v string, def int) int {
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}
