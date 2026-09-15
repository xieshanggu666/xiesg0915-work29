// Package api exposes the HTTP/JSON API.
//
// Every mutating request must identify its operator via the X-Operator
// header (or ?operator=); that identity becomes the audit-log actor.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"idc/decommission/internal/store"
)

type envelope struct {
	Data  any    `json:"data,omitempty"`
	Error *Error `json:"error,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func ok(w http.ResponseWriter, v any)      { writeJSON(w, http.StatusOK, envelope{Data: v}) }
func created(w http.ResponseWriter, v any) { writeJSON(w, http.StatusCreated, envelope{Data: v}) }

func fail(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, envelope{Error: &Error{Code: code, Message: msg}})
}

func writeStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		fail(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrConflict):
		fail(w, http.StatusConflict, "conflict", err.Error())
	default:
		fail(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// operator returns the caller identity from X-Operator / ?operator. Every
// mutating endpoint requires it (enforced by the service layer); an empty
// string means "unauthenticated" and the operation is rejected.
func operator(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Operator")); v != "" {
		return v
	}
	return strings.TrimSpace(r.URL.Query().Get("operator"))
}
