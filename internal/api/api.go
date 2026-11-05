// Package api is the title storage's HTTP surface. It is small on purpose: the data path is
// gRPC for the console adapters.
//
//	GET    /healthz                                  200 when the database answers
//	DELETE /internal/accounts/{account}/data         account erasure: every variable, object and pending upload the
//	                                                 account owns, and their blobs in the bucket
//	                                                 (X-Internal-Key) -> {"deleted":rows,"blobs":n}
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/openpak/title-storage/internal/service"
)

type Server struct {
	Svc *service.Service
	Key string
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Svc.Store.DB.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("DELETE /internal/accounts/{account}/data", s.auth(s.erase))
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Key == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Key")), []byte(s.Key)) != 1 {
			jsonOut(w, http.StatusUnauthorized, map[string]any{"error": "internal key"})
			return
		}
		h(w, r)
	}
}

func jsonOut(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// erase is the account-erasure finaliser's call once the deletion grace period is over; a repeat is a 200.
func (s *Server) erase(w http.ResponseWriter, r *http.Request) {
	n, b, err := s.Svc.EraseAccount(r.Context(), r.PathValue("account"))
	switch {
	case errors.Is(err, service.ErrBadAccount):
		jsonOut(w, http.StatusBadRequest, map[string]any{"error": "account ids are UUIDs"})
	case err != nil:
		// Blobs not yet deleted stay in the trash under the account: the finaliser's repeat finishes them.
		log.Printf("title-storage: erase: %v", err)
		jsonOut(w, http.StatusBadGateway, map[string]any{"error": "title storage error", "deleted": n, "blobs": b})
	default:
		jsonOut(w, 200, map[string]any{"deleted": n, "blobs": b})
	}
}
