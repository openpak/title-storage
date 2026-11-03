// Package httplog writes one slog line per HTTP request: method, path, status and
// duration. It never logs the query string, headers, cookies or bodies, which carry
// tokens, passwords, PINs and certificates. /healthz and /metrics are skipped.
//
// The same file is copied into every OpenPak service (they share no library); keep
// the copies identical.
package httplog

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Flush keeps streaming responses working.
func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack keeps websocket and CONNECT upgrades working.
func (s *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("httplog: underlying ResponseWriter is not a Hijacker")
	}
	if s.status == 0 {
		s.status = http.StatusSwitchingProtocols
	}
	return h.Hijack()
}

// Unwrap lets http.ResponseController reach the real writer.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Middleware logs one line per request after it completes.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "/healthz" || p == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		if len(p) > 200 {
			p = p[:200]
		}
		slog.Info("http", "method", r.Method, "path", p, "status", status,
			"ms", time.Since(start).Milliseconds())
	})
}
