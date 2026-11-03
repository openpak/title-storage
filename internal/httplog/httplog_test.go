package httplog

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func capture(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestLogsMethodPathStatusDurationNoSecrets(t *testing.T) {
	buf := capture(t)
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("body-secret"))
	}))
	req := httptest.NewRequest("POST", "/v1/login?password=hunter2&token=abc", strings.NewReader("pin=1234"))
	req.Header.Set("Authorization", "Bearer sekrit")
	req.AddCookie(&http.Cookie{Name: "s", Value: "cookieval"})
	h.ServeHTTP(httptest.NewRecorder(), req)
	out := buf.String()
	for _, want := range []string{"method=POST", "path=/v1/login", "status=418", "ms="} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
	for _, bad := range []string{"hunter2", "token", "abc", "sekrit", "cookieval", "1234", "body-secret", "?"} {
		if strings.Contains(out, bad) {
			t.Errorf("leaked %q in %q", bad, out)
		}
	}
	if strings.Count(out, "\n") != 1 {
		t.Errorf("want one line, got %q", out)
	}
}

func TestDefaultStatus200AndSkips(t *testing.T) {
	buf := capture(t)
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("x")) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
	if buf.Len() != 0 {
		t.Fatalf("skipped paths logged: %q", buf.String())
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/a", nil))
	if !strings.Contains(buf.String(), "status=200") {
		t.Fatalf("got %q", buf.String())
	}
}

func TestFlusherPreserved(t *testing.T) {
	capture(t)
	flushed := false
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("Flusher lost")
		}
		f.Flush()
		flushed = true
		if http.NewResponseController(w).Flush() != nil {
			t.Error("ResponseController flush failed")
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/s", nil))
	if !flushed {
		t.Fatal("handler did not run")
	}
}
