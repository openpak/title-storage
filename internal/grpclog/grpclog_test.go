package grpclog

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryLogsCodeNoPayload(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)
	_, err := Unary(context.Background(), "secret-req", &grpc.UnaryServerInfo{FullMethod: "/a.B/C"},
		func(context.Context, any) (any, error) { return nil, status.Error(codes.NotFound, "secret-msg") })
	if err == nil {
		t.Fatal("want error")
	}
	out := buf.String()
	for _, want := range []string{"method=/a.B/C", "code=NotFound", "ms="} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
	if strings.Contains(out, "secret") {
		t.Errorf("leaked: %q", out)
	}
	_, _ = Unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(context.Context, any) (any, error) { return nil, errors.New("x") })
	if strings.Contains(buf.String(), "health") {
		t.Error("health logged")
	}
}
