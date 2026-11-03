// Package grpclog writes one slog line per gRPC unary call: method, code and
// duration. It never logs requests, responses, metadata or error messages.
//
// The same file is copied into every OpenPak gRPC service; keep the copies identical.
package grpclog

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Unary is a grpc.UnaryServerInterceptor; put it first in the chain.
func Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if strings.HasPrefix(info.FullMethod, "/grpc.health.") {
		return h(ctx, req)
	}
	start := time.Now()
	resp, err := h(ctx, req)
	slog.Info("grpc", "method", info.FullMethod, "code", status.Code(err).String(),
		"ms", time.Since(start).Milliseconds())
	return resp, err
}
