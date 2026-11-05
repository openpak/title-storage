// Title storage: the neutral per-title, per-account store. Variables (int64 slots with the Title
// User Storage semantics) and objects (named blobs with metadata, visibility and quotas) under a
// title key that is the same on every network. Console adapters (np-tus, np-uploader, later
// wfc-sake, nex DataStore, xb-stfd, npln UGC) call it over gRPC. Ports: 20230 HTTP, 20231 gRPC,
// 20232 metrics (block 20230).
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/openpak/title-storage/internal/api"
	"github.com/openpak/title-storage/internal/blobs"
	"github.com/openpak/title-storage/internal/core"
	"github.com/openpak/title-storage/internal/grpcapi"
	"github.com/openpak/title-storage/internal/grpclog"
	"github.com/openpak/title-storage/internal/httplog"
	"github.com/openpak/title-storage/internal/service"
	"github.com/openpak/title-storage/internal/store"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(k), 10, 64); err == nil && v > 0 {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	defaults := store.Defaults{
		QuotaBytes:     envInt("TITLE_STORAGE_QUOTA_BYTES", 32<<20),
		QuotaObjects:   int(envInt("TITLE_STORAGE_QUOTA_OBJECTS", 256)),
		MaxObjectBytes: int(envInt("TITLE_STORAGE_MAX_OBJECT_BYTES", 4<<20)),
	}
	st, err := store.Open(ctx, env("TITLE_STORAGE_DATABASE_URL", "postgres://postgres:test@127.0.0.1:54330/title_storage?sslmode=disable"), defaults)
	if err != nil {
		log.Fatalf("title-storage: database: %v", err)
	}
	defer st.Close()
	coreClient, err := core.Dial(env("TITLE_STORAGE_CORE_ADDRESS", "127.0.0.1:20001"), os.Getenv("TITLE_STORAGE_CORE_INTERNAL_KEY"))
	if err != nil {
		log.Fatalf("title-storage: %v", err)
	}
	coreClient.FriendsTTL = envDur("TITLE_STORAGE_FRIENDS_CACHE_TTL", 2*time.Minute)
	svc := &service.Service{Store: st, Core: coreClient,
		URLTTL: envDur("TITLE_STORAGE_URL_TTL", 15*time.Minute), UploadTTL: envDur("TITLE_STORAGE_UPLOAD_TTL", time.Hour)}
	key := os.Getenv("TITLE_STORAGE_INTERNAL_KEY")

	// The blob tier is optional: without a bucket, PrepareUpload answers FailedPrecondition and
	// every object keeps its body inline, exactly as before.
	if ep := os.Getenv("TITLE_STORAGE_S3_ENDPOINT"); ep != "" {
		b, err := blobs.New(blobs.Config{Endpoint: ep, PublicURL: os.Getenv("TITLE_STORAGE_S3_PUBLIC_URL"),
			Bucket: env("TITLE_STORAGE_S3_BUCKET", "title-storage"), Prefix: os.Getenv("TITLE_STORAGE_S3_PREFIX"),
			AccessKey: os.Getenv("TITLE_STORAGE_S3_ACCESS_KEY"), SecretKey: os.Getenv("TITLE_STORAGE_S3_SECRET_KEY"),
			Region: os.Getenv("TITLE_STORAGE_S3_REGION")})
		if err != nil {
			log.Fatalf("title-storage: blob tier: %v", err)
		}
		bctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := b.EnsureBucket(bctx); err != nil {
			log.Printf("title-storage: blob tier: bucket: %v (uploads fail until it is reachable)", err)
		}
		cancel()
		svc.Blobs = b
		log.Printf("title-storage: blob tier on (bucket %s)", env("TITLE_STORAGE_S3_BUCKET", "title-storage"))
	} else {
		log.Printf("title-storage: TITLE_STORAGE_S3_ENDPOINT not set: blob tier off, bodies inline only")
	}

	// Expired objects (a TTL on an upload) are never served and uploads never completed expire;
	// the janitor frees the rows and deletes the trashed blobs (those a write could not delete).
	go func() {
		for range time.Tick(10 * time.Minute) {
			if n, err := st.Expire(ctx); err != nil {
				log.Printf("title-storage: expire: %v", err)
			} else if n > 0 {
				log.Printf("title-storage: expired %d objects and uploads", n)
			}
			if n, err := svc.Drain(ctx, ""); err != nil {
				log.Printf("title-storage: blob trash: %v", err)
			} else if n > 0 {
				log.Printf("title-storage: deleted %d trashed blobs", n)
			}
		}
	}()

	mux := http.NewServeMux()
	(&api.Server{Svc: svc, Key: key}).Register(mux)
	httpAddr := env("TITLE_STORAGE_HTTP_ADDR", "127.0.0.1:20230")
	go func() {
		log.Printf("title-storage: http on %s", httpAddr)
		srv := &http.Server{Addr: httpAddr, Handler: httplog.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
		log.Fatal(srv.ListenAndServe())
	}()

	grpcAddr := env("TITLE_STORAGE_GRPC_ADDR", "127.0.0.1:20231")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("title-storage: grpc listen: %v", err)
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(grpclog.Unary), grpc.MaxRecvMsgSize(int(defaults.MaxObjectBytes)+(1<<20)), grpc.MaxSendMsgSize(256<<20))
	pb.RegisterTitleStorageServer(gs, grpcapi.New(svc, key))
	go func() {
		log.Printf("title-storage: grpc on %s", grpcAddr)
		log.Fatal(gs.Serve(lis))
	}()

	// Metrics: none beyond the process yet; the port stays claimed so the block is 20230..20239.
	metricsAddr := env("TITLE_STORAGE_METRICS_ADDR", "127.0.0.1:20232")
	go func() {
		m := http.NewServeMux()
		m.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		log.Fatal(http.ListenAndServe(metricsAddr, m))
	}()

	select {}
}
