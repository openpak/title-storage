// The blob tier against a real Postgres (TITLE_STORAGE_TEST_DATABASE_URL) and an S3 bucket: an
// in-process fake S3 always, and a real one (MinIO) as well when TITLE_STORAGE_TEST_S3_ENDPOINT,
// _ACCESS_KEY, _SECRET_KEY (and optionally _BUCKET) are set. Prepare/PUT/Complete, POST forms,
// size and digest checks, size cap and quota reservation, replacement, delete, inline overwrite,
// abandoned-upload expiry and account erasure counting the blobs it deleted.
package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/openpak/title-storage/internal/blobs"
	"github.com/openpak/title-storage/internal/service"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

// fakeS3 is the slice of S3 the blob tier and minio-go use: bucket HEAD/PUT, object
// PUT/GET/HEAD/DELETE and the POST form upload. Signatures are not checked (the live
// verification and the MinIO run do that); their presence is.
type fakeS3 struct {
	mu      sync.Mutex
	buckets map[string]bool
	objs    map[string][]byte
}

func newFakeS3() *fakeS3 { return &fakeS3{buckets: map[string]bool{}, objs: map[string][]byte{}} }

func s3Err(w http.ResponseWriter, code int, c string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+c+`</Code><Message>`+c+`</Message></Error>`)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bucket, k, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if k == "" {
		switch r.Method {
		case http.MethodHead:
			if !f.buckets[bucket] {
				w.WriteHeader(404)
			}
		case http.MethodPut:
			f.buckets[bucket] = true
		case http.MethodPost:
			if err := r.ParseMultipartForm(32 << 20); err != nil || r.FormValue("policy") == "" || r.FormValue("x-amz-signature") == "" {
				s3Err(w, 400, "InvalidPolicyDocument")
				return
			}
			file, _, err := r.FormFile("file")
			if err != nil {
				s3Err(w, 400, "IncorrectNumberOfFilesInPostRequest")
				return
			}
			b, _ := io.ReadAll(file)
			f.objs[bucket+"/"+r.FormValue("key")] = b
			w.WriteHeader(204)
		default:
			s3Err(w, 405, "MethodNotAllowed")
		}
		return
	}
	name := bucket + "/" + k
	if r.Method == http.MethodPut && r.URL.Query().Get("X-Amz-Signature") == "" && r.Header.Get("Authorization") == "" {
		s3Err(w, 403, "AccessDenied")
		return
	}
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.objs[name] = b
		w.Header().Set("ETag", `"etag"`)
	case http.MethodGet, http.MethodHead:
		b, ok := f.objs[name]
		if !ok {
			s3Err(w, 404, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		if r.Method == http.MethodGet {
			w.Write(b)
		}
	case http.MethodDelete:
		delete(f.objs, name)
		w.WriteHeader(204)
	default:
		s3Err(w, 405, "MethodNotAllowed")
	}
}

type blobEnv struct {
	*env
	b *blobs.Store
}

func blobSetup(t *testing.T, cfg blobs.Config) *blobEnv {
	b, err := blobs.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	e := setupWith(t, func(s *service.Service) { s.Blobs = b; s.URLTTL = 5 * time.Minute })
	return &blobEnv{env: e, b: b}
}

func TestBlobTierFake(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	// Consoles are told a different host than the one the service dials: URLs must be signed for it.
	pub := "http://localhost:" + u.Port()
	e := blobSetup(t, blobs.Config{Endpoint: srv.URL, PublicURL: pub, Bucket: "title-storage", Prefix: "objects/", AccessKey: "k", SecretKey: "s"})
	blobFlow(t, e, "localhost:"+u.Port())
}

func TestBlobTierS3(t *testing.T) {
	ep := os.Getenv("TITLE_STORAGE_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("TITLE_STORAGE_TEST_S3_ENDPOINT not set")
	}
	bucket := os.Getenv("TITLE_STORAGE_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "title-storage-test"
	}
	e := blobSetup(t, blobs.Config{Endpoint: ep, Bucket: bucket, Prefix: "test/", AccessKey: os.Getenv("TITLE_STORAGE_TEST_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("TITLE_STORAGE_TEST_S3_SECRET_KEY")})
	pu, _ := url.Parse(ep)
	blobFlow(t, e, pu.Host)
}

func send(t *testing.T, p *pb.PrepareUploadResponse, body []byte) {
	t.Helper()
	var req *http.Request
	if p.Method == "POST" {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for k, v := range p.FormFields {
			mw.WriteField(k, v)
		}
		fw, _ := mw.CreateFormFile("file", "body.bin")
		fw.Write(body)
		mw.Close()
		req, _ = http.NewRequest("POST", p.PutUrl, &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
	} else {
		req, _ = http.NewRequest("PUT", p.PutUrl, bytes.NewReader(body))
		for k, v := range p.Headers {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s upload: %s %s", p.Method, resp.Status, b)
	}
}

func fetch(t *testing.T, u string) []byte {
	t.Helper()
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("get: %s %s", resp.Status, b)
	}
	return b
}

func blobKeyOf(t *testing.T, e *blobEnv, id uint64) string {
	t.Helper()
	var k string
	if err := e.st.DB.QueryRow(context.Background(), `SELECT blob_key FROM objects WHERE id=$1 UNION ALL SELECT blob_key FROM uploads WHERE id=$1 LIMIT 1`, int64(id)).Scan(&k); err != nil {
		t.Fatal(err)
	}
	return k
}

func gone(t *testing.T, e *blobEnv, k string) {
	t.Helper()
	if _, err := e.b.Size(context.Background(), k); !errors.Is(err, blobs.ErrMissing) {
		t.Fatalf("blob %s still there (%v)", k, err)
	}
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func blobFlow(t *testing.T, e *blobEnv, publicHost string) {
	c, ctx := e.ts, e.ctx
	body := bytes.Repeat([]byte("course"), 50) // 300 bytes; the test title caps objects at 400, quota 1000 / 4

	// Size cap and the write rule are checked before anything is reserved.
	if _, err := c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Name: "big", Size: 401}); code(err) != codes.ResourceExhausted {
		t.Fatalf("size cap: %v", err)
	}
	if _, err := c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: dave, Name: "x", Size: 1}); code(err) != codes.PermissionDenied {
		t.Fatalf("blocked writer: %v", err)
	}
	if _, err := c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Name: "x", Size: 1, Sha256: "nothex"}); code(err) != codes.InvalidArgument {
		t.Fatalf("bad digest: %v", err)
	}

	// PUT: prepare, complete too early, send, complete, read through the presigned GET.
	p := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Setter: "a@us", Collection: "courses", Name: "c1",
		Size: uint64(len(body)), Sha256: digest(body), MetaJson: `{"kind":"course"}`, Visibility: pb.Visibility_VISIBILITY_PUBLIC}))
	pu, _ := url.Parse(p.PutUrl)
	if p.Method != "PUT" || p.Id == 0 || pu.Host != publicHost || pu.Query().Get("X-Amz-Signature") == "" || pu.Query().Get("X-Amz-Expires") != "300" {
		t.Fatalf("prepare: %+v", p)
	}
	if p.ExpiresUnixMs < time.Now().Add(50*time.Minute).UnixMilli() {
		t.Fatalf("upload lifetime: %d", p.ExpiresUnixMs)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: alice, Id: p.Id})); g.Found {
		t.Fatal("a pending upload is visible")
	}
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}); code(err) != codes.FailedPrecondition {
		t.Fatalf("complete before upload: %v", err)
	}
	send(t, p, body[:299]) // short body: refused, deleted, the upload stays open
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}); code(err) != codes.FailedPrecondition {
		t.Fatalf("short body: %v", err)
	}
	gone(t, e, blobKeyOf(t, e, p.Id))
	tampered := append([]byte{}, body...)
	tampered[0] = 'C'
	send(t, p, tampered) // right size, wrong digest
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}); code(err) != codes.FailedPrecondition {
		t.Fatalf("digest mismatch: %v", err)
	}
	send(t, p, body)
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: dave}); code(err) != codes.PermissionDenied {
		t.Fatalf("complete by a blocked writer: %v", err)
	}
	r := must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}))
	if !r.Found || r.Object.Id != p.Id || !r.Object.Blob || r.Object.Size != 300 || r.Object.Sha256 != digest(body) || r.Object.GetUrl == "" || len(r.Object.Body) != 0 {
		t.Fatalf("complete: %+v", r)
	}
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}); code(err) != codes.NotFound {
		t.Fatalf("second complete: %v", err)
	}
	g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: p.Id, IncludeBody: true}))
	gu, _ := url.Parse(g.Object.GetUrl)
	if !g.Found || gu.Host != publicHost || g.Object.MetaJson != `{"kind": "course"}` || g.Object.Setter != "a@us" || len(g.Object.Body) != 0 {
		t.Fatalf("get by id: %+v", g)
	}
	if got := fetch(t, g.Object.GetUrl); !bytes.Equal(got, body) {
		t.Fatalf("presigned get: %d bytes", len(got))
	}
	l := must(c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, Viewer: bob, Owners: []string{alice}, Collection: "courses"}))
	if len(l.Objects) != 1 || l.Objects[0].GetUrl == "" {
		t.Fatalf("list: %+v", l)
	}
	if u := must(c.Usage(ctx, &pb.UsageRequest{Ref: np, Owner: alice})); u.Bytes != 300 || u.Objects != 1 {
		t.Fatalf("usage: %+v", u)
	}

	// Replacement keeps the id, deletes the old bytes; a POST form (as NEX consoles send) works.
	first := blobKeyOf(t, e, p.Id)
	p2 := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Collection: "courses", Name: "c1", Size: 10, PostForm: true}))
	if p2.Id != p.Id || p2.Method != "POST" || p2.FormFields["policy"] == "" || p2.FormFields["key"] == "" {
		t.Fatalf("prepare post: %+v", p2)
	}
	send(t, p2, []byte("0123456789"))
	r = must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p2.Id, Writer: alice}))
	if r.Object.Size != 10 || r.Object.Visibility != pb.Visibility_VISIBILITY_PUBLIC {
		t.Fatalf("replace: %+v", r.Object)
	}
	gone(t, e, first)

	// Quota: pending uploads reserve their bytes (10 stored + 3 x 400 pending > 1000).
	var pend []*pb.PrepareUploadResponse
	for _, n := range []string{"q1", "q2", "q3"} {
		pp, err := c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Name: n, Size: 400})
		if n == "q3" {
			if code(err) != codes.ResourceExhausted {
				t.Fatalf("quota with pending uploads: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		pend = append(pend, pp)
	}
	if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "inline", Body: make([]byte, 400)}); code(err) != codes.ResourceExhausted {
		t.Fatalf("inline put over reserved quota: %v", err)
	}

	// An abandoned upload expires and its bytes are deleted.
	send(t, pend[0], make([]byte, 400))
	abandoned := blobKeyOf(t, e, pend[0].Id)
	if _, err := e.st.DB.Exec(context.Background(), `UPDATE uploads SET expires_at = now() - interval '1 second' WHERE id = $1`, int64(pend[0].Id)); err != nil {
		t.Fatal(err)
	}
	if n := must(e.st.Expire(context.Background())); n != 1 {
		t.Fatalf("expired %d", n)
	}
	svc := &service.Service{Store: e.st, Blobs: e.b}
	if n := must(svc.Drain(context.Background(), "")); n != 1 {
		t.Fatalf("janitor drained %d", n)
	}
	gone(t, e, abandoned)
	if _, err := c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: pend[0].Id, Writer: alice}); code(err) != codes.NotFound {
		t.Fatalf("complete after expiry: %v", err)
	}

	// An inline write over a blob-backed object drops the blob; a delete drops the blob.
	p3 := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: bob, Writer: bob, Name: "b1", Size: 3}))
	send(t, p3, []byte("bob"))
	must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p3.Id, Writer: bob}))
	k3 := blobKeyOf(t, e, p3.Id)
	o := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: bob, Writer: bob, Name: "b1", Body: []byte("inline")}))
	if o.Object.Blob || o.Object.Id != p3.Id {
		t.Fatalf("inline over blob: %+v", o.Object)
	}
	gone(t, e, k3)
	p4 := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: bob, Writer: bob, Name: "b2", Size: 3}))
	send(t, p4, []byte("bob"))
	must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p4.Id, Writer: bob}))
	k4 := blobKeyOf(t, e, p4.Id)
	if d := must(c.DeleteObjects(ctx, &pb.DeleteObjectsRequest{Ref: np, Owner: bob, Writer: bob, Names: []string{"b2"}})); d.Deleted != 1 {
		t.Fatalf("delete: %+v", d)
	}
	gone(t, e, k4)

	// Erasure: alice holds c1 (blob), a sent-but-pending upload (q2) and a variable; the answer counts
	// rows and blobs, and every blob is gone. The repeat deletes nothing.
	send(t, pend[1], make([]byte, 400))
	keys := []string{blobKeyOf(t, e, p.Id), blobKeyOf(t, e, pend[1].Id)}
	must(c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Items: items(1, 1)}))
	for i, want := range []map[string]float64{{"deleted": 3, "blobs": 2}, {"deleted": 0, "blobs": 0}} {
		req, _ := http.NewRequest("DELETE", e.http.URL+"/internal/accounts/"+alice+"/data", nil)
		req.Header.Set("X-Internal-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("erase %d: %v %v", i, resp, err)
		}
		var got map[string]float64
		json.NewDecoder(resp.Body).Decode(&got)
		resp.Body.Close()
		if got["deleted"] != want["deleted"] || got["blobs"] != want["blobs"] {
			t.Fatalf("erase %d: %v, want %v", i, got, want)
		}
	}
	for _, k := range keys {
		gone(t, e, k)
	}
	var trash int
	e.st.DB.QueryRow(context.Background(), `SELECT count(*) FROM blob_trash`).Scan(&trash)
	if trash != 0 {
		t.Fatalf("trash left: %d", trash)
	}
}
