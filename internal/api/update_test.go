package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/openpak/title-storage/internal/blobs"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

func ptr[T any](v T) *T { return &v }

func metaOf(t *testing.T, o *pb.Object) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(o.GetMetaJson()), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// rated puts a rating slot with one rating of 4 (by bob) on the object.
func rated(t *testing.T, e *env, id uint64) {
	t.Helper()
	must(e.ts.InitRating(e.ctx, &pb.InitRatingRequest{Network: "np", Id: id, Writer: alice, Slot: 0, Policy: &pb.RatingPolicy{InitialValue: 10}}))
	if r := must(e.ts.RateObject(e.ctx, &pb.RateObjectRequest{Network: "np", Id: id, Slot: 0, Rater: bob, Value: 4})); !r.Found {
		t.Fatal("rate")
	}
}

func ratingsKept(t *testing.T, o *pb.Object) {
	t.Helper()
	if rs := o.GetRatings(); len(rs) != 1 || rs[0].Total != 14 || rs[0].Count != 1 || rs[0].InitialValue != 10 {
		t.Fatalf("ratings changed: %v", rs)
	}
}

func TestUpdateObjectMeta(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	put := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Setter: "s1", Collection: "uploads", Name: "u",
		Info: []byte("old"), Body: []byte("hello"), MetaJson: `{"tags":["a"],"plays":3}`, Visibility: pb.Visibility_VISIBILITY_PUBLIC})).Object
	rated(t, e, put.Id)
	get := func(viewer string) *pb.Object {
		t.Helper()
		g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: viewer, Id: put.Id, IncludeBody: true, IncludeRatings: true}))
		if !g.Found {
			return nil
		}
		return g.Object
	}

	// Info, merged metadata, visibility + recipients, expiry, setter; the version check holds.
	time.Sleep(20 * time.Millisecond) // past the 10 ms stamp, so updated moves
	exp := time.Now().Add(time.Hour).UnixMilli()
	r := must(c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: put.Id, Writer: alice, Setter: "s2", Info: []byte("new"),
		MetaMergeJson: `{"tags":["b"],"period":90}`, Visibility: pb.Visibility_VISIBILITY_RECIPIENTS, Recipients: []string{dave},
		ExpiresUnixMs: ptr(exp), Condition: &pb.Condition{ExpectUpdatedUnixMs: ptr(put.UpdatedUnixMs), ExpectSetter: ptr("s1")}}))
	if !r.Found || r.ConditionFailed {
		t.Fatalf("update: %+v", r)
	}
	o := get(alice)
	m := metaOf(t, o)
	if string(o.Body) != "hello" || o.Size != 5 || o.Id != put.Id || o.CreatedUnixMs != put.CreatedUnixMs || o.UpdatedUnixMs <= put.UpdatedUnixMs {
		t.Fatalf("body/size/id/times: %+v", o)
	}
	ratingsKept(t, o)
	if string(o.Info) != "new" || m["plays"] != 3.0 || m["period"] != 90.0 || m["tags"].([]any)[0] != "b" || o.Setter != "s2" ||
		o.Visibility != pb.Visibility_VISIBILITY_RECIPIENTS || o.Recipients[0] != dave || o.ExpiresUnixMs/10 != exp/10 {
		t.Fatalf("metadata: %+v %v", o, m)
	}
	if get(dave) == nil || get(carol) != nil {
		t.Fatal("the new visibility does not apply")
	}

	// A stale version changes nothing.
	r = must(c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: put.Id, Writer: alice, MetaJson: ptr(`{}`),
		Condition: &pb.Condition{ExpectUpdatedUnixMs: ptr(put.UpdatedUnixMs)}}))
	if !r.Found || !r.ConditionFailed || r.Object != nil || metaOf(t, get(alice))["plays"] != 3.0 {
		t.Fatalf("stale version: %+v", r)
	}

	// Replace the metadata, clear the info (present and empty), never expire, rename and move.
	r = must(c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: put.Id, Writer: alice, MetaJson: ptr(`{"x":1}`),
		Info: []byte{}, ExpiresUnixMs: ptr(int64(0)), Name: ptr("u2"), Collection: ptr("ghosts")}))
	o = get(alice)
	if len(o.Info) != 0 || o.MetaJson != `{"x": 1}` || o.ExpiresUnixMs != 0 || o.Name != "u2" || o.Collection != "ghosts" ||
		string(o.Body) != "hello" || o.Visibility != pb.Visibility_VISIBILITY_RECIPIENTS {
		t.Fatalf("replace/rename: %+v", o)
	}
	ratingsKept(t, o)
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: alice, Owner: alice, Collection: "ghosts", Name: "u2"})); !g.Found || g.Object.Id != put.Id {
		t.Fatal("not found under the new name")
	}
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: alice, Owner: alice, Collection: "uploads", Name: "u"})); g.Found {
		t.Fatal("still found under the old name")
	}
	if u := must(c.Usage(ctx, &pb.UsageRequest{Ref: np, Owner: alice})); u.Bytes != 5 || u.Objects != 1 {
		t.Fatalf("usage: %+v", u)
	}

	// Collisions, refusals and the write rule.
	must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Collection: "ghosts", Name: "taken", Body: []byte("t")}))
	for name, tc := range map[string]struct {
		req  *pb.UpdateObjectMetaRequest
		want codes.Code
	}{
		"name taken":            {&pb.UpdateObjectMetaRequest{Name: ptr("taken")}, codes.AlreadyExists},
		"nothing to change":     {&pb.UpdateObjectMetaRequest{}, codes.InvalidArgument},
		"empty name":            {&pb.UpdateObjectMetaRequest{Name: ptr("")}, codes.InvalidArgument},
		"bad collection":        {&pb.UpdateObjectMetaRequest{Collection: ptr("a b")}, codes.InvalidArgument},
		"meta not an object":    {&pb.UpdateObjectMetaRequest{MetaJson: ptr(`[1]`)}, codes.InvalidArgument},
		"merge not an object":   {&pb.UpdateObjectMetaRequest{MetaMergeJson: `"x"`}, codes.InvalidArgument},
		"expiry in the past":    {&pb.UpdateObjectMetaRequest{ExpiresUnixMs: ptr(time.Now().Add(-time.Minute).UnixMilli())}, codes.InvalidArgument},
		"recipients, no vis":    {&pb.UpdateObjectMetaRequest{Recipients: []string{bob}}, codes.InvalidArgument},
		"bad recipient":         {&pb.UpdateObjectMetaRequest{Visibility: pb.Visibility_VISIBILITY_RECIPIENTS, Recipients: []string{"nope"}}, codes.InvalidArgument},
		"info too large":        {&pb.UpdateObjectMetaRequest{Info: make([]byte, 4097)}, codes.ResourceExhausted},
		"merged meta too large": {&pb.UpdateObjectMetaRequest{MetaJson: ptr(`{"a":"` + strings.Repeat("x", 15000) + `"}`), MetaMergeJson: `{"b":"` + strings.Repeat("y", 2000) + `"}`}, codes.ResourceExhausted},
	} {
		tc.req.Network, tc.req.Id, tc.req.Writer = "np", put.Id, alice
		if _, err := c.UpdateObjectMeta(ctx, tc.req); code(err) != tc.want {
			t.Errorf("%s: %v, want %v", name, err, tc.want)
		}
	}
	if _, err := c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: put.Id, Writer: dave, MetaMergeJson: `{"a":1}`}); code(err) != codes.PermissionDenied {
		t.Fatalf("blocked writer: %v", err)
	}
	if r := must(c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: 999999999, Writer: alice, MetaMergeJson: `{"a":1}`})); r.Found {
		t.Fatal("missing id found")
	}
	o = get(alice)
	if o.MetaJson != `{"x": 1}` || o.Name != "u2" {
		t.Fatalf("a refused update changed the object: %+v", o)
	}
	ratingsKept(t, o)
}

// The blob-backed case: UpdateObjectMeta keeps the bucket body; PutObject over the same object
// still drops it (the documented difference).
func TestUpdateObjectMetaBlob(t *testing.T) {
	srv := httptest.NewServer(newFakeS3())
	t.Cleanup(srv.Close)
	e := blobSetup(t, blobs.Config{Endpoint: srv.URL, Bucket: "title-storage", AccessKey: "k", SecretKey: "s"})
	c, ctx := e.ts, e.ctx
	body := bytes.Repeat([]byte("ghost"), 40)
	p := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Collection: "ghosts", Name: "g", Size: uint64(len(body)),
		Sha256: digest(body), MetaJson: `{"course":7}`, Visibility: pb.Visibility_VISIBILITY_PUBLIC}))
	send(t, p, body)
	must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice}))
	rated(t, e.env, p.Id)
	k := blobKeyOf(t, e, p.Id)

	// A rename is refused while an upload for the object is in flight.
	p2 := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Collection: "ghosts", Name: "g", Size: 3}))
	if p2.Id != p.Id {
		t.Fatalf("replacement upload id %d", p2.Id)
	}
	if _, err := c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: p.Id, Writer: alice, Name: ptr("g2")}); code(err) != codes.FailedPrecondition {
		t.Fatalf("rename with a pending upload: %v", err)
	}

	r := must(c.UpdateObjectMeta(ctx, &pb.UpdateObjectMetaRequest{Network: "np", Id: p.Id, Writer: alice, Info: []byte("meta-binary"),
		MetaMergeJson: `{"period":30}`, Collection: ptr("ghosts"), Visibility: pb.Visibility_VISIBILITY_FRIENDS}))
	o := r.Object
	if !r.Found || !o.Blob || o.GetUrl == "" || o.Size != uint64(len(body)) || o.Sha256 != digest(body) || o.Visibility != pb.Visibility_VISIBILITY_FRIENDS {
		t.Fatalf("update: %+v", r)
	}
	if got := fetch(t, o.GetUrl); !bytes.Equal(got, body) {
		t.Fatalf("blob body changed: %q", got)
	}
	if blobKeyOf(t, e, p.Id) != k {
		t.Fatal("blob key changed")
	}
	g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: p.Id, IncludeRatings: true}))
	if !g.Found || string(g.Object.Info) != "meta-binary" || metaOf(t, g.Object)["course"] != 7.0 || metaOf(t, g.Object)["period"] != 30.0 {
		t.Fatalf("read back: %+v", g)
	}
	ratingsKept(t, g.Object)
	var trashed int
	if err := e.st.DB.QueryRow(context.Background(), `SELECT count(*) FROM blob_trash WHERE key=$1`, k).Scan(&trashed); err != nil || trashed != 0 {
		t.Fatalf("blob trashed by a metadata update: %d %v", trashed, err)
	}

	// PutObject is a replacement: over the blob-backed object it drops the blob (and keeps the id).
	put := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Collection: "ghosts", Name: "g", Body: []byte("inline")}))
	if put.Object.Id != p.Id || put.Object.Blob {
		t.Fatalf("put over blob: %+v", put.Object)
	}
	gone(t, e, k)
}
