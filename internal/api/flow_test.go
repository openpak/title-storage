// The title storage flow against a real Postgres (TITLE_STORAGE_TEST_DATABASE_URL) and a fake
// account core over bufconn: the Title User Storage variable semantics (set, add, compare-and-set,
// conditions, friend scopes, paging), objects (metadata filter, visibility, quota, size cap, expiry,
// ids), title keys across networks, the per-title network opt-out and account erasure. Skipped
// without a database, like every integration surface here.
package api_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	accountv1 "github.com/openpak/account-api/openpak/account/v1"
	"github.com/openpak/title-storage/internal/api"
	"github.com/openpak/title-storage/internal/core"
	"github.com/openpak/title-storage/internal/grpcapi"
	"github.com/openpak/title-storage/internal/service"
	"github.com/openpak/title-storage/internal/store"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

const (
	alice = "11111111-1111-1111-1111-111111111111"
	bob   = "22222222-2222-2222-2222-222222222222"
	carol = "33333333-3333-3333-3333-333333333333"
	dave  = "44444444-4444-4444-4444-444444444444"
	key   = "test-key"
)

type fakeCore struct {
	accountv1.UnimplementedSocialServer
	friends map[string][]string
	blocked map[string]bool // actor+"|"+target
}

func (f *fakeCore) ListFriends(_ context.Context, req *accountv1.ListFriendsRequest) (*accountv1.ListFriendsResponse, error) {
	out := &accountv1.ListFriendsResponse{}
	for _, id := range f.friends[req.GetAccountId()] {
		out.Friends = append(out.Friends, &accountv1.Friend{AccountId: id})
	}
	return out, nil
}

func (f *fakeCore) CheckInteraction(_ context.Context, req *accountv1.CheckInteractionRequest) (*accountv1.CheckInteractionResponse, error) {
	return &accountv1.CheckInteractionResponse{Allowed: !f.blocked[req.GetActorId()+"|"+req.GetTargetId()]}, nil
}

type env struct {
	ts   pb.TitleStorageClient
	http *httptest.Server
	st   *store.Store
	ctx  context.Context
}

func setup(t *testing.T) *env { t.Helper(); return setupWith(t, nil) }

// setupWith lets a test configure the service (the blob tier) before it serves.
func setupWith(t *testing.T, configure func(*service.Service)) *env {
	t.Helper()
	tt = t
	dbURL := os.Getenv("TITLE_STORAGE_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TITLE_STORAGE_TEST_DATABASE_URL not set")
	}
	st, err := store.Open(context.Background(), dbURL, store.Defaults{QuotaBytes: 1000, QuotaObjects: 4, MaxObjectBytes: 400})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if _, err := st.DB.Exec(context.Background(), `TRUNCATE variables, objects, title_keys, titles, uploads, blob_trash`); err != nil {
		t.Fatal(err)
	}
	if err := st.MapTitle(context.Background(), "terraria", "Terraria", "np", "NPWR04270_00"); err != nil {
		t.Fatal(err)
	}

	fake := &fakeCore{friends: map[string][]string{alice: {bob, carol}, bob: {alice}, carol: {alice}}, blocked: map[string]bool{dave + "|" + alice: true}}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	accountv1.RegisterSocialServer(gs, fake)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	dial := func(l *bufconn.Listener) *grpc.ClientConn {
		c, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	svc := &service.Service{Store: st, Core: core.New(accountv1.NewSocialClient(dial(lis)), key)}
	if configure != nil {
		configure(svc)
	}

	lis2 := bufconn.Listen(1 << 20)
	gs2 := grpc.NewServer()
	pb.RegisterTitleStorageServer(gs2, grpcapi.New(svc, key))
	go func() { _ = gs2.Serve(lis2) }()
	t.Cleanup(gs2.Stop)

	mux := http.NewServeMux()
	(&api.Server{Svc: svc, Key: key}).Register(mux)
	h := httptest.NewServer(mux)
	t.Cleanup(h.Close)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-internal-key", key)
	return &env{ts: pb.NewTitleStorageClient(dial(lis2)), http: h, st: st, ctx: ctx}
}

var np = &pb.TitleRef{Network: "np", Title: "NPWR04270_00"}

var tt *testing.T // the running test, for must

func must[T any](v T, err error) T {
	tt.Helper()
	if err != nil {
		tt.Fatal(err)
	}
	return v
}

func code(err error) codes.Code { return status.Code(err) }

func items(kv ...int64) []*pb.VariableItem {
	var out []*pb.VariableItem
	for i := 0; i < len(kv); i += 2 {
		out = append(out, &pb.VariableItem{Slot: int32(kv[i]), Value: kv[i+1]})
	}
	return out
}

func TestVariables(t *testing.T) {
	e := setup(t)
	c := e.ctx
	if _, err := e.ts.SetVariables(c, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Setter: "alice@us", Items: items(1, 10, 2, 20), Visibility: pb.Visibility_VISIBILITY_PUBLIC}); err != nil {
		t.Fatal(err)
	}
	// Anyone reads public rows; slot filter and missing slots.
	r := must(e.ts.ListVariables(c, &pb.ListVariablesRequest{Ref: np, Viewer: dave, Owners: []string{alice}, Slots: []int32{1, 3}}))
	if len(r.Variables) != 1 || r.Variables[0].Value != 10 || r.Variables[0].Setter != "alice@us" || r.Hits != 1 {
		t.Fatalf("list: %+v", r)
	}
	// Add (missing counts as 0), then add to existing.
	a := must(e.ts.AddVariable(c, &pb.AddVariableRequest{Ref: np, Owner: alice, Writer: alice, Setter: "alice@us", Slot: 9, Delta: 5}))
	if a.Variable.Value != 5 || !a.Stored {
		t.Fatalf("add new: %+v", a)
	}
	a = must(e.ts.AddVariable(c, &pb.AddVariableRequest{Ref: np, Owner: alice, Writer: alice, Setter: "alice@us", Slot: 9, Delta: -2}))
	if a.Variable.Value != 3 {
		t.Fatalf("add: %+v", a)
	}
	// TrySet: high-score idiom "operand gt current" stores only a bigger value.
	try := func(v int64, op string, cond *pb.Condition) *pb.VariableResult {
		return must(e.ts.TrySetVariable(c, &pb.TrySetVariableRequest{Ref: np, Owner: alice, Writer: alice, Setter: "alice@us", Slot: 1, Value: v, Op: op, Operand: v, Condition: cond}))
	}
	if r := try(50, "gt", nil); !r.Stored || r.PreviousValue != 10 || r.Variable.Value != 50 {
		t.Fatalf("tryset gt: %+v", r)
	}
	if r := try(40, "gt", nil); r.Stored || r.Variable.Value != 50 || r.PreviousValue != 50 {
		t.Fatalf("tryset must keep: %+v", r)
	}
	// A missing slot compares as 0.
	if r := must(e.ts.TrySetVariable(c, &pb.TrySetVariableRequest{Ref: np, Owner: alice, Writer: alice, Slot: 7, Value: 1, Op: "gt", Operand: 1})); !r.Stored {
		t.Fatalf("tryset on missing: %+v", r)
	}
	// Conditions: wrong date, wrong setter, right ones.
	cur := must(e.ts.ListVariables(c, &pb.ListVariablesRequest{Ref: np, Viewer: alice, Owners: []string{alice}, Slots: []int32{1}})).Variables[0]
	bad := cur.UpdatedUnixMs + 10
	if r := try(99, "ge", &pb.Condition{ExpectUpdatedUnixMs: &bad}); !r.ConditionFailed || r.Stored {
		t.Fatalf("wrong date should fail: %+v", r)
	}
	other := "bob@us"
	if r := try(99, "ge", &pb.Condition{ExpectSetter: &other}); !r.ConditionFailed {
		t.Fatalf("wrong setter should fail: %+v", r)
	}
	if r := try(99, "ge", &pb.Condition{ExpectUpdatedUnixMs: &cur.UpdatedUnixMs, ExpectSetter: &cur.Setter}); !r.Stored || r.Variable.Value != 99 {
		t.Fatalf("right condition: %+v", r)
	}
	// A condition on a missing row fails.
	when := int64(1)
	if r := must(e.ts.AddVariable(c, &pb.AddVariableRequest{Ref: np, Owner: alice, Writer: alice, Slot: 77, Delta: 1, Condition: &pb.Condition{ExpectUpdatedUnixMs: &when}})); !r.ConditionFailed || r.Variable != nil {
		t.Fatalf("missing row with condition: %+v", r)
	}
	// Writer rules: self ok, a blocked writer is refused, an allowed one writes (TUS lets players write to each other).
	_, err := e.ts.SetVariables(c, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: dave, Items: items(1, 1)})
	if code(err) != codes.PermissionDenied {
		t.Fatalf("blocked writer: %v", err)
	}
	if _, err = e.ts.SetVariables(c, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: bob, Setter: "bob@us", Items: items(5, 5)}); err != nil {
		t.Fatalf("interacting writer: %v", err)
	}
	// Virtual owners: any account writes and reads (title-wide data).
	if _, err = e.ts.SetVariables(c, &pb.SetVariablesRequest{Ref: np, Owner: "v:global", Writer: dave, Items: items(1, 123)}); err != nil {
		t.Fatal(err)
	}
	r = must(e.ts.ListVariables(c, &pb.ListVariablesRequest{Ref: np, Viewer: carol, Owners: []string{"v:global"}}))
	if len(r.Variables) != 1 || r.Variables[0].Value != 123 {
		t.Fatalf("virtual: %+v", r)
	}
	// Delete.
	d := must(e.ts.DeleteVariables(c, &pb.DeleteVariablesRequest{Ref: np, Owner: alice, Writer: alice, Slots: []int32{2, 9, 404}}))
	if d.Deleted != 2 {
		t.Fatalf("deleted %d", d.Deleted)
	}
	// Overflow is a clean error.
	if _, err = e.ts.SetVariables(c, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Items: items(40, 1<<62)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err = e.ts.AddVariable(c, &pb.AddVariableRequest{Ref: np, Owner: alice, Writer: alice, Slot: 40, Delta: 1 << 62})
	}
	if code(err) != codes.InvalidArgument {
		t.Fatalf("overflow: %v", err)
	}
}

func TestVisibilityAndFriendScopes(t *testing.T) {
	e := setup(t)
	c := e.ts
	set := func(owner string, vis pb.Visibility, v int64) {
		if _, err := c.SetVariables(e.ctx, &pb.SetVariablesRequest{Ref: np, Owner: owner, Writer: owner, Setter: owner[:2], Items: items(1, v), Visibility: vis}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Millisecond)
	}
	set(bob, pb.Visibility_VISIBILITY_FRIENDS, 30)
	set(carol, pb.Visibility_VISIBILITY_PUBLIC, 20)
	set(dave, pb.Visibility_VISIBILITY_PRIVATE, 99)
	set(alice, pb.Visibility_VISIBILITY_PRIVATE, 10)

	list := func(viewer string, req *pb.ListVariablesRequest) []int64 {
		req.Ref, req.Viewer = np, viewer
		var out []int64
		for _, v := range must(c.ListVariables(e.ctx, req)).Variables {
			out = append(out, v.Value)
		}
		return out
	}
	eq := func(got, want []int64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v want %v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("got %v want %v", got, want)
			}
		}
	}
	all := []string{alice, bob, carol, dave}
	// alice (bob and carol are friends): sees her own private, bob's friends row, carol's public; not dave's private.
	eq(list(alice, &pb.ListVariablesRequest{Owners: all, Sort: "value"}), []int64{30, 20, 10})
	// dave (no friends): only public.
	eq(list(dave, &pb.ListVariablesRequest{Owners: all}), []int64{99, 20}) // his own private row, newest first
	// anonymous: public only.
	eq(list("", &pb.ListVariablesRequest{Owners: all}), []int64{20})
	// Friend scope: alice's friends, newest first (default), then ascending by value, with paging.
	eq(list(alice, &pb.ListVariablesRequest{FriendsOfViewer: true}), []int64{20, 30})
	eq(list(alice, &pb.ListVariablesRequest{FriendsOfViewer: true, IncludeSelf: true, Sort: "value", Ascending: true}), []int64{10, 20, 30})
	r := must(c.ListVariables(e.ctx, &pb.ListVariablesRequest{Ref: np, Viewer: alice, FriendsOfViewer: true, IncludeSelf: true, Sort: "value", Offset: 1, Limit: 1}))
	if r.Hits != 3 || len(r.Variables) != 1 || r.Variables[0].Value != 20 {
		t.Fatalf("paging: %+v", r)
	}
	// No owners and no friend scope is a bad request; an unwritten title is empty, not an error.
	if _, err := c.ListVariables(e.ctx, &pb.ListVariablesRequest{Ref: np, Viewer: alice}); code(err) != codes.InvalidArgument {
		t.Fatalf("empty scope: %v", err)
	}
	r = must(c.ListVariables(e.ctx, &pb.ListVariablesRequest{Ref: &pb.TitleRef{Network: "np", Title: "NPWR99999_00"}, Viewer: alice, Owners: []string{alice}}))
	if len(r.Variables) != 0 {
		t.Fatalf("unwritten: %+v", r)
	}
	// Bad ids and missing credentials.
	if _, err := c.ListVariables(e.ctx, &pb.ListVariablesRequest{Ref: np, Owners: []string{"nobody"}}); code(err) != codes.InvalidArgument {
		t.Fatalf("bad owner: %v", err)
	}
	if _, err := c.ListVariables(context.Background(), &pb.ListVariablesRequest{Ref: np, Owners: []string{alice}}); code(err) != codes.Unauthenticated {
		t.Fatalf("no key: %v", err)
	}
}

func TestObjects(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	put := func(req *pb.PutObjectRequest) *pb.ObjectResponse {
		req.Ref = np
		if req.Owner == "" {
			req.Owner, req.Writer = alice, alice
		}
		return must(c.PutObject(ctx, req))
	}
	o := put(&pb.PutObjectRequest{Collection: "tus", Name: "3", Info: []byte("hi"), MetaJson: `{"kind":"save","lvl":3}`, Body: []byte("0123456789"), Visibility: pb.Visibility_VISIBILITY_FRIENDS, Setter: "al"})
	if !o.Found || o.Object.Size != 10 || o.Object.Id == 0 || o.Object.Visibility != pb.Visibility_VISIBILITY_FRIENDS {
		t.Fatalf("put: %+v", o)
	}
	id := o.Object.Id
	// Overwrite keeps the id and visibility (none given).
	o = put(&pb.PutObjectRequest{Collection: "tus", Name: "3", Body: []byte("abc")})
	if o.Object.Id != id || o.Object.Size != 3 || o.Object.Visibility != pb.Visibility_VISIBILITY_FRIENDS {
		t.Fatalf("overwrite: %+v", o)
	}
	// Reads: owner, friend yes; stranger and anonymous get the same answer as a missing object.
	get := func(viewer string) *pb.ObjectResponse {
		return must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: viewer, Owner: alice, Collection: "tus", Name: "3", IncludeBody: true}))
	}
	if g := get(alice); !g.Found || string(g.Object.Body) != "abc" {
		t.Fatalf("owner get: %+v", g)
	}
	if g := get(bob); !g.Found {
		t.Fatalf("friend get: %+v", g)
	}
	if g := get(dave); g.Found {
		t.Fatalf("stranger get: %+v", g)
	}
	if g := get(""); g.Found {
		t.Fatalf("anonymous get: %+v", g)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: id})); !g.Found || len(g.Object.Body) != 0 {
		t.Fatalf("by id (no body requested): %+v", g)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Viewer: dave, Id: id})); g.Found {
		t.Fatalf("by id stranger: %+v", g)
	}
	// Without an owner the name is looked up across owners (an upload reference), viewer rules intact.
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: bob, Collection: "tus", Name: "3"})); !g.Found || g.Object.Owner != alice {
		t.Fatalf("find by name: %+v", g)
	}
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: dave, Collection: "tus", Name: "3"})); g.Found {
		t.Fatalf("find by name, stranger: %+v", g)
	}
	// A server-named object gets its id as its name.
	n := put(&pb.PutObjectRequest{Collection: "uploads", Body: []byte("x"), Visibility: pb.Visibility_VISIBILITY_PUBLIC})
	if n.Object.Name == "" || n.Object.Collection != "uploads" {
		t.Fatalf("auto name: %+v", n)
	}
	// List with metadata filter and friend scope.
	put(&pb.PutObjectRequest{Collection: "tus", Name: "4", MetaJson: `{"kind":"photo"}`, Body: []byte("p"), Visibility: pb.Visibility_VISIBILITY_PUBLIC, Owner: bob, Writer: bob})
	l := must(c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, Viewer: alice, FriendsOfViewer: true, IncludeSelf: true, Collection: "tus", MetaFilterJson: `{"kind":"photo"}`}))
	if l.Hits != 1 || l.Objects[0].Owner != bob || len(l.Objects[0].Body) != 0 {
		t.Fatalf("meta filter: %+v", l)
	}
	l = must(c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, Viewer: alice, FriendsOfViewer: true, IncludeSelf: true, Collection: "tus", Sort: "size", IncludeBody: true}))
	if l.Hits != 2 || l.Objects[0].Owner != bob && string(l.Objects[0].Body) != "abc" {
		t.Fatalf("list: %+v", l)
	}
	l = must(c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, Viewer: dave, Owners: []string{alice, bob}}))
	if l.Hits != 2 { // alice's public upload and bob's public photo; the friends-only save is hidden
		t.Fatalf("stranger list: %+v", l)
	}
	// Size cap and quota (defaults here: 400 bytes per object, 1000 bytes, 4 objects).
	if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: "big", Body: make([]byte, 401)}); code(err) != codes.ResourceExhausted {
		t.Fatalf("size cap: %v", err)
	}
	for _, nm := range []string{"a", "b"} {
		if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: nm, Body: make([]byte, 400)}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: "c", Body: make([]byte, 400)})
	if code(err) != codes.ResourceExhausted {
		t.Fatalf("byte quota: %v", err)
	}
	// Replacing an object counts the difference, not the sum.
	if _, err = c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: "a", Body: make([]byte, 200)}); err != nil {
		t.Fatalf("replace within quota: %v", err)
	}
	u := must(c.Usage(ctx, &pb.UsageRequest{Ref: np, Owner: carol}))
	if u.Bytes != 600 || u.Objects != 2 || u.QuotaBytes != 1000 || u.QuotaObjects != 4 {
		t.Fatalf("usage: %+v", u)
	}
	// A per-title override beats the defaults.
	if _, err = c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "terraria", MaxObjectBytes: 800, QuotaBytes: 5000}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: "c", Body: make([]byte, 800)}); err != nil {
		t.Fatalf("override: %v", err)
	}
	// Conditions on objects.
	bad := o.Object.UpdatedUnixMs + 10
	r := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Collection: "tus", Name: "3", Body: []byte("z"), Condition: &pb.Condition{ExpectUpdatedUnixMs: &bad}}))
	if !r.ConditionFailed {
		t.Fatalf("object condition: %+v", r)
	}
	// Delete: named, then the rest of a collection.
	d := must(c.DeleteObjects(ctx, &pb.DeleteObjectsRequest{Ref: np, Owner: carol, Writer: carol, Collection: "nothing", Names: []string{"a"}}))
	if d.Deleted != 0 {
		t.Fatalf("wrong collection deleted %d", d.Deleted)
	}
	d = must(c.DeleteObjects(ctx, &pb.DeleteObjectsRequest{Ref: np, Owner: carol, Writer: carol, Collection: "default"}))
	if d.Deleted != 3 {
		t.Fatalf("collection delete %d", d.Deleted)
	}
	// A bad metadata blob and an oversize info are refused.
	if _, err = c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "m", MetaJson: `[1]`}); code(err) != codes.InvalidArgument {
		t.Fatalf("meta: %v", err)
	}
	if _, err = c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "i", Info: make([]byte, 5000)}); code(err) != codes.ResourceExhausted {
		t.Fatalf("info: %v", err)
	}
}

func TestExpiry(t *testing.T) {
	e := setup(t)
	if _, err := e.ts.PutObject(e.ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "tmp", Body: []byte("x"), TtlSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.st.DB.Exec(e.ctx, `UPDATE objects SET expires_at = now() - interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	if g := must(e.ts.GetObject(e.ctx, &pb.GetObjectRequest{Ref: np, Viewer: alice, Owner: alice, Name: "tmp"})); g.Found {
		t.Fatalf("expired object served: %+v", g)
	}
	if u := must(e.ts.Usage(e.ctx, &pb.UsageRequest{Ref: np, Owner: alice})); u.Objects != 0 {
		t.Fatalf("expired object counted: %+v", u)
	}
	if n := must(e.st.Expire(e.ctx)); n != 1 {
		t.Fatalf("janitor removed %d", n)
	}
}

func TestTitleKeysAndNetworks(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	// One neutral title, two networks: a PS3 write is a Xbox 360 read.
	if _, err := c.MapTitle(ctx, &pb.MapTitleRequest{Title: "terraria", Network: "x360", NativeId: "4D5307E6"}); err != nil {
		t.Fatal(err)
	}
	x360 := &pb.TitleRef{Network: "x360", Title: "4D5307E6"}
	if _, err := c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Items: items(1, 7), Visibility: pb.Visibility_VISIBILITY_PUBLIC}); err != nil {
		t.Fatal(err)
	}
	r := must(c.ListVariables(ctx, &pb.ListVariablesRequest{Ref: x360, Viewer: bob, Owners: []string{alice}}))
	if len(r.Variables) != 1 || r.Variables[0].Value != 7 || r.Variables[0].Network != "np" {
		t.Fatalf("cross-network read: %+v", r)
	}
	// A native id nobody mapped creates its own title.
	odd := &pb.TitleRef{Network: "wiiu", Title: "0005000010101234"}
	if _, err := c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: odd, Owner: alice, Writer: alice, Items: items(1, 1)}); err != nil {
		t.Fatal(err)
	}
	ts := must(c.ListTitles(ctx, &pb.ListTitlesRequest{}))
	found := map[string]*pb.TitleInfo{}
	for _, ti := range ts.Titles {
		found[ti.Title] = ti
	}
	if found["terraria"].NativeIds["np"] != "NPWR04270_00" || found["terraria"].NativeIds["x360"] != "4D5307E6" || found["wiiu:0005000010101234"] == nil {
		t.Fatalf("titles: %+v", ts)
	}
	// A mapping cannot move once its title holds rows.
	if _, err := c.MapTitle(ctx, &pb.MapTitleRequest{Title: "other", Network: "wiiu", NativeId: "0005000010101234"}); code(err) != codes.FailedPrecondition {
		t.Fatalf("remap: %v", err)
	}
	// Per-title opt-out: a per-network title shows each network its own rows only.
	f := false
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "private-title", CrossNetwork: pb.Tristate_TRISTATE_FALSE}); err != nil {
		t.Fatal(err)
	}
	_ = f
	if _, err := c.MapTitle(ctx, &pb.MapTitleRequest{Title: "private-title", Network: "np", NativeId: "NPWR00001_00"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MapTitle(ctx, &pb.MapTitleRequest{Title: "private-title", Network: "x360", NativeId: "FFFF0001"}); err != nil {
		t.Fatal(err)
	}
	a, b := &pb.TitleRef{Network: "np", Title: "NPWR00001_00"}, &pb.TitleRef{Network: "x360", Title: "FFFF0001"}
	for _, ref := range []*pb.TitleRef{a, b} {
		if _, err := c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: ref, Owner: alice, Writer: alice, Items: items(1, int64(len(ref.Network))), Visibility: pb.Visibility_VISIBILITY_PUBLIC}); err != nil {
			t.Fatal(err)
		}
	}
	if r := must(c.ListVariables(ctx, &pb.ListVariablesRequest{Ref: a, Viewer: alice, Owners: []string{alice}})); len(r.Variables) != 1 || r.Variables[0].Value != 2 {
		t.Fatalf("np view: %+v", r)
	}
	if r := must(c.ListVariables(ctx, &pb.ListVariablesRequest{Ref: b, Viewer: alice, Owners: []string{alice}})); len(r.Variables) != 1 || r.Variables[0].Value != 4 {
		t.Fatalf("x360 view: %+v", r)
	}
	// The flag cannot flip once rows exist, and the neutral key needs a network on a per-network title.
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "private-title", CrossNetwork: pb.Tristate_TRISTATE_TRUE}); code(err) != codes.FailedPrecondition {
		t.Fatalf("flip: %v", err)
	}
	if _, err := c.ListVariables(ctx, &pb.ListVariablesRequest{Ref: &pb.TitleRef{Title: "private-title"}, Owners: []string{alice}}); code(err) != codes.FailedPrecondition {
		t.Fatalf("need network: %v", err)
	}
}

func TestReservedTitleAndErasure(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	// A reserved neutral key (network empty) declares itself; adapters set its policy at start.
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "np-messages", Name: "PS3 message attachments", QuotaBytes: 4000}); err != nil {
		t.Fatal(err)
	}
	msg := &pb.TitleRef{Title: "np-messages"}
	if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: msg, Owner: alice, Writer: alice, Name: "f1", Body: []byte("att"), Visibility: pb.Visibility_VISIBILITY_FRIENDS}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Items: items(1, 1)}); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("DELETE", e.http.URL+"/internal/accounts/"+alice+"/data", nil)
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 401 {
		t.Fatalf("erase without key: %d", resp.StatusCode)
	}
	req.Header.Set("X-Internal-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("erase: %v %v", resp, err)
	}
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: msg, Viewer: alice, Owner: alice, Name: "f1"})); g.Found {
		t.Fatal("object survived erasure")
	}
	if r := must(c.ListVariables(ctx, &pb.ListVariablesRequest{Ref: np, Viewer: alice, Owners: []string{alice}})); len(r.Variables) != 0 {
		t.Fatal("variable survived erasure")
	}
	hz, _ := http.Get(e.http.URL + "/healthz")
	if hz.StatusCode != 200 {
		t.Fatalf("healthz %d", hz.StatusCode)
	}
}
