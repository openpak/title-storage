package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/openpak/title-storage/internal/blobs"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

// Friends in the fake core: alice <-> bob, alice <-> carol. dave is nobody's friend.
const erin = "55555555-5555-5555-5555-555555555555"

func names(os []*pb.Object) []string {
	var out []string
	for _, o := range os {
		out = append(out, o.Name)
	}
	slices.Sort(out)
	return out
}

func TestRecipients(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	rcpt := pb.Visibility_VISIBILITY_RECIPIENTS

	// "only": bob alone (uppercase in, lowercase out, repeats dropped). "mixed": dave plus alice's friends.
	only := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "only", Body: []byte("o"),
		Visibility: rcpt, Recipients: []string{strings.ToUpper(bob), bob}}))
	if o := only.Object; o.Visibility != rcpt || !slices.Equal(o.Recipients, []string{bob}) || o.RecipientsIncludeFriends {
		t.Fatalf("put: %+v", o)
	}
	mixed := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "mixed", Body: []byte("m"),
		Visibility: rcpt, Recipients: []string{dave}, RecipientsIncludeFriends: true}))
	must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "priv", Body: []byte("p"),
		Visibility: pb.Visibility_VISIBILITY_PRIVATE, Recipients: []string{dave}}))

	// Get by name and by id: owner + recipients (+ friends with the flag); nobody else, not anonymous.
	for _, tc := range []struct {
		viewer, name string
		want         bool
	}{
		{alice, "only", true}, {bob, "only", true}, {carol, "only", false}, {dave, "only", false}, {"", "only", false},
		{alice, "mixed", true}, {bob, "mixed", true}, {carol, "mixed", true}, {dave, "mixed", true}, {erin, "mixed", false}, {"", "mixed", false},
		{dave, "priv", false}, // recipients grant nothing under another visibility
	} {
		g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: tc.viewer, Owner: alice, Name: tc.name, IncludeBody: true}))
		if g.Found != tc.want {
			t.Errorf("GetObject %s as %q: found=%v", tc.name, tc.viewer, g.Found)
		}
		if g.Found && len(g.Object.Body) != 1 {
			t.Errorf("GetObject %s body: %q", tc.name, g.Object.Body)
		}
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: carol, Id: only.Object.Id})); g.Found {
		t.Fatal("GetObjectByID: carol is not a recipient")
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: carol, Id: mixed.Object.Id})); !g.Found {
		t.Fatal("GetObjectByID: carol is alice's friend")
	}

	// List: the same rule in SQL, in every scope; received = the viewer is named.
	list := func(r *pb.ListObjectsRequest) []string {
		t.Helper()
		r.Ref = np
		return names(must(c.ListObjects(ctx, r)).Objects)
	}
	for _, tc := range []struct {
		viewer string
		want   []string
	}{{alice, []string{"mixed", "only", "priv"}}, {bob, []string{"mixed", "only"}}, {carol, []string{"mixed"}}, {dave, []string{"mixed"}}, {erin, nil}, {"", nil}} {
		if got := list(&pb.ListObjectsRequest{Viewer: tc.viewer, AllOwners: true}); !slices.Equal(got, tc.want) {
			t.Errorf("all_owners as %q: %v, want %v", tc.viewer, got, tc.want)
		}
		if got := list(&pb.ListObjectsRequest{Viewer: tc.viewer, Owners: []string{alice}}); !slices.Equal(got, tc.want) {
			t.Errorf("owners as %q: %v, want %v", tc.viewer, got, tc.want)
		}
	}
	if got := list(&pb.ListObjectsRequest{Viewer: bob, FriendsOfViewer: true}); !slices.Equal(got, []string{"mixed", "only"}) {
		t.Errorf("friends_of_viewer as bob: %v", got)
	}
	if got := list(&pb.ListObjectsRequest{Viewer: dave, AllOwners: true, Received: true}); !slices.Equal(got, []string{"mixed"}) {
		t.Errorf("received as dave: %v (priv names dave but is private)", got)
	}
	if got := list(&pb.ListObjectsRequest{Viewer: bob, AllOwners: true, Received: true}); !slices.Equal(got, []string{"only"}) {
		t.Errorf("received as bob: %v (mixed lets bob in as a friend, it does not name him)", got)
	}
	if _, err := c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, AllOwners: true, Received: true}); code(err) != codes.InvalidArgument {
		t.Fatalf("received without a viewer: %v", err)
	}

	// Ratings follow the read rule: a recipient may rate and read, a stranger is not found.
	must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: only.Object.Id, Writer: alice, Slot: 0, Policy: &pb.RatingPolicy{}}))
	if r := must(c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: only.Object.Id, Slot: 0, Rater: bob, Value: 3})); !r.Found {
		t.Fatal("recipient cannot rate")
	}
	if r := must(c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: only.Object.Id, Slot: 0, Rater: carol, Value: 3})); r.Found {
		t.Fatal("non-recipient rated")
	}
	if g := must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Viewer: carol, Ids: []uint64{only.Object.Id, mixed.Object.Id}})); len(g.Ratings) != 0 {
		t.Fatalf("GetRatings as carol: %v", g.Ratings)
	}
	if g := must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Viewer: bob, Ids: []uint64{only.Object.Id}})); len(g.Ratings) != 1 || g.Ratings[0].Total != 3 {
		t.Fatalf("GetRatings as bob: %v", g.Ratings)
	}

	// Overwrites: no visibility keeps the list and the flag; a visibility replaces them.
	kept := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "mixed", Body: []byte("m2")}))
	if o := kept.Object; o.Visibility != rcpt || !slices.Equal(o.Recipients, []string{dave}) || !o.RecipientsIncludeFriends {
		t.Fatalf("unspecified visibility: %+v", o)
	}
	cleared := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "mixed", Body: []byte("m3"),
		Visibility: pb.Visibility_VISIBILITY_PUBLIC}))
	if o := cleared.Object; len(o.Recipients) != 0 || o.RecipientsIncludeFriends {
		t.Fatalf("new visibility kept recipients: %+v", o)
	}

	// Refusals: bad ids, too many, recipients on variables and as a title default.
	if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "x", Visibility: rcpt, Recipients: []string{"v:npc"}}); code(err) != codes.InvalidArgument {
		t.Fatalf("virtual recipient: %v", err)
	}
	many := make([]string, 257)
	for i := range many {
		many[i] = bob
	}
	if _, err := c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "x", Visibility: rcpt, Recipients: many}); code(err) != codes.ResourceExhausted {
		t.Fatalf("257 recipients: %v", err)
	}
	if _, err := c.SetVariables(ctx, &pb.SetVariablesRequest{Ref: np, Owner: alice, Writer: alice, Items: items(1, 1), Visibility: rcpt}); code(err) != codes.InvalidArgument {
		t.Fatalf("recipients on a variable: %v", err)
	}
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "terraria", DefaultVisibility: rcpt}); code(err) != codes.InvalidArgument {
		t.Fatalf("recipients as a title default: %v", err)
	}
}

func TestRecipientsErasure(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	rcpt := pb.Visibility_VISIBILITY_RECIPIENTS
	a := must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: alice, Writer: alice, Name: "a", Visibility: rcpt, Recipients: []string{dave, bob}}))
	must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: carol, Writer: carol, Name: "c", Visibility: rcpt, Recipients: []string{dave}}))
	must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: dave, Writer: dave, Name: "d", Visibility: rcpt, Recipients: []string{alice}}))

	req, _ := http.NewRequest("DELETE", e.http.URL+"/internal/accounts/"+dave+"/data", nil)
	req.Header.Set("X-Internal-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("erase: %v %v", resp, err)
	}
	var out map[string]int64
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out["deleted"] != 1 || out["recipient_of"] != 2 {
		t.Fatalf("erase answer: %v", out)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: alice, Id: a.Object.Id})); !slices.Equal(g.Object.GetRecipients(), []string{bob}) {
		t.Fatalf("dave still listed: %v", g.Object.GetRecipients())
	}
	if g := must(c.GetObject(ctx, &pb.GetObjectRequest{Ref: np, Viewer: dave, Owner: dave, Name: "d"})); g.Found {
		t.Fatal("dave's own object survived")
	}
	var left int
	if err := e.st.DB.QueryRow(ctx, `SELECT count(*) FROM objects WHERE $1 = ANY(recipients)`, dave).Scan(&left); err != nil || left != 0 {
		t.Fatalf("rows still naming dave: %d %v", left, err)
	}
	// A repeat is a 200 with nothing left to do.
	resp, _ = http.DefaultClient.Do(req)
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out["deleted"] != 0 || out["recipient_of"] != 0 {
		t.Fatalf("repeat erase: %v", out)
	}
}

// An upload carries its recipients to the object it becomes.
func TestRecipientsUpload(t *testing.T) {
	srv := httptest.NewServer(newFakeS3())
	t.Cleanup(srv.Close)
	e := blobSetup(t, blobs.Config{Endpoint: srv.URL, Bucket: "title-storage", AccessKey: "k", SecretKey: "s"})
	c, ctx := e.ts, e.ctx
	body := []byte("ghost")
	p := must(c.PrepareUpload(ctx, &pb.PrepareUploadRequest{Ref: np, Owner: alice, Writer: alice, Name: "g", Size: uint64(len(body)),
		Visibility: pb.Visibility_VISIBILITY_RECIPIENTS, Recipients: []string{dave}}))
	send(t, p, body)
	o := must(c.CompleteUpload(ctx, &pb.CompleteUploadRequest{Id: p.Id, Writer: alice})).Object
	if o.Visibility != pb.Visibility_VISIBILITY_RECIPIENTS || !slices.Equal(o.Recipients, []string{dave}) {
		t.Fatalf("completed: %+v", o)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: dave, Id: p.Id})); !g.Found || g.Object.GetUrl == "" {
		t.Fatalf("recipient read: %+v", g)
	}
	if g := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: p.Id})); g.Found {
		t.Fatal("non-recipient read the upload")
	}
}
