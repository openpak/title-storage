package api_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"google.golang.org/grpc/codes"

	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

// TestRatings covers the v0.5.0 ratings tier: slot policies (initial value, lock, range, flags),
// one row per rater, visibility, rating sorts and filters, reset, cascade on delete and erasure of
// both sides (ratings an account made, ratings on its objects).
func TestRatings(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "terraria", QuotaObjects: 50, QuotaBytes: 50000}); err != nil {
		t.Fatal(err)
	}
	put := func(owner, name string, vis pb.Visibility) uint64 {
		return must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: owner, Writer: owner, Collection: "course", Name: name, Visibility: vis})).Object.Id
	}
	pub := pb.Visibility_VISIBILITY_PUBLIC
	a, cc, priv := put(alice, "a", pub), put(carol, "c", pub), put(alice, "secret", pb.Visibility_VISIBILITY_PRIVATE)
	rate := func(id uint64, slot int32, rater string, v int64) *pb.RatingResponse {
		return must(c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: id, Slot: slot, Rater: rater, Value: v}))
	}

	// InitRating: the owner (or anyone the write rule allows); the first policy wins.
	init := must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: alice, Slot: 0, Policy: &pb.RatingPolicy{InitialValue: 100}}))
	if !init.Found || init.Rating.Total != 100 || init.Rating.Count != 0 || init.Rating.Policy.Lock != "none" {
		t.Fatalf("init: %+v", init)
	}
	if again := must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: alice, Slot: 0, Policy: &pb.RatingPolicy{InitialValue: 5}})); again.Rating.Total != 100 {
		t.Fatalf("second init replaced the first: %+v", again)
	}
	if _, err := c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: dave, Slot: 1}); code(err) != codes.PermissionDenied {
		t.Fatalf("blocked writer init: %v", err)
	}
	if r := must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: 999999, Writer: alice, Slot: 0})); r.Found {
		t.Fatal("init on a missing object")
	}
	for _, p := range []*pb.RatingPolicy{{Lock: "daily"}, {Lock: "interval"}, {Lock: "period", PeriodDay: 3}} {
		if _, err := c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: alice, Slot: 9, Policy: p}); code(err) != codes.InvalidArgument {
			t.Fatalf("bad policy %+v: %v", p, err)
		}
	}

	// No lock: every call is one more rating; the rater's row sums what it gave.
	rate(a, 0, bob, 3)
	r := rate(a, 0, bob, 4)
	if r.Locked || r.Rating.Total != 107 || r.Rating.Count != 2 || r.Rating.RaterValue != 7 || r.Rating.RaterCount != 2 || r.Rating.LockExpiresUnixMs != 0 {
		t.Fatalf("free slot: %+v", r)
	}
	// A slot nobody created: InvalidArgument without a policy, created by the first rating with one.
	if _, err := c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: a, Slot: 1, Rater: bob, Value: 1}); code(err) != codes.InvalidArgument {
		t.Fatalf("missing slot: %v", err)
	}
	perm := &pb.RatingPolicy{Lock: "permanent"}
	r = must(c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: a, Slot: 1, Rater: bob, Value: 1, Policy: perm}))
	if r.Rating.Total != 1 || r.Rating.Policy.Lock != "permanent" || r.Rating.LockExpiresUnixMs != -1 {
		t.Fatalf("permanent first: %+v", r)
	}
	// Once per rater: the second is locked and changes nothing; another rater still may.
	if r = rate(a, 1, bob, 1); !r.Locked || r.Rating.Total != 1 || r.Rating.Count != 1 || r.Rating.LockExpiresUnixMs != -1 {
		t.Fatalf("permanent lock: %+v", r)
	}
	if r = rate(a, 1, carol, 1); r.Locked || r.Rating.Total != 2 {
		t.Fatalf("other rater: %+v", r)
	}
	// Interval lock: locked until the interval has passed.
	must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: alice, Slot: 2, Policy: &pb.RatingPolicy{Lock: "interval", IntervalSeconds: 3600}}))
	r = rate(a, 2, bob, 1)
	if r = rate(a, 2, bob, 1); !r.Locked || r.Rating.LockExpiresUnixMs <= r.Rating.RatedUnixMs {
		t.Fatalf("interval lock: %+v", r)
	}
	if _, err := e.st.DB.Exec(ctx, `UPDATE ratings SET rated = rated - interval '2 hours' WHERE slot = 2`); err != nil {
		t.Fatal(err)
	}
	if r = rate(a, 2, bob, 1); r.Locked || r.Rating.Total != 2 {
		t.Fatalf("interval expired: %+v", r)
	}
	// Modifiable: a repeat replaces the rater's value and stays one rating; range with round-minus.
	lo, hi := int64(0), int64(5)
	must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: a, Writer: alice, Slot: 3,
		Policy: &pb.RatingPolicy{Modifiable: true, RoundMinus: true, RangeMin: &lo, RangeMax: &hi, DisableSelfRating: true}}))
	rate(a, 3, bob, 5)
	if r = rate(a, 3, bob, 2); r.Rating.Total != 2 || r.Rating.Count != 1 || r.Rating.RaterValue != 2 || r.Rating.RaterCount != 1 {
		t.Fatalf("modifiable: %+v", r)
	}
	if r = rate(a, 3, carol, -4); r.Rating.Total != 2 || r.Rating.Count != 2 { // -4 counts as 0
		t.Fatalf("round minus: %+v", r)
	}
	if _, err := c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: a, Slot: 3, Rater: carol, Value: 6}); code(err) != codes.InvalidArgument {
		t.Fatalf("range: %v", err)
	}
	if _, err := c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: a, Slot: 3, Rater: alice, Value: 1}); code(err) != codes.PermissionDenied {
		t.Fatalf("self rating: %v", err)
	}
	// Visibility: a stranger cannot rate (or see) a private object.
	if _, err := c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: priv, Writer: alice, Slot: 0}); err != nil {
		t.Fatal(err)
	}
	if r = rate(priv, 0, dave, 1); r.Found {
		t.Fatalf("stranger rated a private object: %+v", r)
	}
	if _, err := c.RateObject(ctx, &pb.RateObjectRequest{Network: "np", Id: a, Slot: 0, Rater: "v:someone", Value: 1}); code(err) != codes.InvalidArgument {
		t.Fatalf("virtual rater: %v", err)
	}

	// GetRatings: visible objects only, every slot or some, with a rater's rows.
	g := must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Viewer: dave, Ids: []uint64{a, priv}, Rater: bob}))
	if len(g.Ratings) != 4 || g.Ratings[0].Id != a || g.Ratings[0].Slot != 0 || !g.Ratings[0].Rated || g.Ratings[0].RaterValue != 7 ||
		g.Ratings[1].LockExpiresUnixMs != -1 || g.Ratings[2].LockExpiresUnixMs <= 0 {
		t.Fatalf("get ratings (dave, rater bob): %+v", g.Ratings)
	}
	if g = must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Viewer: alice, Ids: []uint64{a, priv}, Slots: []int32{0}})); len(g.Ratings) != 2 || g.Ratings[1].Id != priv || g.Ratings[0].Rated {
		t.Fatalf("get ratings (alice, slot 0): %+v", g.Ratings)
	}
	if _, err := c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Viewer: alice}); code(err) != codes.InvalidArgument {
		t.Fatalf("no ids: %v", err)
	}

	// Rating sorts: by total (objects without the slot last), by average, at least N ratings.
	must(c.InitRating(ctx, &pb.InitRatingRequest{Network: "np", Id: cc, Writer: carol, Slot: 0, Policy: &pb.RatingPolicy{}}))
	for _, v := range []int64{50, 60, 70} {
		rate(cc, 0, bob, v) // total 180 over 3: average 60; a: total 107 over 2 from 100: average 3.5
	}
	b := put(bob, "b", pub) // no slot 0 at all
	list := func(req *pb.ListObjectsRequest) string {
		req.Ref, req.Viewer, req.AllOwners, req.Collection = np, alice, true, "course"
		s := ""
		for _, o := range must(c.ListObjects(ctx, req)).Objects {
			s += o.Name + ","
		}
		return s
	}
	if got := list(&pb.ListObjectsRequest{Sort: "rating:0"}); got != "c,a,secret,b," {
		t.Fatalf("rating sort: %s", got)
	}
	if got := list(&pb.ListObjectsRequest{Sort: "rating:0", Ascending: true}); got != "secret,a,c,b," {
		t.Fatalf("rating sort ascending: %s", got)
	}
	if got := list(&pb.ListObjectsRequest{Sort: "rating_avg:0"}); got != "c,a,secret,b," {
		t.Fatalf("average sort: %s", got)
	}
	if got := list(&pb.ListObjectsRequest{Sort: "rating:0", MinRatingCount: 3}); got != "c," {
		t.Fatalf("min ratings: %s", got)
	}
	l := must(c.ListObjects(ctx, &pb.ListObjectsRequest{Ref: np, Viewer: alice, Owners: []string{alice}, Names: []string{"a"}, IncludeRatings: true}))
	if len(l.Objects) != 1 || len(l.Objects[0].Ratings) != 4 || l.Objects[0].Ratings[0].Total != 107 {
		t.Fatalf("include ratings: %+v", l.Objects)
	}
	if o := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: cc, IncludeRatings: true})); len(o.Object.Ratings) != 1 || o.Object.Ratings[0].Total != 180 {
		t.Fatalf("by id with ratings: %+v", o.Object)
	}
	if o := must(c.GetObjectByID(ctx, &pb.GetObjectByIDRequest{Network: "np", Viewer: bob, Id: b})); len(o.Object.Ratings) != 0 {
		t.Fatalf("ratings without asking: %+v", o.Object)
	}

	// Reset: back to the initial value, no raters (owner side; a blocked writer may not).
	if _, err := c.ResetRatings(ctx, &pb.ResetRatingsRequest{Network: "np", Id: a, Writer: dave, Slots: []int32{2}}); code(err) != codes.PermissionDenied {
		t.Fatalf("blocked reset: %v", err)
	}
	if d := must(c.ResetRatings(ctx, &pb.ResetRatingsRequest{Network: "np", Id: a, Writer: alice, Slots: []int32{2}})); d.Deleted != 1 {
		t.Fatalf("reset removed %d", d.Deleted)
	}
	if g = must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Ids: []uint64{a}, Slots: []int32{2}, Rater: bob})); g.Ratings[0].Total != 0 || g.Ratings[0].Count != 0 || g.Ratings[0].Rated {
		t.Fatalf("after reset: %+v", g.Ratings)
	}

	// Erasure of a rater: bob's ratings go and come back out of the totals (carol's object too).
	before := ratingRows(t, e, `rater = '`+bob+`'`)
	erase := func(acct string) map[string]int64 {
		req, _ := http.NewRequest("DELETE", e.http.URL+"/internal/accounts/"+acct+"/data", nil)
		req.Header.Set("X-Internal-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("erase %s: %v %v", acct, resp, err)
		}
		defer resp.Body.Close()
		var out map[string]int64
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	out := erase(bob)
	if out["ratings_made"] != before || before != 4 || out["deleted"] != 1 { // a/0, a/1, a/3, c/0 (a/2 was reset); and bob's object b
		t.Fatalf("erase bob: %v (rows before %d)", out, before)
	}
	g = must(c.GetRatings(ctx, &pb.GetRatingsRequest{Network: "np", Ids: []uint64{a, cc}, Slots: []int32{0, 1, 3}}))
	want := map[[2]int64][2]int64{{int64(a), 0}: {100, 0}, {int64(a), 1}: {1, 1}, {int64(a), 3}: {0, 1}, {int64(cc), 0}: {0, 0}}
	for _, x := range g.Ratings {
		if w := want[[2]int64{int64(x.Id), int64(x.Slot)}]; x.Total != w[0] || int64(x.Count) != w[1] {
			t.Errorf("after erasing bob: %d/%d = %d over %d, want %v", x.Id, x.Slot, x.Total, x.Count, w)
		}
	}
	// Erasure of an owner: the ratings others gave its objects go with the objects.
	if n := ratingRows(t, e, `object_id IN (`+itoa(a)+`,`+itoa(priv)+`)`); n != 2 { // carol's a/1 and a/3
		t.Fatalf("ratings on alice's objects: %d", n)
	}
	out = erase(alice)
	if out["ratings_received"] != 2 || out["ratings_made"] != 0 || out["deleted"] != 2 {
		t.Fatalf("erase alice: %v", out)
	}
	var slots int64
	if err := e.st.DB.QueryRow(ctx, `SELECT count(*) FROM rating_slots WHERE object_id IN ($1, $2)`, int64(a), int64(priv)).Scan(&slots); err != nil || slots != 0 {
		t.Fatalf("rating slots of erased objects: %d %v", slots, err)
	}
	// Deleting an object deletes its ratings.
	rate(cc, 0, dave, 1)
	must(c.DeleteObjects(ctx, &pb.DeleteObjectsRequest{Ref: np, Owner: carol, Writer: carol, Collection: "course"}))
	if n := ratingRows(t, e, `TRUE`); n != 0 {
		t.Fatalf("ratings left after delete: %d", n)
	}
}

func ratingRows(t *testing.T, e *env, where string) int64 {
	t.Helper()
	var n int64
	if err := e.st.DB.QueryRow(e.ctx, `SELECT count(*) FROM ratings WHERE `+where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }
