package api_test

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

// TestSearch covers the v0.4.0 ListObjects filters (NEX DataStoreSearchParam): every-owner scope
// and exclusions, created/updated ranges, numeric metadata ranges, collection lists, and the
// rating sort's refusal before the ratings tier.
func TestSearch(t *testing.T) {
	e := setup(t)
	c, ctx := e.ts, e.ctx
	if _, err := c.UpsertTitle(ctx, &pb.UpsertTitleRequest{Title: "terraria", QuotaObjects: 50, QuotaBytes: 50000}); err != nil {
		t.Fatal(err)
	}
	put := func(owner, coll, name, meta string, vis pb.Visibility) *pb.Object {
		return must(c.PutObject(ctx, &pb.PutObjectRequest{Ref: np, Owner: owner, Writer: owner, Collection: coll, Name: name, MetaJson: meta, Visibility: vis})).Object
	}
	pub, fr := pb.Visibility_VISIBILITY_PUBLIC, pb.Visibility_VISIBILITY_FRIENDS
	a1 := put(alice, "course", "a1", `{"data_type":1,"plays":5}`, pub)
	put(bob, "course", "b1", `{"data_type":2,"plays":50}`, pub)
	put(carol, "course", "c1", `{"data_type":3,"plays":"many"}`, fr) // a string, never in a numeric range
	put(dave, "course", "d1", `{"data_type":9,"plays":9007199254740993}`, pub)
	put(alice, "photo", "p1", `{"data_type":1}`, pub)
	// Spread the created times so the ranges have something to cut.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, n := range []string{"a1", "b1", "c1", "d1", "p1"} {
		at := base.Add(time.Duration(i) * time.Hour)
		if _, err := e.st.DB.Exec(ctx, `UPDATE objects SET created = $1, updated = $1 WHERE name = $2`, at, n); err != nil {
			t.Fatal(err)
		}
	}
	list := func(req *pb.ListObjectsRequest) *pb.ListObjectsResponse {
		req.Ref = np
		return must(c.ListObjects(ctx, req))
	}
	names := func(l *pb.ListObjectsResponse) string {
		s := ""
		for _, o := range l.Objects {
			s += o.Name + ","
		}
		return s
	}

	// Every owner: alice sees carol's friends-only course (they are friends); dave does not.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, Collection: "course", Sort: "created", Ascending: true}); names(l) != "a1,b1,c1,d1," || l.Hits != 4 {
		t.Fatalf("all owners (alice): %s %d", names(l), l.Hits)
	}
	if l := list(&pb.ListObjectsRequest{Viewer: dave, AllOwners: true, Collection: "course", Sort: "created", Ascending: true}); names(l) != "a1,b1,d1," {
		t.Fatalf("all owners (dave): %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{AllOwners: true, Collection: "course", Sort: "created", Ascending: true}); names(l) != "a1,b1,d1," {
		t.Fatalf("all owners (anonymous): %s", names(l))
	}
	// Anybody except the given owners.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, ExcludeOwners: []string{alice, carol}, Collection: "course", Sort: "created", Ascending: true}); names(l) != "b1,d1," {
		t.Fatalf("exclude: %s", names(l))
	}
	// Created range, inclusive at both ends.
	from, to := base.Add(time.Hour).UnixMilli(), base.Add(3*time.Hour).UnixMilli()
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, Collection: "course", CreatedAfterMs: from, CreatedBeforeMs: to, Sort: "created", Ascending: true}); names(l) != "b1,c1,d1," {
		t.Fatalf("created range: %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, UpdatedBeforeMs: base.UnixMilli()}); names(l) != "a1," {
		t.Fatalf("updated before: %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, UpdatedAfterMs: base.Add(4 * time.Hour).UnixMilli()}); names(l) != "p1," {
		t.Fatalf("updated after: %s", names(l))
	}
	// Numeric meta ranges: non-numbers never match, big integers compare exactly.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, MetaRangeJson: `{"plays":{"ge":5,"le":50}}`, Sort: "created", Ascending: true}); names(l) != "a1,b1," {
		t.Fatalf("meta range: %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, MetaRangeJson: `{"plays":{"ge":9007199254740993}}`}); names(l) != "d1," {
		t.Fatalf("meta range big int: %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, MetaRangeJson: `{"data_type":{"le":2},"plays":{}}`, Sort: "created", Ascending: true}); names(l) != "a1,b1," {
		t.Fatalf("meta range two keys: %s", names(l))
	}
	// Combined with the containment filter and an owner scope.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, Owners: []string{alice}, MetaFilterJson: `{"data_type":1}`, MetaRangeJson: `{"data_type":{"ge":1}}`, Sort: "name"}); names(l) != "p1,a1," {
		t.Fatalf("filter + range + owners: %s", names(l))
	}
	// Collection lists (NEX DataTypes), with the single collection added in.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, Collection: "photo", Collections: []string{"none"}}); names(l) != "p1," {
		t.Fatalf("collections: %s", names(l))
	}
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, Collections: []string{"photo", "course"}}); l.Hits != 5 {
		t.Fatalf("collections hits: %d", l.Hits)
	}
	// Paging keeps the hit count of the whole match.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, AllOwners: true, Collection: "course", Sort: "created", Ascending: true, Offset: 1, Limit: 2}); names(l) != "b1,c1," || l.Hits != 4 {
		t.Fatalf("paging: %s %d", names(l), l.Hits)
	}
	// Refusals.
	for _, req := range []*pb.ListObjectsRequest{
		{Viewer: alice, AllOwners: true, Owners: []string{bob}},
		{Viewer: alice, AllOwners: true, FriendsOfViewer: true},
		{Viewer: alice, AllOwners: true, MetaRangeJson: `{"plays":{"gt":1}}`},
		{Viewer: alice, AllOwners: true, MetaRangeJson: `{"plays":{"ge":"1"}}`},
		{Viewer: alice, AllOwners: true, ExcludeOwners: []string{"not-an-account"}},
		{Viewer: alice, AllOwners: true, Collections: []string{"bad coll"}},
		{Viewer: alice, AllOwners: true, Sort: "bogus"},
		{Viewer: alice, AllOwners: true, Sort: "rating:0"}, // the ratings tier is v0.5.0
	} {
		req.Ref = np
		if _, err := c.ListObjects(ctx, req); code(err) != codes.InvalidArgument {
			t.Errorf("%+v: %v", req, err)
		}
	}
	// Existing calls are unchanged: an owner scope without the new fields.
	if l := list(&pb.ListObjectsRequest{Viewer: alice, Owners: []string{alice}, Collection: "course"}); l.Hits != 1 || l.Objects[0].Id != a1.Id {
		t.Fatalf("plain list: %+v", l)
	}
}
