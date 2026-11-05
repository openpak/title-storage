// Package service is the title storage's rules on top of the store: title resolution, input
// limits, who may read and write which owner's rows, and the TUS operator semantics. The gRPC
// and HTTP surfaces are thin maps onto it.
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openpak/title-storage/internal/blobs"
	"github.com/openpak/title-storage/internal/store"
)

const (
	MaxInfo      = 4096
	MaxMeta      = 16 << 10
	MaxName      = 256
	MaxItems     = 1024
	MaxOwners    = 1000
	DefaultLimit = 100
	MaxLimit     = 1000
	// MaxRecipients caps an object's recipient list (NEX DataStore sends a handful).
	MaxRecipients = 256
)

var (
	ErrBadAccount = errors.New("title-storage: owners are account UUIDs or v:<name>")
	ErrBadRequest = errors.New("title-storage: bad request")
	ErrTooLarge   = errors.New("title-storage: value too large")
	ErrForbidden  = errors.New("title-storage: the writer may not write to this owner")
	ErrNeedNet    = errors.New("title-storage: this title is per network: the request must name its network")
	ErrNoBlobs    = errors.New("title-storage: the blob tier is not configured")
	ErrNoUpload   = errors.New("title-storage: no such upload (expired, replaced or already completed)")
	ErrMismatch   = errors.New("title-storage: the uploaded body does not match the prepared upload")
	ErrBlobs      = errors.New("title-storage: blob store unavailable")
	ErrSelfRating = errors.New("title-storage: the owner may not rate this object")
	ErrRange      = errors.New("title-storage: rating value out of the slot's range")
)

var (
	uuidRE    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	virtualRE = regexp.MustCompile(`^v:[A-Za-z0-9_.-]{1,64}$`)
	collRE    = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
)

// Core is the account core as this service uses it: the friend graph (read rules) and the
// interaction check (write rule). Title storage never stores a friendship.
type Core interface {
	Friends(ctx context.Context, account string) ([]string, error)
	CanInteract(ctx context.Context, actor, target string) (bool, error)
}

type Service struct {
	Store *store.Store
	Core  Core
	// Blobs is the blob tier; nil turns PrepareUpload and CompleteUpload into ErrNoBlobs.
	Blobs     *blobs.Store
	URLTTL    time.Duration // presigned PUT/GET lifetime (default 15 minutes)
	UploadTTL time.Duration // how long a prepared upload waits for CompleteUpload (default 1 hour)
}

func (s *Service) urlTTL() time.Duration {
	if s.URLTTL > 0 {
		return s.URLTTL
	}
	return 15 * time.Minute
}

func (s *Service) uploadTTL() time.Duration {
	if s.UploadTTL > 0 {
		return s.UploadTTL
	}
	return time.Hour
}

// Ref names a title as a caller sees it.
type Ref struct{ Network, Title string }

func IsVirtual(owner string) bool { return strings.HasPrefix(owner, "v:") }

func validOwner(o string) error {
	if uuidRE.MatchString(o) || virtualRE.MatchString(o) {
		return nil
	}
	return ErrBadAccount
}

func validAccount(a string) error {
	if !uuidRE.MatchString(a) {
		return ErrBadAccount
	}
	return nil
}

// validVis is a variable's (and a title default's) visibility.
func validVis(v string) error {
	switch v {
	case "", store.Private, store.Friends, store.Public:
		return nil
	}
	return ErrBadRequest
}

// validObjVis is an object's visibility: validVis plus recipients.
func validObjVis(v string) error {
	if v == store.Recipients {
		return nil
	}
	return validVis(v)
}

// cleanRecipients checks a recipient list (account UUIDs, at most MaxRecipients) and returns it
// lowercased without repeats, in the order given.
func cleanRecipients(rs []string) ([]string, error) {
	if len(rs) > MaxRecipients {
		return nil, ErrTooLarge
	}
	out := make([]string, 0, len(rs))
	seen := make(map[string]bool, len(rs))
	for _, r := range rs {
		if err := validAccount(r); err != nil {
			return nil, err
		}
		r = strings.ToLower(r)
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out, nil
}

func clamp(n int) int {
	if n <= 0 {
		return DefaultLimit
	}
	return min(n, MaxLimit)
}

// forWrite resolves the title (creating an unmapped native id) and checks the caller's
// network when the title is per network.
func (s *Service) forWrite(ctx context.Context, r Ref) (store.Title, error) {
	t, err := s.Store.ResolveTitle(ctx, r.Network, r.Title, true)
	if err != nil {
		return t, err
	}
	if !t.Cross && r.Network == "" {
		return t, ErrNeedNet
	}
	return t, nil
}

// forRead resolves the title without creating it. found=false is a title nobody has written
// to: reads of it are empty, not errors.
func (s *Service) forRead(ctx context.Context, r Ref) (t store.Title, found bool, err error) {
	t, err = s.Store.ResolveTitle(ctx, r.Network, r.Title, false)
	if errors.Is(err, store.ErrNoTitle) {
		return t, false, nil
	}
	if err != nil {
		return t, false, err
	}
	if !t.Cross && r.Network == "" {
		return t, false, ErrNeedNet
	}
	return t, true, nil
}

// authorizeWrite: the writer is the owner, the owner is a title's virtual user, or the account
// core lets the writer interact with the owner (no block either way).
func (s *Service) authorizeWrite(ctx context.Context, writer, owner string) error {
	if err := validAccount(writer); err != nil {
		return err
	}
	if err := validOwner(owner); err != nil {
		return err
	}
	if IsVirtual(owner) || strings.EqualFold(writer, owner) {
		return nil
	}
	ok, err := s.Core.CanInteract(ctx, writer, owner)
	if err != nil {
		return err
	}
	if !ok {
		return ErrForbidden
	}
	return nil
}

// reader builds the visibility context. Friends are fetched only when the viewer can meet a
// friends-only row of someone else.
func (s *Service) reader(ctx context.Context, viewer string, owners []string, needFriends bool) (store.Reader, error) {
	if viewer != "" {
		if err := validAccount(viewer); err != nil {
			return store.Reader{}, err
		}
	}
	rd := store.Reader{Viewer: strings.ToLower(viewer)}
	if viewer == "" || !needFriends {
		return rd, nil
	}
	for _, o := range owners {
		if !IsVirtual(o) && !strings.EqualFold(o, viewer) {
			fs, err := s.Core.Friends(ctx, viewer)
			if err != nil {
				return rd, err
			}
			rd.Friends = fs
			break
		}
	}
	return rd, nil
}

// scope turns a request's owner selection into the owner list: the given owners, or the
// viewer's friends (plus the viewer).
func (s *Service) scope(ctx context.Context, viewer string, owners []string, friends, self bool) ([]string, store.Reader, error) {
	if len(owners) > MaxOwners {
		return nil, store.Reader{}, ErrTooLarge
	}
	if friends {
		if err := validAccount(viewer); err != nil {
			return nil, store.Reader{}, err
		}
		fs, err := s.Core.Friends(ctx, viewer)
		if err != nil {
			return nil, store.Reader{}, err
		}
		out := append([]string(nil), fs...)
		if self {
			out = append(out, strings.ToLower(viewer))
		}
		if len(out) > 20000 {
			out = out[:20000]
		}
		return out, store.Reader{Viewer: strings.ToLower(viewer), Friends: fs}, nil
	}
	if len(owners) == 0 {
		return nil, store.Reader{}, ErrBadRequest
	}
	for i, o := range owners {
		if err := validOwner(o); err != nil {
			return nil, store.Reader{}, err
		}
		owners[i] = strings.ToLower(o)
	}
	rd, err := s.reader(ctx, viewer, owners, true)
	return owners, rd, err
}

// Access is what the read rule needs of an object.
type Access struct {
	Owner, Visibility string
	Recipients        []string
	RecipientsFriends bool
}

// Readable is the object read rule (the Go twin of the store's SQL predicate): public and virtual
// owners for anyone; otherwise a signed-in viewer that is the owner, a friend of the owner on a
// friends row, or on a recipients row a named recipient or (with RecipientsFriends) a friend.
// isFriend is asked only when the answer depends on it.
func Readable(viewer string, a Access, isFriend func() (bool, error)) (bool, error) {
	if a.Visibility == store.Public || IsVirtual(a.Owner) {
		return true, nil
	}
	if viewer == "" {
		return false, nil
	}
	if strings.EqualFold(viewer, a.Owner) {
		return true, nil
	}
	switch a.Visibility {
	case store.Friends:
		return isFriend()
	case store.Recipients:
		for _, r := range a.Recipients {
			if strings.EqualFold(r, viewer) {
				return true, nil
			}
		}
		if a.RecipientsFriends {
			return isFriend()
		}
	}
	return false, nil
}

// canRead applies the read rule to one object.
func (s *Service) canRead(ctx context.Context, viewer string, o *store.Object) (bool, error) {
	return Readable(viewer, Access{Owner: o.Owner, Visibility: o.Visibility, Recipients: o.Recipients, RecipientsFriends: o.RecipientsFriends},
		func() (bool, error) {
			fs, err := s.Core.Friends(ctx, viewer)
			if err != nil {
				return false, err
			}
			for _, f := range fs {
				if strings.EqualFold(f, o.Owner) {
					return true, nil
				}
			}
			return false, nil
		})
}

// ---- variables ----

func (s *Service) SetVariables(ctx context.Context, r Ref, owner, writer, setter, vis string, items map[int32]int64) error {
	if len(items) == 0 || len(items) > MaxItems || validVis(vis) != nil {
		return ErrBadRequest
	}
	if err := s.authorizeWrite(ctx, writer, owner); err != nil {
		return err
	}
	t, err := s.forWrite(ctx, r)
	if err != nil {
		return err
	}
	for slot := range items {
		if slot < 0 {
			return ErrBadRequest
		}
	}
	return s.Store.SetVariables(ctx, store.VarWrite{T: t, Network: r.Network, Owner: strings.ToLower(owner), Setter: setter, Vis: vis}, items)
}

type Query struct {
	Viewer    string
	Owners    []string
	Friends   bool
	Self      bool
	Slots     []int32
	Sort      string
	Ascending bool
	Offset    int
	Limit     int
}

func (s *Service) ListVariables(ctx context.Context, r Ref, q Query) ([]store.Variable, int, error) {
	owners, rd, err := s.scope(ctx, q.Viewer, q.Owners, q.Friends, q.Self)
	if err != nil {
		return nil, 0, err
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return nil, 0, err
	}
	return s.Store.ListVariables(ctx, store.VarQuery{T: t, Network: r.Network, Reader: rd, Owners: owners, Slots: q.Slots,
		Sort: q.Sort, Ascending: q.Ascending, Offset: max(q.Offset, 0), Limit: clamp(q.Limit)})
}

// Condition is a TUS comp-date / comp-setter check against the existing row.
type Condition struct {
	Updated *time.Time
	Setter  *string
}

func (c Condition) none() bool { return c.Updated == nil && c.Setter == nil }

// holds reports whether the row (nil when missing) satisfies the condition.
func (c Condition) holds(updated *time.Time, setter string) bool {
	if c.Updated != nil && (updated == nil || !updated.Equal(*c.Updated)) {
		return false
	}
	return c.Setter == nil || (updated != nil && *c.Setter == setter)
}

type VarResult struct {
	Var      store.Variable
	Exists   bool
	Stored   bool
	CondFail bool
	HadPrev  bool
	Previous int64
}

func (s *Service) writeVar(ctx context.Context, r Ref, owner, writer, setter, vis string, slot int32) (store.VarWrite, error) {
	if slot < 0 || validVis(vis) != nil {
		return store.VarWrite{}, ErrBadRequest
	}
	if err := s.authorizeWrite(ctx, writer, owner); err != nil {
		return store.VarWrite{}, err
	}
	t, err := s.forWrite(ctx, r)
	if err != nil {
		return store.VarWrite{}, err
	}
	return store.VarWrite{T: t, Network: r.Network, Owner: strings.ToLower(owner), Setter: setter, Vis: vis}, nil
}

// AddVariable adds delta (a missing slot counts as 0). With a condition the row is locked and
// checked first.
func (s *Service) AddVariable(ctx context.Context, r Ref, owner, writer, setter, vis string, slot int32, delta int64, c Condition) (VarResult, error) {
	w, err := s.writeVar(ctx, r, owner, writer, setter, vis, slot)
	if err != nil {
		return VarResult{}, err
	}
	if c.none() {
		v, err := s.Store.AddVariable(ctx, w, slot, delta)
		if err != nil {
			return VarResult{}, err
		}
		return VarResult{Var: v, Exists: true, Stored: true}, nil
	}
	return s.update(ctx, w, slot, c, func(cur int64) (int64, bool) { return cur + delta, true })
}

// TrySetVariable stores value when "operand <op> current" holds.
func (s *Service) TrySetVariable(ctx context.Context, r Ref, owner, writer, setter, vis string, slot int32, value int64, op string, operand int64, c Condition) (VarResult, error) {
	switch op {
	case "eq", "ne", "gt", "ge", "lt", "le":
	default:
		return VarResult{}, ErrBadRequest
	}
	w, err := s.writeVar(ctx, r, owner, writer, setter, vis, slot)
	if err != nil {
		return VarResult{}, err
	}
	return s.update(ctx, w, slot, c, func(cur int64) (int64, bool) { return value, Compare(op, operand, cur) })
}

func (s *Service) update(ctx context.Context, w store.VarWrite, slot int32, c Condition, fn func(cur int64) (int64, bool)) (VarResult, error) {
	var res VarResult
	next, err := s.Store.UpdateVariable(ctx, w, slot, func(cur *store.Variable) (int64, bool) {
		var when *time.Time
		setter := ""
		if cur != nil {
			when, setter = &cur.Updated, cur.Setter
			res.HadPrev, res.Previous = true, cur.Value
		}
		if !c.holds(when, setter) {
			res.CondFail = true
			return 0, false
		}
		v, ok := fn(res.Previous)
		res.Stored = ok
		return v, ok
	})
	if err != nil {
		return res, err
	}
	if next != nil {
		res.Var, res.Exists = *next, true
	}
	return res, nil
}

// Compare is the TUS operator: "a <op> b".
func Compare(op string, a, b int64) bool {
	switch op {
	case "eq":
		return a == b
	case "ne":
		return a != b
	case "gt":
		return a > b
	case "ge":
		return a >= b
	case "lt":
		return a < b
	case "le":
		return a <= b
	}
	return false
}

func (s *Service) DeleteVariables(ctx context.Context, r Ref, owner, writer string, slots []int32) (int64, error) {
	if len(slots) == 0 || len(slots) > MaxItems {
		return 0, ErrBadRequest
	}
	if err := s.authorizeWrite(ctx, writer, owner); err != nil {
		return 0, err
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return 0, err
	}
	return s.Store.DeleteVariables(ctx, t, r.Network, strings.ToLower(owner), slots)
}

// ---- objects ----

type Put struct {
	Owner, Writer, Setter string
	Collection, Name      string
	Info                  []byte
	Meta                  string
	Body                  []byte
	Vis                   string
	Recipients            []string
	RecipientsFriends     bool
	TTL                   time.Duration
	Cond                  Condition
}

func (s *Service) PutObject(ctx context.Context, r Ref, p Put) (store.Object, bool, error) {
	if p.Collection == "" {
		p.Collection = "default"
	}
	if !collRE.MatchString(p.Collection) || len(p.Name) > MaxName || validObjVis(p.Vis) != nil || !store.ValidJSONObject(p.Meta) {
		return store.Object{}, false, ErrBadRequest
	}
	recips, err := cleanRecipients(p.Recipients)
	if err != nil {
		return store.Object{}, false, err
	}
	if len(p.Info) > MaxInfo || len(p.Meta) > MaxMeta {
		return store.Object{}, false, ErrTooLarge
	}
	if err := s.authorizeWrite(ctx, p.Writer, p.Owner); err != nil {
		return store.Object{}, false, err
	}
	t, err := s.forWrite(ctx, r)
	if err != nil {
		return store.Object{}, false, err
	}
	if len(p.Body) > t.MaxObjectBytes {
		return store.Object{}, false, ErrTooLarge
	}
	put := store.ObjPut{T: t, Network: r.Network, Owner: strings.ToLower(p.Owner), Setter: p.Setter, Collection: p.Collection, Name: p.Name,
		Info: p.Info, Meta: p.Meta, Body: p.Body, Vis: p.Vis, Recipients: recips, RecipientsFriends: p.RecipientsFriends, TTL: p.TTL}
	if !p.Cond.none() {
		put.Cond = func(cur *store.Object) bool {
			if cur == nil {
				return p.Cond.holds(nil, "")
			}
			return p.Cond.holds(&cur.Updated, cur.Setter)
		}
	}
	o, failed, err := s.Store.PutObject(ctx, put)
	if err == nil && !failed {
		s.drainQuiet(ctx, put.Owner)
	}
	return o, failed, err
}

// GetObject returns the object (with owner empty: the newest of that name under any owner, for
// unguessable names) when it exists and the viewer may read it (nil otherwise: the
// same answer for both, so private rows cannot be probed).
func (s *Service) GetObject(ctx context.Context, r Ref, viewer, owner, collection, name string, body bool) (*store.Object, error) {
	if owner != "" {
		if err := validOwner(owner); err != nil {
			return nil, err
		}
	} else if name == "" {
		return nil, ErrBadRequest
	}
	if viewer != "" {
		if err := validAccount(viewer); err != nil {
			return nil, err
		}
	}
	if collection == "" {
		collection = "default"
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return nil, err
	}
	var o *store.Object
	if owner == "" {
		o, err = s.Store.FindObject(ctx, t, r.Network, collection, name, body)
	} else {
		o, err = s.Store.GetObject(ctx, t, r.Network, strings.ToLower(owner), collection, name, body)
	}
	if err != nil || o == nil {
		return nil, err
	}
	ok, err := s.canRead(ctx, viewer, o)
	if err != nil || !ok {
		return nil, err
	}
	return o, s.withURL(ctx, o)
}

// GetObjectByID is GetObject for a caller that holds the id. The id is global, so a title that
// is per network hides objects written by other networks only when the caller names its own.
func (s *Service) GetObjectByID(ctx context.Context, network, viewer string, id int64, body, ratings bool) (*store.Object, error) {
	if viewer != "" {
		if err := validAccount(viewer); err != nil {
			return nil, err
		}
	}
	o, ns, err := s.Store.ObjectByID(ctx, id, body)
	if err != nil || o == nil {
		return nil, err
	}
	if ns != "" && ns != network {
		return nil, nil
	}
	ok, err := s.canRead(ctx, viewer, o)
	if err != nil || !ok {
		return nil, err
	}
	if ratings {
		one := []store.Object{*o}
		if err := s.attachRatings(ctx, one); err != nil {
			return nil, err
		}
		o = &one[0]
	}
	return o, s.withURL(ctx, o)
}

type ObjQuery struct {
	Query
	AllOwners     bool
	ExcludeOwners []string
	Collection    string
	Collections   []string
	Names         []string
	MetaFilter    string
	MetaRange     string // {"key": {"ge": n, "le": m}, ...}
	CreatedAfter  int64  // Unix ms, 0 = unbounded
	CreatedBefore int64
	UpdatedAfter  int64
	UpdatedBefore int64
	MinRatings    uint32 // with a rating sort
	Received      bool   // only objects whose recipients name the viewer
	Body          bool
	Ratings       bool // attach every rating slot (Object.Ratings)
}

const MaxMetaRanges = 8

var (
	metaKeyRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	numberRE  = regexp.MustCompile(`^-?(0|[1-9][0-9]{0,39})(\.[0-9]{1,20})?([eE][-+]?[0-9]{1,3})?$`)
)

// ParseMetaRange reads meta_range_json: top-level keys to {"ge": n, "le": m} (either bound may be
// left out). Bounds stay decimal strings so int64 values compare exactly.
func ParseMetaRange(js string) ([]store.MetaRange, error) {
	if strings.TrimSpace(js) == "" {
		return nil, nil
	}
	var m map[string]map[string]json.RawMessage
	d := json.NewDecoder(strings.NewReader(js))
	if err := d.Decode(&m); err != nil || d.More() {
		return nil, ErrBadRequest
	}
	if len(m) > MaxMetaRanges {
		return nil, ErrTooLarge
	}
	out := make([]store.MetaRange, 0, len(m))
	for k, b := range m {
		if !metaKeyRE.MatchString(k) {
			return nil, ErrBadRequest
		}
		r := store.MetaRange{Key: k}
		for op, n := range b {
			// A JSON number literal only (a quoted "1" is refused).
			if !numberRE.Match(n) {
				return nil, ErrBadRequest
			}
			switch op {
			case "ge":
				r.Min = string(n)
			case "le":
				r.Max = string(n)
			default:
				return nil, ErrBadRequest
			}
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func msTime(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// objSort checks a ListObjects sort; a rating sort ("rating:<slot>", "rating_avg:<slot>") returns
// its slot.
func objSort(s string) (slot *int32, avg bool, err error) {
	switch s {
	case "", "updated", "created", "size", "name", "id":
		return nil, false, nil
	}
	n, ok := strings.CutPrefix(s, "rating:")
	if !ok {
		if n, ok = strings.CutPrefix(s, "rating_avg:"); !ok {
			return nil, false, ErrBadRequest
		}
		avg = true
	}
	v, err := strconv.ParseInt(n, 10, 32)
	if err != nil || !validSlot(int32(v)) {
		return nil, false, ErrBadRequest
	}
	sl := int32(v)
	return &sl, avg, nil
}

func (s *Service) ListObjects(ctx context.Context, r Ref, q ObjQuery) ([]store.Object, int, error) {
	if !store.ValidJSONObject(q.MetaFilter) || len(q.Collections) > MaxItems || len(q.ExcludeOwners) > MaxOwners {
		return nil, 0, ErrBadRequest
	}
	ratingSlot, ratingAvg, err := objSort(q.Sort)
	if err != nil {
		return nil, 0, err
	}
	if q.MinRatings > 0 && ratingSlot == nil {
		return nil, 0, ErrBadRequest
	}
	if q.Received && validAccount(q.Viewer) != nil {
		return nil, 0, ErrBadRequest
	}
	for _, c := range q.Collections {
		if !collRE.MatchString(c) {
			return nil, 0, ErrBadRequest
		}
	}
	ranges, err := ParseMetaRange(q.MetaRange)
	if err != nil {
		return nil, 0, err
	}
	var owners []string
	var rd store.Reader
	if q.AllOwners {
		if len(q.Owners) > 0 || q.Friends {
			return nil, 0, ErrBadRequest
		}
		// Any owner can hold a friends-only row, so a signed-in viewer needs the friend list.
		if rd, err = s.reader(ctx, q.Viewer, []string{""}, true); err != nil {
			return nil, 0, err
		}
	} else if owners, rd, err = s.scope(ctx, q.Viewer, q.Owners, q.Friends, q.Self); err != nil {
		return nil, 0, err
	}
	excl := make([]string, len(q.ExcludeOwners))
	for i, o := range q.ExcludeOwners {
		if err := validOwner(o); err != nil {
			return nil, 0, err
		}
		excl[i] = strings.ToLower(o)
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return nil, 0, err
	}
	os, hits, err := s.Store.ListObjects(ctx, store.ObjQuery{T: t, Network: r.Network, Reader: rd, Owners: owners, AllOwners: q.AllOwners,
		ExcludeOwners: excl, Collection: q.Collection, Collections: q.Collections, Names: q.Names, MetaFilter: q.MetaFilter, MetaRanges: ranges,
		CreatedAfter: msTime(q.CreatedAfter), CreatedBefore: msTime(q.CreatedBefore), UpdatedAfter: msTime(q.UpdatedAfter), UpdatedBefore: msTime(q.UpdatedBefore),
		Sort: q.Sort, RatingSlot: ratingSlot, RatingAvg: ratingAvg, MinRatings: int64(q.MinRatings), Received: q.Received,
		Ascending: q.Ascending, Offset: max(q.Offset, 0), Limit: clamp(q.Limit), WithBody: q.Body})
	for i := range os {
		if err == nil {
			err = s.withURL(ctx, &os[i])
		}
	}
	if err == nil && q.Ratings {
		err = s.attachRatings(ctx, os)
	}
	return os, hits, err
}

// MetaChange is UpdateObjectMeta's request: nil / zero fields keep the stored value.
type MetaChange struct {
	Writer, Setter    string
	Info              *[]byte
	Meta              *string
	MetaMerge         string
	Name, Collection  *string
	Vis               string
	Recipients        []string
	RecipientsFriends bool
	Expires           *int64 // Unix ms; 0 = never
	Cond              Condition
}

// UpdateObjectMeta changes an object's metadata only; the body (inline or blob), the size and the
// ratings stay. found=false: no such object for this network. The writer follows the write rule
// for the object's owner.
func (s *Service) UpdateObjectMeta(ctx context.Context, network string, id int64, c MetaChange) (o store.Object, found, condFailed bool, err error) {
	if c.Info == nil && c.Meta == nil && c.MetaMerge == "" && c.Name == nil && c.Collection == nil && c.Vis == "" &&
		len(c.Recipients) == 0 && !c.RecipientsFriends && c.Expires == nil && c.Setter == "" {
		return o, false, false, ErrBadRequest
	}
	if validObjVis(c.Vis) != nil || (c.Meta != nil && (*c.Meta == "" || !store.ValidJSONObject(*c.Meta))) ||
		!store.ValidJSONObject(c.MetaMerge) || (c.Name != nil && (*c.Name == "" || len(*c.Name) > MaxName)) ||
		(c.Collection != nil && !collRE.MatchString(*c.Collection)) {
		return o, false, false, ErrBadRequest
	}
	// A recipient list only travels with a visibility (as PutObject stores it).
	if c.Vis == "" && (len(c.Recipients) > 0 || c.RecipientsFriends) {
		return o, false, false, ErrBadRequest
	}
	if (c.Info != nil && len(*c.Info) > MaxInfo) || (c.Meta != nil && len(*c.Meta) > MaxMeta) || len(c.MetaMerge) > MaxMeta {
		return o, false, false, ErrTooLarge
	}
	recips, err := cleanRecipients(c.Recipients)
	if err != nil {
		return o, false, false, err
	}
	if err := validAccount(c.Writer); err != nil {
		return o, false, false, err
	}
	u := store.MetaUpdate{ID: id, Setter: c.Setter, Info: c.Info, Meta: c.Meta, MetaMerge: c.MetaMerge, MaxMeta: MaxMeta,
		Name: c.Name, Collection: c.Collection, Vis: c.Vis, Recipients: recips, RecipFrnds: c.RecipientsFriends}
	if c.Expires != nil {
		u.SetExpiry = true
		if *c.Expires != 0 {
			t := time.UnixMilli(*c.Expires).UTC()
			if !t.After(time.Now()) {
				return o, false, false, ErrBadRequest
			}
			u.Expires = &t
		}
	}
	cur, err := s.object(ctx, network, id)
	if err != nil || cur == nil {
		return o, false, false, err
	}
	if err := s.authorizeWrite(ctx, c.Writer, cur.Owner); err != nil {
		return o, false, false, err
	}
	if !c.Cond.none() {
		u.Cond = func(cur *store.Object) bool { return c.Cond.holds(&cur.Updated, cur.Setter) }
	}
	o, found, condFailed, err = s.Store.UpdateObjectMeta(ctx, u)
	if errors.Is(err, store.ErrMetaTooLarge) {
		err = ErrTooLarge
	}
	if err != nil || !found || condFailed {
		return store.Object{}, found, condFailed, err
	}
	return o, true, false, s.withURL(ctx, &o)
}

func (s *Service) DeleteObjects(ctx context.Context, r Ref, owner, writer, collection string, names []string) (int64, error) {
	if err := s.authorizeWrite(ctx, writer, owner); err != nil {
		return 0, err
	}
	if collection == "" {
		collection = "default"
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return 0, err
	}
	n, err := s.Store.DeleteObjects(ctx, t, r.Network, strings.ToLower(owner), collection, names)
	if err == nil && n > 0 {
		s.drainQuiet(ctx, strings.ToLower(owner))
	}
	return n, err
}

type Usage struct {
	Bytes, QuotaBytes                                int64
	Objects, Variables, QuotaObjects, MaxObjectBytes int
}

func (s *Service) Usage(ctx context.Context, r Ref, owner string) (Usage, error) {
	if err := validOwner(owner); err != nil {
		return Usage{}, err
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil {
		return Usage{}, err
	}
	if !found {
		d := s.Store.Defaults
		return Usage{QuotaBytes: d.QuotaBytes, QuotaObjects: d.QuotaObjects, MaxObjectBytes: d.MaxObjectBytes}, nil
	}
	b, o, v, err := s.Store.Usage(ctx, t.Key, strings.ToLower(owner))
	return Usage{Bytes: b, Objects: o, Variables: v, QuotaBytes: t.QuotaBytes, QuotaObjects: t.QuotaObjects, MaxObjectBytes: t.MaxObjectBytes}, err
}

// EraseAccount is the account-erasure finaliser's call: rows deleted (variables, objects, pending
// uploads), the ratings it made and the ratings on its objects, the other objects whose recipient
// lists named it (it is taken out), and blobs deleted from the bucket.
// The blobs are deleted before it answers; when the bucket fails the rows are already gone and the
// error asks for the repeat, which finishes the blobs (the trash remembers them under the account).
func (s *Service) EraseAccount(ctx context.Context, account string) (e store.Erased, blobCount int64, err error) {
	if err := validAccount(account); err != nil {
		return e, 0, err
	}
	account = strings.ToLower(account)
	if e, err = s.Store.EraseAccount(ctx, account); err != nil {
		return store.Erased{}, 0, err
	}
	blobCount, err = s.Drain(ctx, account)
	return e, blobCount, err
}

// ---- blob tier ----

// withURL adds the presigned GET to a blob-backed object.
func (s *Service) withURL(ctx context.Context, o *store.Object) error {
	if o == nil || o.BlobKey == "" || s.Blobs == nil {
		return nil
	}
	u, err := s.Blobs.PresignGet(ctx, o.BlobKey, s.urlTTL())
	if err != nil {
		return errors.Join(ErrBlobs, err)
	}
	o.GetURL = u
	return nil
}

// Drain deletes trashed blobs from the bucket (one owner's, or with owner "" a batch of anyone's)
// and returns how many it deleted. A key whose delete fails stays in the trash for the janitor.
func (s *Service) Drain(ctx context.Context, owner string) (int64, error) {
	if s.Blobs == nil {
		return 0, nil
	}
	var n int64
	for {
		keys, err := s.Store.Trash(ctx, owner, 500)
		if err != nil || len(keys) == 0 {
			return n, err
		}
		done := keys[:0:0]
		var failed error
		for _, k := range keys {
			if err := s.Blobs.Delete(ctx, k); err != nil {
				failed = errors.Join(ErrBlobs, err)
				break
			}
			done = append(done, k)
		}
		if err := s.Store.Untrash(ctx, done); err != nil {
			return n, err
		}
		n += int64(len(done))
		if failed != nil {
			return n, failed
		}
	}
}

// drainQuiet is Drain after a write: a failure is left to the janitor.
func (s *Service) drainQuiet(ctx context.Context, owner string) {
	if _, err := s.Drain(ctx, owner); err != nil {
		log.Printf("title-storage: blob delete deferred to the janitor: %v", err)
	}
}

type Prepare struct {
	Owner, Writer, Setter string
	Collection, Name      string
	Size                  int64
	SHA256                string
	Info                  []byte
	Meta                  string
	Vis                   string
	Recipients            []string
	RecipientsFriends     bool
	TTL                   time.Duration
	PostForm              bool
}

type Prepared struct {
	ID         int64
	Method     string
	URL        string
	Headers    map[string]string
	FormFields map[string]string
	Expires    time.Time
}

// PrepareUpload checks the write rule, the size cap and the quota, reserves the object id and
// returns the presigned request for the body.
func (s *Service) PrepareUpload(ctx context.Context, r Ref, p Prepare) (Prepared, error) {
	if s.Blobs == nil {
		return Prepared{}, ErrNoBlobs
	}
	if p.Collection == "" {
		p.Collection = "default"
	}
	if !collRE.MatchString(p.Collection) || len(p.Name) > MaxName || validObjVis(p.Vis) != nil || !store.ValidJSONObject(p.Meta) ||
		p.Size < 0 || !blobs.ValidDigest(p.SHA256) {
		return Prepared{}, ErrBadRequest
	}
	recips, err := cleanRecipients(p.Recipients)
	if err != nil {
		return Prepared{}, err
	}
	if len(p.Info) > MaxInfo || len(p.Meta) > MaxMeta {
		return Prepared{}, ErrTooLarge
	}
	if err := s.authorizeWrite(ctx, p.Writer, p.Owner); err != nil {
		return Prepared{}, err
	}
	t, err := s.forWrite(ctx, r)
	if err != nil {
		return Prepared{}, err
	}
	if p.Size > int64(t.MaxObjectBytes) {
		return Prepared{}, ErrTooLarge
	}
	owner := strings.ToLower(p.Owner)
	u, err := s.Store.PrepareUpload(ctx, store.Upload{T: t, Network: r.Network, Owner: owner, Setter: p.Setter, Collection: p.Collection,
		Name: p.Name, Info: p.Info, Meta: p.Meta, Size: p.Size, SHA256: p.SHA256, Vis: p.Vis, Recipients: recips, RecipientsFriends: p.RecipientsFriends,
		TTL: p.TTL, Expires: store.Now().Add(s.uploadTTL())},
		func(id int64) string { return blobKey(id) })
	if err != nil {
		return Prepared{}, err
	}
	s.drainQuiet(ctx, owner) // a replaced pending upload may have sent its bytes
	out := Prepared{ID: u.ID, Expires: u.Expires, Method: "PUT"}
	if p.PostForm {
		out.Method = "POST"
		out.URL, out.FormFields, err = s.Blobs.PresignPost(ctx, u.BlobKey, p.Size, s.urlTTL())
	} else {
		out.Headers = map[string]string{"Content-Type": "application/octet-stream"}
		out.URL, err = s.Blobs.PresignPut(ctx, u.BlobKey, s.urlTTL())
	}
	if err != nil {
		return Prepared{}, errors.Join(ErrBlobs, err)
	}
	return out, nil
}

// CompleteUpload verifies the uploaded body (size, and the digest when one was given) and makes
// the object visible. A body that does not match is deleted and the upload stays open, so the
// console can send it again before the upload expires.
func (s *Service) CompleteUpload(ctx context.Context, id int64, writer string) (*store.Object, error) {
	if s.Blobs == nil {
		return nil, ErrNoBlobs
	}
	u, err := s.Store.PendingUpload(ctx, id)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, ErrNoUpload
	}
	if err := s.authorizeWrite(ctx, writer, u.Owner); err != nil {
		return nil, err
	}
	size, err := s.Blobs.Size(ctx, u.BlobKey)
	if errors.Is(err, blobs.ErrMissing) {
		return nil, ErrMismatch
	}
	if err != nil {
		return nil, errors.Join(ErrBlobs, err)
	}
	ok := size == u.Size
	if ok && u.SHA256 != "" {
		sum, err := s.Blobs.SHA256(ctx, u.BlobKey)
		if err != nil {
			return nil, errors.Join(ErrBlobs, err)
		}
		ok = sum == u.SHA256
	}
	if !ok {
		if err := s.Blobs.Delete(ctx, u.BlobKey); err != nil {
			log.Printf("title-storage: delete of a mismatched upload: %v", err)
		}
		return nil, ErrMismatch
	}
	o, found, err := s.Store.CompleteUpload(ctx, id, u.BlobKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNoUpload
	}
	s.drainQuiet(ctx, u.Owner) // the object it replaced
	return &o, s.withURL(ctx, &o)
}

// blobKey names a body in the bucket: the object id and a random part, so a replacement never
// overwrites the bytes a reader may still be fetching and keys cannot be guessed.
func blobKey(id int64) string {
	return strconv.FormatInt(id, 10) + "/" + randHex(16)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
