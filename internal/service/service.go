// Package service is the title storage's rules on top of the store: title resolution, input
// limits, who may read and write which owner's rows, and the TUS operator semantics. The gRPC
// and HTTP surfaces are thin maps onto it.
package service

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

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
)

var (
	ErrBadAccount = errors.New("title-storage: owners are account UUIDs or v:<name>")
	ErrBadRequest = errors.New("title-storage: bad request")
	ErrTooLarge   = errors.New("title-storage: value too large")
	ErrForbidden  = errors.New("title-storage: the writer may not write to this owner")
	ErrNeedNet    = errors.New("title-storage: this title is per network: the request must name its network")
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

func validVis(v string) error {
	switch v {
	case "", store.Private, store.Friends, store.Public:
		return nil
	}
	return ErrBadRequest
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

// canRead applies the visibility rule to one row whose owner is already known.
func (s *Service) canRead(ctx context.Context, viewer, owner, vis string) (bool, error) {
	if vis == store.Public || IsVirtual(owner) {
		return true, nil
	}
	if viewer == "" {
		return false, nil
	}
	if strings.EqualFold(viewer, owner) {
		return true, nil
	}
	if vis != store.Friends {
		return false, nil
	}
	fs, err := s.Core.Friends(ctx, viewer)
	if err != nil {
		return false, err
	}
	for _, f := range fs {
		if strings.EqualFold(f, owner) {
			return true, nil
		}
	}
	return false, nil
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
	TTL                   time.Duration
	Cond                  Condition
}

func (s *Service) PutObject(ctx context.Context, r Ref, p Put) (store.Object, bool, error) {
	if p.Collection == "" {
		p.Collection = "default"
	}
	if !collRE.MatchString(p.Collection) || len(p.Name) > MaxName || validVis(p.Vis) != nil || !store.ValidJSONObject(p.Meta) {
		return store.Object{}, false, ErrBadRequest
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
		Info: p.Info, Meta: p.Meta, Body: p.Body, Vis: p.Vis, TTL: p.TTL}
	if !p.Cond.none() {
		put.Cond = func(cur *store.Object) bool {
			if cur == nil {
				return p.Cond.holds(nil, "")
			}
			return p.Cond.holds(&cur.Updated, cur.Setter)
		}
	}
	return s.Store.PutObject(ctx, put)
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
	ok, err := s.canRead(ctx, viewer, o.Owner, o.Visibility)
	if err != nil || !ok {
		return nil, err
	}
	return o, nil
}

// GetObjectByID is GetObject for a caller that holds the id. The id is global, so a title that
// is per network hides objects written by other networks only when the caller names its own.
func (s *Service) GetObjectByID(ctx context.Context, network, viewer string, id int64, body bool) (*store.Object, error) {
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
	ok, err := s.canRead(ctx, viewer, o.Owner, o.Visibility)
	if err != nil || !ok {
		return nil, err
	}
	return o, nil
}

type ObjQuery struct {
	Query
	Collection string
	Names      []string
	MetaFilter string
	Body       bool
}

func (s *Service) ListObjects(ctx context.Context, r Ref, q ObjQuery) ([]store.Object, int, error) {
	if !store.ValidJSONObject(q.MetaFilter) {
		return nil, 0, ErrBadRequest
	}
	owners, rd, err := s.scope(ctx, q.Viewer, q.Owners, q.Friends, q.Self)
	if err != nil {
		return nil, 0, err
	}
	t, found, err := s.forRead(ctx, r)
	if err != nil || !found {
		return nil, 0, err
	}
	return s.Store.ListObjects(ctx, store.ObjQuery{T: t, Network: r.Network, Reader: rd, Owners: owners, Collection: q.Collection, Names: q.Names,
		MetaFilter: q.MetaFilter, Sort: q.Sort, Ascending: q.Ascending, Offset: max(q.Offset, 0), Limit: clamp(q.Limit), WithBody: q.Body})
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
	return s.Store.DeleteObjects(ctx, t, r.Network, strings.ToLower(owner), collection, names)
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

// EraseAccount is the account-erasure finaliser's call; a repeat deletes nothing.
func (s *Service) EraseAccount(ctx context.Context, account string) (int64, error) {
	if err := validAccount(account); err != nil {
		return 0, err
	}
	return s.Store.EraseAccount(ctx, strings.ToLower(account))
}
