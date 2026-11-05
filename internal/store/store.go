// Package store holds the title storage: titles and their per-network native ids, integer
// variables and named object blobs per (title, owner). One Postgres database (`title_storage`);
// the schema migrates at start. Nothing console-shaped is stored: adapters map their ids to core
// accounts (or a title's virtual user) first.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var keyChars = regexp.MustCompile(`[^A-Za-z0-9:_-]`)

var (
	ErrNoTitle  = errors.New("title-storage: unknown title")
	ErrConflict = errors.New("title-storage: title conflict")
	ErrQuota    = errors.New("title-storage: quota exceeded")
	ErrOverflow = errors.New("title-storage: integer overflow")
)

const (
	Private = "private"
	Friends = "friends"
	Public  = "public"
)

// Stamp is the resolution of every stored time: Title User Storage prints centiseconds, so a
// condition can only match what the console saw if the service never stores finer.
const Stamp = 10 * time.Millisecond

func Now() time.Time { return time.Now().UTC().Truncate(Stamp) }

// Title is a title's resolved policy. Zero quota fields are filled from the service defaults.
type Title struct {
	Key            string
	Name           string
	Cross          bool
	QuotaBytes     int64
	QuotaObjects   int
	MaxObjectBytes int
	DefaultVis     string
	NativeIDs      map[string]string
	Exists         bool
}

// NS is the row namespace: "" when the title is shared across networks, else the network.
func (t Title) NS(network string) string {
	if t.Cross {
		return ""
	}
	return network
}

// Defaults are the quota values a title without its own gets.
type Defaults struct {
	QuotaBytes     int64
	QuotaObjects   int
	MaxObjectBytes int
}

type Store struct {
	DB       *pgxpool.Pool
	Defaults Defaults
}

type Variable struct {
	Owner      string
	Slot       int32
	Value      int64
	Setter     string
	Updated    time.Time
	Visibility string
	Network    string
}

type Object struct {
	ID         int64
	Title      string
	Owner      string
	Collection string
	Name       string
	Info       []byte
	Meta       string
	Body       []byte
	Size       int64
	Setter     string
	Visibility string
	Network    string
	Created    time.Time
	Updated    time.Time
	Expires    *time.Time
	BlobKey    string // non-empty: the body lives in the blob tier under this key, Body is empty
	SHA256     string // the blob's digest when the uploader gave one
	GetURL     string // not stored: the presigned GET the service adds to a blob-backed object
}

// Reader is who is asking: the viewer's account (empty = anonymous) and, when the query can
// touch friends-only rows, the viewer's friend list.
type Reader struct {
	Viewer  string
	Friends []string
}

const schema = `
CREATE TABLE IF NOT EXISTS titles (
    key               TEXT PRIMARY KEY,
    name              TEXT NOT NULL DEFAULT '',
    cross_network     BOOLEAN NOT NULL DEFAULT TRUE,
    quota_bytes       BIGINT NOT NULL DEFAULT 0,
    quota_objects     INTEGER NOT NULL DEFAULT 0,
    max_object_bytes  INTEGER NOT NULL DEFAULT 0,
    default_vis       TEXT NOT NULL DEFAULT 'private' CHECK (default_vis IN ('private','friends','public'))
);
CREATE TABLE IF NOT EXISTS title_keys (
    network   TEXT NOT NULL,
    native_id TEXT NOT NULL,
    title_key TEXT NOT NULL REFERENCES titles(key) ON UPDATE CASCADE,
    PRIMARY KEY (network, native_id)
);
CREATE TABLE IF NOT EXISTS variables (
    title_key  TEXT NOT NULL,
    ns         TEXT NOT NULL DEFAULT '',
    owner      TEXT NOT NULL,
    slot       INTEGER NOT NULL,
    value      BIGINT NOT NULL,
    setter     TEXT NOT NULL DEFAULT '',
    visibility TEXT NOT NULL DEFAULT 'private' CHECK (visibility IN ('private','friends','public')),
    network    TEXT NOT NULL DEFAULT '',
    updated    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (title_key, ns, owner, slot)
);
CREATE INDEX IF NOT EXISTS variables_owner ON variables (owner);
CREATE SEQUENCE IF NOT EXISTS objects_id_seq;
CREATE TABLE IF NOT EXISTS objects (
    id         BIGINT PRIMARY KEY DEFAULT nextval('objects_id_seq'),
    title_key  TEXT NOT NULL,
    ns         TEXT NOT NULL DEFAULT '',
    owner      TEXT NOT NULL,
    collection TEXT NOT NULL,
    name       TEXT NOT NULL,
    info       BYTEA NOT NULL DEFAULT ''::bytea,
    meta       JSONB NOT NULL DEFAULT '{}',
    body       BYTEA NOT NULL DEFAULT ''::bytea,
    size       BIGINT NOT NULL DEFAULT 0,
    setter     TEXT NOT NULL DEFAULT '',
    visibility TEXT NOT NULL DEFAULT 'private' CHECK (visibility IN ('private','friends','public')),
    network    TEXT NOT NULL DEFAULT '',
    created    TIMESTAMPTZ NOT NULL,
    updated    TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    UNIQUE (title_key, ns, owner, collection, name)
);
CREATE INDEX IF NOT EXISTS objects_owner ON objects (owner);
CREATE INDEX IF NOT EXISTS objects_name ON objects (title_key, collection, name);
CREATE INDEX IF NOT EXISTS objects_list ON objects (title_key, collection, updated);
CREATE INDEX IF NOT EXISTS objects_expiry ON objects (expires_at) WHERE expires_at IS NOT NULL;

-- Blob tier (v0.3.0): a body in S3 instead of the row. uploads are prepared, not yet completed
-- writes (the id is reserved; they hold quota until they complete or expire); blob_trash is every
-- S3 key whose row is gone, deleted from the bucket by the caller and, on failure, by the janitor.
ALTER TABLE objects ADD COLUMN IF NOT EXISTS blob_key TEXT NOT NULL DEFAULT '';
ALTER TABLE objects ADD COLUMN IF NOT EXISTS sha256 TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS uploads (
    id          BIGINT PRIMARY KEY,
    title_key   TEXT NOT NULL,
    ns          TEXT NOT NULL DEFAULT '',
    owner       TEXT NOT NULL,
    collection  TEXT NOT NULL,
    name        TEXT NOT NULL,
    info        BYTEA NOT NULL DEFAULT ''::bytea,
    meta        JSONB NOT NULL DEFAULT '{}',
    size        BIGINT NOT NULL,
    sha256      TEXT NOT NULL DEFAULT '',
    setter      TEXT NOT NULL DEFAULT '',
    visibility  TEXT NOT NULL DEFAULT '',
    network     TEXT NOT NULL DEFAULT '',
    ttl_seconds INTEGER NOT NULL DEFAULT 0,
    blob_key    TEXT NOT NULL UNIQUE,
    created     TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (title_key, ns, owner, collection, name)
);
CREATE INDEX IF NOT EXISTS uploads_owner ON uploads (owner);
CREATE INDEX IF NOT EXISTS uploads_expiry ON uploads (expires_at);
CREATE TABLE IF NOT EXISTS blob_trash (
    key     TEXT PRIMARY KEY,
    owner   TEXT NOT NULL DEFAULT '',
    trashed TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS blob_trash_owner ON blob_trash (owner);

INSERT INTO titles (key, name) VALUES ('terraria', 'Terraria') ON CONFLICT DO NOTHING;
INSERT INTO title_keys (network, native_id, title_key) VALUES ('np', 'NPWR04270_00', 'terraria') ON CONFLICT DO NOTHING;
`

// Open opens the pool and migrates.
func Open(ctx context.Context, url string, d Defaults) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{DB: pool, Defaults: d}, nil
}

func (s *Store) Close() { s.DB.Close() }

const titleCols = `key, name, cross_network, quota_bytes, quota_objects, max_object_bytes, default_vis`

func (s *Store) fill(t *Title) {
	if t.QuotaBytes <= 0 {
		t.QuotaBytes = s.Defaults.QuotaBytes
	}
	if t.QuotaObjects <= 0 {
		t.QuotaObjects = s.Defaults.QuotaObjects
	}
	if t.MaxObjectBytes <= 0 {
		t.MaxObjectBytes = s.Defaults.MaxObjectBytes
	}
	t.Exists = true
}

func scanTitle(row pgx.Row) (Title, error) {
	var t Title
	err := row.Scan(&t.Key, &t.Name, &t.Cross, &t.QuotaBytes, &t.QuotaObjects, &t.MaxObjectBytes, &t.DefaultVis)
	return t, err
}

// Title loads a title by its neutral key.
func (s *Store) Title(ctx context.Context, key string) (Title, error) {
	t, err := scanTitle(s.DB.QueryRow(ctx, `SELECT `+titleCols+` FROM titles WHERE key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Title{}, ErrNoTitle
	}
	if err != nil {
		return Title{}, err
	}
	s.fill(&t)
	return t, nil
}

// ResolveTitle maps (network, native id) to the title. With create, an unmapped native id
// registers itself as title "<network>:<native>" so nothing is lost; the operator can MapTitle
// it before data exists. With an empty network, native is already a title key (a missing one
// is created when create is set: reserved keys such as np-messages declare themselves).
func (s *Store) ResolveTitle(ctx context.Context, network, native string, create bool) (Title, error) {
	native = strings.TrimSpace(native)
	if native == "" {
		return Title{}, ErrNoTitle
	}
	if network == "" {
		t, err := s.Title(ctx, native)
		if errors.Is(err, ErrNoTitle) && create {
			if keyChars.MatchString(native) {
				return Title{}, ErrNoTitle
			}
			if _, err = s.DB.Exec(ctx, `INSERT INTO titles (key, name) VALUES ($1, $1) ON CONFLICT DO NOTHING`, native); err != nil {
				return Title{}, err
			}
			return s.Title(ctx, native)
		}
		return t, err
	}
	var k string
	err := s.DB.QueryRow(ctx, `SELECT title_key FROM title_keys WHERE network = $1 AND native_id = $2`, network, native).Scan(&k)
	if errors.Is(err, pgx.ErrNoRows) {
		if !create {
			return Title{}, ErrNoTitle
		}
		err = s.register(ctx, network, native)
		if err == nil {
			// A concurrent MapTitle may have won, so read the mapping back.
			err = s.DB.QueryRow(ctx, `SELECT title_key FROM title_keys WHERE network = $1 AND native_id = $2`, network, native).Scan(&k)
		}
	}
	if err != nil {
		return Title{}, err
	}
	return s.Title(ctx, k)
}

// register creates the title "<network>:<native>" and maps the native id to it.
func (s *Store) register(ctx context.Context, network, native string) error {
	k := network + ":" + keyChars.ReplaceAllString(native, "_")
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO titles (key, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`, k, native); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO title_keys (network, native_id, title_key) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, network, native, k); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MapTitle creates the title if needed and maps (network, native) to it. A mapping that already
// points elsewhere is moved only when nothing has been stored under the old key.
func (s *Store) MapTitle(ctx context.Context, title, name, network, native string) error {
	if title == "" || keyChars.MatchString(title) {
		return ErrNoTitle
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO titles (key, name) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE titles.name END`, title, name); err != nil {
		return err
	}
	if network != "" && native != "" {
		var old string
		err = tx.QueryRow(ctx, `SELECT title_key FROM title_keys WHERE network = $1 AND native_id = $2 FOR UPDATE`, network, native).Scan(&old)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			_, err = tx.Exec(ctx, `INSERT INTO title_keys (network, native_id, title_key) VALUES ($1, $2, $3)`, network, native, title)
		case err != nil:
		case old != title:
			var n int
			if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM variables WHERE title_key = $1) + (SELECT count(*) FROM objects WHERE title_key = $1)`, old).Scan(&n); err == nil {
				if n > 0 {
					return fmt.Errorf("%w: %s/%s already holds %d rows under %q", ErrConflict, network, native, n, old)
				}
				_, err = tx.Exec(ctx, `UPDATE title_keys SET title_key = $3 WHERE network = $1 AND native_id = $2`, network, native, title)
			}
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// TitlePolicy is what UpsertTitle carries; zero fields keep the stored value.
type TitlePolicy struct {
	Name           string
	Cross          *bool
	QuotaBytes     int64
	QuotaObjects   int
	MaxObjectBytes int
	DefaultVis     string
}

// UpsertTitle creates or updates a title's policy. cross_network cannot flip once rows exist
// (rows are namespaced by network on a per-network title).
func (s *Store) UpsertTitle(ctx context.Context, key string, p TitlePolicy) (Title, error) {
	if key == "" || keyChars.MatchString(key) {
		return Title{}, ErrNoTitle
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Title{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO titles (key, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`, key, p.Name); err != nil {
		return Title{}, err
	}
	cur, err := scanTitle(tx.QueryRow(ctx, `SELECT `+titleCols+` FROM titles WHERE key = $1 FOR UPDATE`, key))
	if err != nil {
		return Title{}, err
	}
	if p.Cross != nil && *p.Cross != cur.Cross {
		var n int
		if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM variables WHERE title_key = $1) + (SELECT count(*) FROM objects WHERE title_key = $1)`, key).Scan(&n); err != nil {
			return Title{}, err
		}
		if n > 0 {
			return Title{}, fmt.Errorf("%w: %s already holds %d rows; cross_network cannot change", ErrConflict, key, n)
		}
		cur.Cross = *p.Cross
	}
	if p.Name != "" {
		cur.Name = p.Name
	}
	if p.QuotaBytes > 0 {
		cur.QuotaBytes = p.QuotaBytes
	}
	if p.QuotaObjects > 0 {
		cur.QuotaObjects = p.QuotaObjects
	}
	if p.MaxObjectBytes > 0 {
		cur.MaxObjectBytes = p.MaxObjectBytes
	}
	if p.DefaultVis != "" {
		cur.DefaultVis = p.DefaultVis
	}
	if _, err = tx.Exec(ctx, `UPDATE titles SET name=$2, cross_network=$3, quota_bytes=$4, quota_objects=$5, max_object_bytes=$6, default_vis=$7 WHERE key=$1`,
		key, cur.Name, cur.Cross, cur.QuotaBytes, cur.QuotaObjects, cur.MaxObjectBytes, cur.DefaultVis); err != nil {
		return Title{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Title{}, err
	}
	s.fill(&cur)
	return cur, nil
}

// Titles lists every title with its native ids.
func (s *Store) Titles(ctx context.Context) ([]Title, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+titleCols+` FROM titles ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Title
	idx := map[string]int{}
	for rows.Next() {
		t, err := scanTitle(rows)
		if err != nil {
			return nil, err
		}
		s.fill(&t)
		t.NativeIDs = map[string]string{}
		idx[t.Key] = len(out)
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	ks, err := s.DB.Query(ctx, `SELECT title_key, network, native_id FROM title_keys ORDER BY network, native_id`)
	if err != nil {
		return nil, err
	}
	defer ks.Close()
	for ks.Next() {
		var k, n, id string
		if err := ks.Scan(&k, &n, &id); err != nil {
			return nil, err
		}
		if i, ok := idx[k]; ok {
			out[i].NativeIDs[n] = id
		}
	}
	return out, ks.Err()
}

// ---- reads: visibility ----

// visSQL is the SQL predicate for what a Reader may see, with the viewer at $v and the
// viewer's friends at $f (argument positions are passed in). A virtual owner is always visible.
func visSQL(v, f int) string {
	return fmt.Sprintf(`(owner = $%d OR starts_with(owner, 'v:') OR visibility = 'public' OR (visibility = 'friends' AND owner = ANY($%d)))`, v, f)
}

// ---- variables ----

func isOverflow(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "22003"
}

func wrapOverflow(err error) error {
	if isOverflow(err) {
		return ErrOverflow
	}
	return err
}

// VarWrite carries what every variable write needs.
type VarWrite struct {
	T       Title
	Network string
	Owner   string
	Setter  string
	Vis     string // "" keeps the stored visibility; a new row takes the title default
}

func (w VarWrite) insertVis() string {
	if w.Vis != "" {
		return w.Vis
	}
	return w.T.DefaultVis
}

func (s *Store) SetVariables(ctx context.Context, w VarWrite, items map[int32]int64) error {
	now := Now()
	b := &pgx.Batch{}
	for slot, v := range items {
		b.Queue(`INSERT INTO variables (title_key, ns, owner, slot, value, setter, visibility, network, updated)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (title_key, ns, owner, slot) DO UPDATE SET value = $5, setter = $6,
			  visibility = COALESCE(NULLIF($10,''), variables.visibility), network = $8, updated = $9`,
			w.T.Key, w.T.NS(w.Network), w.Owner, slot, v, w.Setter, w.insertVis(), w.Network, now, w.Vis)
	}
	br := s.DB.SendBatch(ctx, b)
	for range items {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return err
		}
	}
	return br.Close()
}

// AddVariable adds delta atomically (a missing slot counts as 0) and returns the new row.
func (s *Store) AddVariable(ctx context.Context, w VarWrite, slot int32, delta int64) (Variable, error) {
	v := Variable{Owner: w.Owner, Slot: slot}
	err := s.DB.QueryRow(ctx, `INSERT INTO variables (title_key, ns, owner, slot, value, setter, visibility, network, updated)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (title_key, ns, owner, slot) DO UPDATE SET value = variables.value + $5, setter = $6,
		  visibility = COALESCE(NULLIF($10,''), variables.visibility), network = $8, updated = $9
		RETURNING value, setter, visibility, network, updated`,
		w.T.Key, w.T.NS(w.Network), w.Owner, slot, delta, w.Setter, w.insertVis(), w.Network, Now(), w.Vis).
		Scan(&v.Value, &v.Setter, &v.Visibility, &v.Network, &v.Updated)
	return v, wrapOverflow(err)
}

// UpdateVariable runs fn on the current row (nil when missing) under a lock on the slot; fn
// returns the value to store, or false to leave the slot unchanged. The returned row is the
// slot afterwards (nil when it does not exist).
func (s *Store) UpdateVariable(ctx context.Context, w VarWrite, slot int32, fn func(*Variable) (int64, bool)) (*Variable, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	ns := w.T.NS(w.Network)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%s|%s|%s|%d", w.T.Key, ns, w.Owner, slot)); err != nil {
		return nil, err
	}
	v := Variable{Owner: w.Owner, Slot: slot}
	var cur *Variable
	err = tx.QueryRow(ctx, `SELECT value, setter, visibility, network, updated FROM variables
		WHERE title_key=$1 AND ns=$2 AND owner=$3 AND slot=$4 FOR UPDATE`, w.T.Key, ns, w.Owner, slot).
		Scan(&v.Value, &v.Setter, &v.Visibility, &v.Network, &v.Updated)
	if err == nil {
		cur = &v
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	value, ok := fn(cur)
	if !ok {
		return cur, nil
	}
	next := Variable{Owner: w.Owner, Slot: slot}
	err = tx.QueryRow(ctx, `INSERT INTO variables (title_key, ns, owner, slot, value, setter, visibility, network, updated)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (title_key, ns, owner, slot) DO UPDATE SET value = $5, setter = $6,
		  visibility = COALESCE(NULLIF($10,''), variables.visibility), network = $8, updated = $9
		RETURNING value, setter, visibility, network, updated`,
		w.T.Key, ns, w.Owner, slot, value, w.Setter, w.insertVis(), w.Network, Now(), w.Vis).
		Scan(&next.Value, &next.Setter, &next.Visibility, &next.Network, &next.Updated)
	if err != nil {
		return nil, wrapOverflow(err)
	}
	return &next, tx.Commit(ctx)
}

func (s *Store) DeleteVariables(ctx context.Context, t Title, network, owner string, slots []int32) (int64, error) {
	tag, err := s.DB.Exec(ctx, `DELETE FROM variables WHERE title_key=$1 AND ns=$2 AND owner=$3 AND slot = ANY($4)`,
		t.Key, t.NS(network), owner, slots)
	return tag.RowsAffected(), err
}

// VarQuery is a variable listing: one scope (owners), the visibility filter and the paging.
type VarQuery struct {
	T         Title
	Network   string
	Reader    Reader
	Owners    []string
	Slots     []int32
	Sort      string // updated | value | slot
	Ascending bool
	Offset    int
	Limit     int
}

func orderDir(asc bool) string {
	if asc {
		return "ASC"
	}
	return "DESC"
}

func (s *Store) ListVariables(ctx context.Context, q VarQuery) ([]Variable, int, error) {
	col := map[string]string{"": "updated", "updated": "updated", "value": "value", "slot": "slot"}[q.Sort]
	if col == "" {
		col = "updated"
	}
	args := []any{q.T.Key, q.T.NS(q.Network), q.Owners, q.Reader.Viewer, nonNil(q.Reader.Friends)}
	where := `title_key=$1 AND ns=$2 AND owner = ANY($3) AND ` + visSQL(4, 5)
	if len(q.Slots) > 0 {
		args = append(args, q.Slots)
		where += fmt.Sprintf(` AND slot = ANY($%d)`, len(args))
	}
	args = append(args, q.Limit, q.Offset)
	rows, err := s.DB.Query(ctx, fmt.Sprintf(`SELECT owner, slot, value, setter, visibility, network, updated, count(*) OVER()
		FROM variables WHERE %s ORDER BY %s %s, owner, slot LIMIT $%d OFFSET $%d`, where, col, orderDir(q.Ascending), len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Variable
	hits := 0
	for rows.Next() {
		var v Variable
		if err := rows.Scan(&v.Owner, &v.Slot, &v.Value, &v.Setter, &v.Visibility, &v.Network, &v.Updated, &hits); err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	return out, hits, rows.Err()
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// ---- objects ----

const objCols = `id, title_key, owner, collection, name, info, meta::text, size, setter, visibility, network, created, updated, expires_at, blob_key, sha256`

// objDst is the scan target list for objCols.
func objDst(o *Object) []any {
	return []any{&o.ID, &o.Title, &o.Owner, &o.Collection, &o.Name, &o.Info, &o.Meta, &o.Size, &o.Setter, &o.Visibility, &o.Network, &o.Created, &o.Updated, &o.Expires, &o.BlobKey, &o.SHA256}
}

func scanObject(row pgx.Row, withBody bool) (Object, error) {
	var o Object
	dst := objDst(&o)
	if withBody {
		dst = append(dst, &o.Body)
	}
	return o, row.Scan(dst...)
}

func bodyCol(withBody bool) string {
	if withBody {
		return ", body"
	}
	return ""
}

// Usage returns what one owner holds in a title: object bytes, object count, variable count.
func (s *Store) Usage(ctx context.Context, titleKey, owner string) (bytes int64, objects, variables int, err error) {
	err = s.DB.QueryRow(ctx, `SELECT
		COALESCE((SELECT sum(size) FROM objects WHERE title_key=$1 AND owner=$2 AND (expires_at IS NULL OR expires_at > now())), 0),
		(SELECT count(*) FROM objects WHERE title_key=$1 AND owner=$2 AND (expires_at IS NULL OR expires_at > now())),
		(SELECT count(*) FROM variables WHERE title_key=$1 AND owner=$2)`, titleKey, owner).Scan(&bytes, &objects, &variables)
	return
}

// ObjPut is one object write.
type ObjPut struct {
	T          Title
	Network    string
	Owner      string
	Setter     string
	Collection string
	Name       string // "" = named by the new id
	Info       []byte
	Meta       string // JSON object
	Body       []byte
	Vis        string // "" keeps the stored value; a new row takes the title default
	TTL        time.Duration
	// Cond reports whether the existing row (nil when missing) satisfies the caller's
	// condition; a false answer writes nothing.
	Cond func(*Object) bool
}

// PutObject stores an object, enforcing the owner's quota for the title. conditionFailed is true
// when Cond rejected the existing row.
func (s *Store) PutObject(ctx context.Context, p ObjPut) (o Object, conditionFailed bool, err error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return o, false, err
	}
	defer tx.Rollback(ctx)
	ns := p.T.NS(p.Network)
	// One writer per (title, owner): the quota arithmetic below is a read-modify-write.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`, p.T.Key+"|"+p.Owner); err != nil {
		return o, false, err
	}
	now := Now()
	var id int64
	if p.Name == "" {
		if err = tx.QueryRow(ctx, `SELECT nextval('objects_id_seq')`).Scan(&id); err != nil {
			return o, false, err
		}
		p.Name = strconv.FormatInt(id, 10)
	}
	if p.Info == nil {
		p.Info = []byte{}
	}
	if p.Body == nil {
		p.Body = []byte{}
	}
	// An expired row counts as absent.
	if _, err = s.expireKey(ctx, tx, p.T.Key, ns, p.Owner, p.Collection, p.Name); err != nil {
		return o, false, err
	}
	var cur *Object
	c, err := scanObject(tx.QueryRow(ctx, `SELECT `+objCols+` FROM objects WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 FOR UPDATE`,
		p.T.Key, ns, p.Owner, p.Collection, p.Name), false)
	switch {
	case err == nil:
		cur = &c
	case !errors.Is(err, pgx.ErrNoRows):
		return o, false, err
	}
	if p.Cond != nil && !p.Cond(cur) {
		return o, true, nil
	}
	usedBytes, usedObjs, err := used(ctx, tx, p.T.Key, p.Owner)
	if err != nil {
		return o, false, err
	}
	newBytes, newObjs := usedBytes+int64(len(p.Body)), usedObjs
	if cur != nil {
		newBytes -= cur.Size
	} else {
		newObjs++
	}
	if newBytes > p.T.QuotaBytes || newObjs > p.T.QuotaObjects {
		return o, false, ErrQuota
	}
	var expires *time.Time
	if p.TTL > 0 {
		t := now.Add(p.TTL)
		expires = &t
	} else if cur != nil {
		expires = cur.Expires
	}
	if p.Meta == "" {
		p.Meta = "{}"
	}
	if cur != nil {
		id = cur.ID
	}
	// An inline write over a blob-backed object drops the blob (PrepareUpload is the large path).
	if cur != nil && cur.BlobKey != "" {
		if err = trash(ctx, tx, p.Owner, cur.BlobKey); err != nil {
			return o, false, err
		}
	}
	row := tx.QueryRow(ctx, `INSERT INTO objects (id, title_key, ns, owner, collection, name, info, meta, body, size, setter, visibility, network, created, updated, expires_at)
		VALUES (COALESCE(NULLIF($1::bigint,0), nextval('objects_id_seq')), $2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$14,$15)
		ON CONFLICT (title_key, ns, owner, collection, name) DO UPDATE SET info=$7, meta=$8::jsonb, body=$9, size=$10, setter=$11,
		  visibility = COALESCE(NULLIF($16,''), objects.visibility), network=$13, updated=$14, expires_at=$15, blob_key='', sha256=''
		RETURNING `+objCols,
		id, p.T.Key, ns, p.Owner, p.Collection, p.Name, p.Info, p.Meta, p.Body, int64(len(p.Body)), p.Setter, firstNonEmpty(p.Vis, p.T.DefaultVis), p.Network, now, expires, p.Vis)
	o, err = scanObject(row, false)
	if err != nil {
		return o, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return o, false, err
	}
	return o, false, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// GetObject loads one object by its key, without any visibility check (the caller applies it).
// A missing or expired object is (nil, nil).
func (s *Store) GetObject(ctx context.Context, t Title, network, owner, collection, name string, withBody bool) (*Object, error) {
	o, err := scanObject(s.DB.QueryRow(ctx, `SELECT `+objCols+bodyCol(withBody)+` FROM objects
		WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 AND (expires_at IS NULL OR expires_at > now())`,
		t.Key, t.NS(network), owner, collection, name), withBody)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// FindObject loads the newest object with this collection and name under any owner, without any
// visibility check. For names that are unguessable by construction (an upload reference).
func (s *Store) FindObject(ctx context.Context, t Title, network, collection, name string, withBody bool) (*Object, error) {
	o, err := scanObject(s.DB.QueryRow(ctx, `SELECT `+objCols+bodyCol(withBody)+` FROM objects
		WHERE title_key=$1 AND ns=$2 AND collection=$3 AND name=$4 AND (expires_at IS NULL OR expires_at > now())
		ORDER BY updated DESC LIMIT 1`, t.Key, t.NS(network), collection, name), withBody)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// ObjectByID loads one object by its id, without any visibility check.
func (s *Store) ObjectByID(ctx context.Context, id int64, withBody bool) (*Object, string, error) {
	var ns string
	row := s.DB.QueryRow(ctx, `SELECT ns, `+objCols+bodyCol(withBody)+` FROM objects WHERE id=$1 AND (expires_at IS NULL OR expires_at > now())`, id)
	var o Object
	dst := append([]any{&ns}, objDst(&o)...)
	if withBody {
		dst = append(dst, &o.Body)
	}
	if err := row.Scan(dst...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}
	return &o, ns, nil
}

// ObjQuery is an object listing.
type ObjQuery struct {
	T          Title
	Network    string
	Reader     Reader
	Owners     []string
	Collection string
	Names      []string
	MetaFilter string
	Sort       string // updated | created | size | name | id
	Ascending  bool
	Offset     int
	Limit      int
	WithBody   bool
}

func (s *Store) ListObjects(ctx context.Context, q ObjQuery) ([]Object, int, error) {
	col := map[string]string{"": "updated", "updated": "updated", "created": "created", "size": "size", "name": "name", "id": "id"}[q.Sort]
	if col == "" {
		col = "updated"
	}
	args := []any{q.T.Key, q.T.NS(q.Network), q.Owners, q.Reader.Viewer, nonNil(q.Reader.Friends)}
	where := `title_key=$1 AND ns=$2 AND owner = ANY($3) AND (expires_at IS NULL OR expires_at > now()) AND ` + visSQL(4, 5)
	if q.Collection != "" {
		args = append(args, q.Collection)
		where += fmt.Sprintf(` AND collection = $%d`, len(args))
	}
	if len(q.Names) > 0 {
		args = append(args, q.Names)
		where += fmt.Sprintf(` AND name = ANY($%d)`, len(args))
	}
	if q.MetaFilter != "" {
		args = append(args, q.MetaFilter)
		where += fmt.Sprintf(` AND meta @> $%d::jsonb`, len(args))
	}
	args = append(args, q.Limit, q.Offset)
	rows, err := s.DB.Query(ctx, fmt.Sprintf(`SELECT %s%s, count(*) OVER() FROM objects WHERE %s ORDER BY %s %s, id LIMIT $%d OFFSET $%d`,
		objCols, bodyCol(q.WithBody), where, col, orderDir(q.Ascending), len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Object
	hits := 0
	for rows.Next() {
		var o Object
		dst := objDst(&o)
		if q.WithBody {
			dst = append(dst, &o.Body)
		}
		dst = append(dst, &hits)
		if err := rows.Scan(dst...); err != nil {
			return nil, 0, err
		}
		out = append(out, o)
	}
	return out, hits, rows.Err()
}

// DeleteObjects removes one owner's objects in a collection (the named ones, or all of them).
// Their blobs go to the trash in the same transaction.
func (s *Store) DeleteObjects(ctx context.Context, t Title, network, owner, collection string, names []string) (int64, error) {
	var n int64
	err := s.DB.QueryRow(ctx, `WITH d AS (
		  DELETE FROM objects WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND (cardinality($5::text[]) = 0 OR name = ANY($5))
		  RETURNING owner, blob_key),
		tr AS (INSERT INTO blob_trash (key, owner) SELECT blob_key, owner FROM d WHERE blob_key <> '' ON CONFLICT DO NOTHING)
		SELECT count(*) FROM d`,
		t.Key, t.NS(network), owner, collection, nonNil(names)).Scan(&n)
	return n, err
}

// Expire deletes objects past their expiry and uploads never completed in time, trashing their
// blobs (an abandoned upload may have sent its bytes).
func (s *Store) Expire(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRow(ctx, `WITH o AS (DELETE FROM objects WHERE expires_at IS NOT NULL AND expires_at <= now() RETURNING owner, blob_key),
		u AS (DELETE FROM uploads WHERE expires_at <= now() RETURNING owner, blob_key),
		tr AS (INSERT INTO blob_trash (key, owner)
		  SELECT blob_key, owner FROM o WHERE blob_key <> '' UNION ALL SELECT blob_key, owner FROM u
		  ON CONFLICT DO NOTHING)
		SELECT (SELECT count(*) FROM o) + (SELECT count(*) FROM u)`).Scan(&n)
	return n, err
}

// EraseAccount deletes everything an account owns, in every title: variables, objects and pending
// uploads. Blobs go to the trash under the account (the caller empties it: DrainTrash).
func (s *Store) EraseAccount(ctx context.Context, account string) (int64, error) {
	var n int64
	err := s.DB.QueryRow(ctx, `WITH v AS (DELETE FROM variables WHERE owner = $1 RETURNING 1),
		o AS (DELETE FROM objects WHERE owner = $1 RETURNING owner, blob_key),
		u AS (DELETE FROM uploads WHERE owner = $1 RETURNING owner, blob_key),
		tr AS (INSERT INTO blob_trash (key, owner)
		  SELECT blob_key, owner FROM o WHERE blob_key <> '' UNION ALL SELECT blob_key, owner FROM u
		  ON CONFLICT DO NOTHING)
		SELECT (SELECT count(*) FROM v) + (SELECT count(*) FROM o) + (SELECT count(*) FROM u)`, account).Scan(&n)
	return n, err
}

// ---- blob tier ----

func trash(ctx context.Context, tx pgx.Tx, owner, key string) error {
	_, err := tx.Exec(ctx, `INSERT INTO blob_trash (key, owner) VALUES ($1, $2) ON CONFLICT DO NOTHING`, key, owner)
	return err
}

// used is what an owner holds in a title against the quota: live objects plus pending uploads
// (an upload reserves its bytes and one object until it completes or expires).
func used(ctx context.Context, tx pgx.Tx, titleKey, owner string) (bytes int64, objects int, err error) {
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(size),0), count(*) FROM (
		  SELECT size FROM objects WHERE title_key=$1 AND owner=$2 AND (expires_at IS NULL OR expires_at > now())
		  UNION ALL
		  SELECT size FROM uploads WHERE title_key=$1 AND owner=$2 AND expires_at > now()) x`, titleKey, owner).Scan(&bytes, &objects)
	return
}

// Upload is a prepared write: the object it will become, waiting for its body in the blob tier.
type Upload struct {
	ID         int64
	T          Title
	Network    string
	Owner      string
	Setter     string
	Collection string
	Name       string // "" = named by the id
	Info       []byte
	Meta       string
	Size       int64
	SHA256     string
	Vis        string
	TTL        time.Duration // of the object once complete
	BlobKey    string
	Expires    time.Time // of the upload
}

// PrepareUpload reserves the object's id and the quota for an upload. A second prepare for the
// same object replaces the first (its blob, if it was sent, is trashed). keyFor names the blob
// from the id. The id is the existing object's when the upload replaces one.
func (s *Store) PrepareUpload(ctx context.Context, u Upload, keyFor func(id int64) string) (Upload, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return u, err
	}
	defer tx.Rollback(ctx)
	ns := u.T.NS(u.Network)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`, u.T.Key+"|"+u.Owner); err != nil {
		return u, err
	}
	if u.Name != "" {
		// Expired rows count as absent, as in PutObject.
		if _, err = s.expireKey(ctx, tx, u.T.Key, ns, u.Owner, u.Collection, u.Name); err != nil {
			return u, err
		}
	}
	var curID, curSize int64
	if u.Name != "" {
		err = tx.QueryRow(ctx, `SELECT id, size FROM objects WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 FOR UPDATE`,
			u.T.Key, ns, u.Owner, u.Collection, u.Name).Scan(&curID, &curSize)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return u, err
		}
		var oldKey string
		var oldSize int64
		err = tx.QueryRow(ctx, `DELETE FROM uploads WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 RETURNING blob_key, size`,
			u.T.Key, ns, u.Owner, u.Collection, u.Name).Scan(&oldKey, &oldSize)
		switch {
		case err == nil:
			if err = trash(ctx, tx, u.Owner, oldKey); err != nil {
				return u, err
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return u, err
		}
	}
	usedBytes, usedObjs, err := used(ctx, tx, u.T.Key, u.Owner)
	if err != nil {
		return u, err
	}
	// A replacement counts only the difference, as in PutObject (while it is pending the old
	// object still counts too, so other writes see both).
	if usedBytes-curSize+u.Size > u.T.QuotaBytes || usedObjs+1-boolInt(curID != 0) > u.T.QuotaObjects {
		return u, ErrQuota
	}
	if curID != 0 {
		u.ID = curID
	} else if err = tx.QueryRow(ctx, `SELECT nextval('objects_id_seq')`).Scan(&u.ID); err != nil {
		return u, err
	}
	if u.Name == "" {
		u.Name = strconv.FormatInt(u.ID, 10)
	}
	if u.Info == nil {
		u.Info = []byte{}
	}
	if u.Meta == "" {
		u.Meta = "{}"
	}
	u.BlobKey = keyFor(u.ID)
	now := Now()
	_, err = tx.Exec(ctx, `INSERT INTO uploads (id, title_key, ns, owner, collection, name, info, meta, size, sha256, setter, visibility, network, ttl_seconds, blob_key, created, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		u.ID, u.T.Key, ns, u.Owner, u.Collection, u.Name, u.Info, u.Meta, u.Size, u.SHA256, u.Setter, u.Vis, u.Network, int(u.TTL/time.Second), u.BlobKey, now, u.Expires)
	if err != nil {
		return u, err
	}
	return u, tx.Commit(ctx)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// expireKey drops one expired object (trashing its blob).
func (s *Store) expireKey(ctx context.Context, tx pgx.Tx, title, ns, owner, collection, name string) (int64, error) {
	tag, err := tx.Exec(ctx, `WITH d AS (DELETE FROM objects WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 AND expires_at <= now() RETURNING owner, blob_key)
		INSERT INTO blob_trash (key, owner) SELECT blob_key, owner FROM d WHERE blob_key <> '' ON CONFLICT DO NOTHING`,
		title, ns, owner, collection, name)
	return tag.RowsAffected(), err
}

// PendingUpload loads a prepared upload that has not expired; nil when there is none.
func (s *Store) PendingUpload(ctx context.Context, id int64) (*Upload, error) {
	var u Upload
	var ttl int
	err := s.DB.QueryRow(ctx, `SELECT id, title_key, network, owner, setter, collection, name, info, meta::text, size, sha256, visibility, ttl_seconds, blob_key, expires_at
		FROM uploads WHERE id=$1 AND expires_at > now()`, id).
		Scan(&u.ID, &u.T.Key, &u.Network, &u.Owner, &u.Setter, &u.Collection, &u.Name, &u.Info, &u.Meta, &u.Size, &u.SHA256, &u.Vis, &ttl, &u.BlobKey, &u.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	u.TTL = time.Duration(ttl) * time.Second
	return &u, nil
}

// CompleteUpload turns a verified upload into its object (replacing the one of the same name; that
// one's blob is trashed). ok=false when the upload is gone (expired, replaced or completed).
func (s *Store) CompleteUpload(ctx context.Context, id int64, blobKey string) (o Object, ok bool, err error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return o, false, err
	}
	defer tx.Rollback(ctx)
	// The per-owner lock first, as PrepareUpload takes it, so the two never wait on each other.
	var lockTitle, lockOwner string
	err = tx.QueryRow(ctx, `SELECT title_key, owner FROM uploads WHERE id=$1`, id).Scan(&lockTitle, &lockOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, false, nil
	}
	if err != nil {
		return o, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`, lockTitle+"|"+lockOwner); err != nil {
		return o, false, err
	}
	var u Upload
	var ns, vis string
	var ttl int
	err = tx.QueryRow(ctx, `DELETE FROM uploads WHERE id=$1 AND blob_key=$2 AND expires_at > now()
		RETURNING title_key, ns, network, owner, setter, collection, name, info, meta::text, size, sha256, visibility, ttl_seconds`, id, blobKey).
		Scan(&u.T.Key, &ns, &u.Network, &u.Owner, &u.Setter, &u.Collection, &u.Name, &u.Info, &u.Meta, &u.Size, &u.SHA256, &vis, &ttl)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, false, nil
	}
	if err != nil {
		return o, false, err
	}
	t, err := scanTitle(tx.QueryRow(ctx, `SELECT `+titleCols+` FROM titles WHERE key = $1`, u.T.Key))
	if err != nil {
		return o, false, err
	}
	if _, err = s.expireKey(ctx, tx, u.T.Key, ns, u.Owner, u.Collection, u.Name); err != nil {
		return o, false, err
	}
	var cur *Object
	c, err := scanObject(tx.QueryRow(ctx, `SELECT `+objCols+` FROM objects WHERE title_key=$1 AND ns=$2 AND owner=$3 AND collection=$4 AND name=$5 FOR UPDATE`,
		u.T.Key, ns, u.Owner, u.Collection, u.Name), false)
	switch {
	case err == nil:
		cur = &c
	case !errors.Is(err, pgx.ErrNoRows):
		return o, false, err
	}
	if cur != nil && cur.BlobKey != "" && cur.BlobKey != blobKey {
		if err = trash(ctx, tx, u.Owner, cur.BlobKey); err != nil {
			return o, false, err
		}
	}
	now := Now()
	var expires *time.Time
	if ttl > 0 {
		e := now.Add(time.Duration(ttl) * time.Second)
		expires = &e
	} else if cur != nil {
		expires = cur.Expires
	}
	o, err = scanObject(tx.QueryRow(ctx, `INSERT INTO objects (id, title_key, ns, owner, collection, name, info, meta, body, size, setter, visibility, network, created, updated, expires_at, blob_key, sha256)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,''::bytea,$9,$10,$11,$12,$13,$13,$14,$15,$16)
		ON CONFLICT (title_key, ns, owner, collection, name) DO UPDATE SET info=$7, meta=$8::jsonb, body=''::bytea, size=$9, setter=$10,
		  visibility = COALESCE(NULLIF($17,''), objects.visibility), network=$12, updated=$13, expires_at=$14, blob_key=$15, sha256=$16
		RETURNING `+objCols,
		id, u.T.Key, ns, u.Owner, u.Collection, u.Name, u.Info, u.Meta, u.Size, u.Setter, firstNonEmpty(vis, t.DefaultVis), u.Network, now, expires, blobKey, u.SHA256, vis), false)
	if err != nil {
		return o, false, err
	}
	return o, true, tx.Commit(ctx)
}

// Trash lists trashed blob keys: one owner's, or (owner "") any, oldest first.
func (s *Store) Trash(ctx context.Context, owner string, limit int) ([]string, error) {
	rows, err := s.DB.Query(ctx, `SELECT key FROM blob_trash WHERE ($1 = '' OR owner = $1) ORDER BY trashed LIMIT $2`, owner, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Untrash forgets keys whose blobs are deleted.
func (s *Store) Untrash(ctx context.Context, keys []string) error {
	_, err := s.DB.Exec(ctx, `DELETE FROM blob_trash WHERE key = ANY($1)`, keys)
	return err
}

// ValidJSONObject reports whether s is a JSON object (or empty).
func ValidJSONObject(s string) bool {
	if s == "" {
		return true
	}
	var m map[string]json.RawMessage
	return json.Unmarshal([]byte(s), &m) == nil
}
