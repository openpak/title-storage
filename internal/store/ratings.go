package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Lock policies of a rating slot.
const (
	LockNone      = "none"
	LockInterval  = "interval"
	LockPeriod    = "period"
	LockPermanent = "permanent"
)

// PeriodMonth is the period_day that means "the 1st of the next month" (NEX RatingLockPeriodDay1);
// -1..-7 are Monday..Sunday.
const PeriodMonth = -17

var ErrNoRatingSlot = errors.New("title-storage: no such rating slot (InitRating it, or send a policy)")

// RatingPolicy is a slot's behaviour, fixed when the slot is created.
type RatingPolicy struct {
	Initial         int64
	Lock            string // none | interval | period | permanent
	IntervalSeconds int32
	PeriodDay       int32
	PeriodHour      int32
	Min, Max        *int64
	Modifiable      bool
	RoundMinus      bool
	NoSelf          bool
}

// Valid reports whether the policy can be stored.
func (p RatingPolicy) Valid() bool {
	switch p.Lock {
	case LockNone, LockPermanent:
	case LockInterval:
		if p.IntervalSeconds <= 0 {
			return false
		}
	case LockPeriod:
		if (p.PeriodDay != PeriodMonth && (p.PeriodDay < -7 || p.PeriodDay > -1)) || p.PeriodHour < 0 || p.PeriodHour > 23 {
			return false
		}
	default:
		return false
	}
	return p.Min == nil || p.Max == nil || *p.Min <= *p.Max
}

// LockedUntil is when a rater who last rated at `rated` may rate again: the zero time when the
// slot has no lock, forever=true for a permanent lock.
func (p RatingPolicy) LockedUntil(rated time.Time) (until time.Time, forever bool) {
	rated = rated.UTC()
	switch p.Lock {
	case LockPermanent:
		return time.Time{}, true
	case LockInterval:
		return rated.Add(time.Duration(p.IntervalSeconds) * time.Second), false
	case LockPeriod:
		h := time.Duration(p.PeriodHour) * time.Hour
		y, m, d := rated.Date()
		if p.PeriodDay == PeriodMonth {
			return time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC).Add(h), false
		}
		want := time.Weekday(-p.PeriodDay % 7) // -1 Monday .. -6 Saturday, -7 Sunday
		ahead := (int(want) - int(rated.Weekday()) + 7) % 7
		if ahead == 0 {
			ahead = 7 // the following week's, never the rating day itself
		}
		return time.Date(y, m, d+ahead, 0, 0, 0, 0, time.UTC).Add(h), false
	}
	return time.Time{}, false
}

// Rating is one slot of one object, with one rater's row when asked for.
type Rating struct {
	ID     int64
	Slot   int32
	Total  int64
	Count  int64
	Policy RatingPolicy
	// The rater's row (Rated=false: none).
	Rated      bool
	RaterValue int64
	RaterCount int64
	RatedAt    time.Time
}

// Lock is the rater's lock at now: zero time and false when it may rate.
func (r Rating) Lock(now time.Time) (until time.Time, forever bool) {
	if !r.Rated {
		return time.Time{}, false
	}
	until, forever = r.Policy.LockedUntil(r.RatedAt)
	if !forever && !until.After(now) {
		return time.Time{}, false
	}
	return until, forever
}

const slotCols = `object_id, slot, total, rating_count, initial_value, lock_policy, interval_seconds, period_day, period_hour, range_min, range_max, modifiable, round_minus, no_self_rating`

func slotDst(r *Rating) []any {
	p := &r.Policy
	return []any{&r.ID, &r.Slot, &r.Total, &r.Count, &p.Initial, &p.Lock, &p.IntervalSeconds, &p.PeriodDay, &p.PeriodHour, &p.Min, &p.Max, &p.Modifiable, &p.RoundMinus, &p.NoSelf}
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func insertSlot(ctx context.Context, q execer, id int64, slot int32, p RatingPolicy) error {
	_, err := q.Exec(ctx, `INSERT INTO rating_slots (object_id, slot, total, initial_value, lock_policy, interval_seconds, period_day, period_hour, range_min, range_max, modifiable, round_minus, no_self_rating)
		VALUES ($1,$2,$3,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`,
		id, slot, p.Initial, p.Lock, p.IntervalSeconds, p.PeriodDay, p.PeriodHour, p.Min, p.Max, p.Modifiable, p.RoundMinus, p.NoSelf)
	return err
}

// InitRating creates the slot unless it exists (the first policy wins) and returns it.
func (s *Store) InitRating(ctx context.Context, id int64, slot int32, p RatingPolicy) (Rating, error) {
	if err := insertSlot(ctx, s.DB, id, slot, p); err != nil {
		return Rating{}, err
	}
	var r Rating
	err := s.DB.QueryRow(ctx, `SELECT `+slotCols+` FROM rating_slots WHERE object_id=$1 AND slot=$2`, id, slot).Scan(slotDst(&r)...)
	return r, err
}

// Rate is one RateObject: the value is already rounded and range-checked by Apply.
type Rate struct {
	ID     int64
	Slot   int32
	Rater  string
	Value  int64
	Policy *RatingPolicy // creates the slot when missing
	// Check runs under the slot's lock on the slot's policy before anything is written; it returns
	// the value to apply or an error that aborts the call (range, self-rating).
	Check func(RatingPolicy) (int64, error)
}

// RateObject applies one rating under the slot's row lock. locked=true: the rater may not rate
// yet; the slot is returned unchanged.
func (s *Store) RateObject(ctx context.Context, q Rate) (r Rating, locked bool, err error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback(ctx)
	load := func() error {
		return tx.QueryRow(ctx, `SELECT `+slotCols+` FROM rating_slots WHERE object_id=$1 AND slot=$2 FOR UPDATE`, q.ID, q.Slot).Scan(slotDst(&r)...)
	}
	err = load()
	if errors.Is(err, pgx.ErrNoRows) {
		if q.Policy == nil {
			return r, false, ErrNoRatingSlot
		}
		if err = insertSlot(ctx, tx, q.ID, q.Slot, *q.Policy); err == nil {
			err = load()
		}
	}
	if err != nil {
		return r, false, err
	}
	value := q.Value
	if q.Check != nil {
		if value, err = q.Check(r.Policy); err != nil {
			return r, false, err
		}
	}
	err = tx.QueryRow(ctx, `SELECT value, times, rated FROM ratings WHERE object_id=$1 AND slot=$2 AND rater=$3 FOR UPDATE`, q.ID, q.Slot, q.Rater).
		Scan(&r.RaterValue, &r.RaterCount, &r.RatedAt)
	switch {
	case err == nil:
		r.Rated = true
	case !errors.Is(err, pgx.ErrNoRows):
		return r, false, err
	}
	now := Now()
	if until, forever := r.Lock(now); forever || !until.IsZero() {
		return r, true, nil
	}
	// A modifiable slot replaces the rater's value (its rating is still one rating); otherwise
	// every call is one more rating.
	dTotal, dCount := value, int64(1)
	newValue, newTimes := r.RaterValue+value, r.RaterCount+1
	if r.Rated && r.Policy.Modifiable {
		dTotal, dCount = value-r.RaterValue, 0
		newValue, newTimes = value, r.RaterCount
	}
	if err = tx.QueryRow(ctx, `UPDATE rating_slots SET total = total + $3, rating_count = rating_count + $4 WHERE object_id=$1 AND slot=$2 RETURNING total, rating_count`,
		q.ID, q.Slot, dTotal, dCount).Scan(&r.Total, &r.Count); err != nil {
		return r, false, wrapOverflow(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ratings (object_id, slot, rater, value, times, rated) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (object_id, slot, rater) DO UPDATE SET value=$4, times=$5, rated=$6`, q.ID, q.Slot, q.Rater, newValue, newTimes, now); err != nil {
		return r, false, wrapOverflow(err)
	}
	r.Rated, r.RaterValue, r.RaterCount, r.RatedAt = true, newValue, newTimes, now
	return r, false, tx.Commit(ctx)
}

// Ratings lists the slots of these objects (every slot when slots is empty), by id then slot,
// with rater's rows when rater is set.
func (s *Store) Ratings(ctx context.Context, ids []int64, slots []int32, rater string) ([]Rating, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if slots == nil {
		slots = []int32{}
	}
	rows, err := s.DB.Query(ctx, `SELECT `+"s."+strings.ReplaceAll(slotCols, ", ", ", s.")+`, r.value, r.times, r.rated
		FROM rating_slots s LEFT JOIN ratings r ON r.object_id = s.object_id AND r.slot = s.slot AND r.rater = $3 AND $3 <> ''
		WHERE s.object_id = ANY($1) AND (cardinality($2::int[]) = 0 OR s.slot = ANY($2))
		ORDER BY s.object_id, s.slot`, ids, slots, rater)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rating
	for rows.Next() {
		var r Rating
		var v, n *int64
		var at *time.Time
		if err := rows.Scan(append(slotDst(&r), &v, &n, &at)...); err != nil {
			return nil, err
		}
		if v != nil {
			r.Rated, r.RaterValue, r.RaterCount, r.RatedAt = true, *v, *n, *at
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ResetRatings puts slots of one object back to their initial value and deletes every rater's
// row; the answer is the number of ratings rows removed.
func (s *Store) ResetRatings(ctx context.Context, id int64, slots []int32) (int64, error) {
	if slots == nil {
		slots = []int32{}
	}
	var n int64
	err := s.DB.QueryRow(ctx, `WITH l AS (SELECT slot FROM rating_slots WHERE object_id=$1 AND (cardinality($2::int[]) = 0 OR slot = ANY($2)) ORDER BY slot FOR UPDATE),
		d AS (DELETE FROM ratings WHERE object_id=$1 AND slot IN (SELECT slot FROM l) RETURNING 1),
		u AS (UPDATE rating_slots SET total = initial_value, rating_count = 0 WHERE object_id=$1 AND slot IN (SELECT slot FROM l) RETURNING 1)
		SELECT count(*) FROM d`, id, slots).Scan(&n)
	return n, err
}

// ObjectHead is what a visibility check needs of an object.
type ObjectHead struct {
	ID                int64
	NS                string
	Owner             string
	Visibility        string
	Recipients        []string
	RecipientsFriends bool
}

// ObjectHeads loads the live objects among ids, in id order.
func (s *Store) ObjectHeads(ctx context.Context, ids []int64) ([]ObjectHead, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, ns, owner, visibility, recipients, recipients_friends FROM objects WHERE id = ANY($1) AND (expires_at IS NULL OR expires_at > now()) ORDER BY id`, ids)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ObjectHead])
}
