package service

import (
	"context"
	"strings"

	"github.com/openpak/title-storage/internal/store"
)

// MaxRatingSlot is the highest rating slot (NEX uses 0-15).
const MaxRatingSlot = 255

func validSlot(s int32) bool { return s >= 0 && s <= MaxRatingSlot }

// object loads an object by id for a ratings call: nil when it does not exist or belongs to
// another network's rows of a per-network title.
func (s *Service) object(ctx context.Context, network string, id int64) (*store.Object, error) {
	o, ns, err := s.Store.ObjectByID(ctx, id, false)
	if err != nil || o == nil {
		return nil, err
	}
	if ns != "" && ns != network {
		return nil, nil
	}
	return o, nil
}

// InitRating creates a rating slot with its policy (an existing slot is returned unchanged). The
// writer follows the write rule for the object's owner. nil: no such object.
func (s *Service) InitRating(ctx context.Context, network, writer string, id int64, slot int32, p store.RatingPolicy) (*store.Rating, error) {
	if p.Lock == "" {
		p.Lock = store.LockNone
	}
	if !validSlot(slot) || !p.Valid() {
		return nil, ErrBadRequest
	}
	if err := validAccount(writer); err != nil {
		return nil, err
	}
	o, err := s.object(ctx, network, id)
	if err != nil || o == nil {
		return nil, err
	}
	if err := s.authorizeWrite(ctx, writer, o.Owner); err != nil {
		return nil, err
	}
	r, err := s.Store.InitRating(ctx, id, slot, p)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// RateObject is one rating by an account. found=false: no such object or the rater cannot read
// it. locked=true: the rater's lock has not expired; nothing changed.
func (s *Service) RateObject(ctx context.Context, network, rater string, id int64, slot int32, value int64, p *store.RatingPolicy) (r store.Rating, found, locked bool, err error) {
	if !validSlot(slot) {
		return r, false, false, ErrBadRequest
	}
	if p != nil {
		if p.Lock == "" {
			p.Lock = store.LockNone
		}
		if !p.Valid() {
			return r, false, false, ErrBadRequest
		}
	}
	if err := validAccount(rater); err != nil {
		return r, false, false, err
	}
	rater = strings.ToLower(rater)
	o, err := s.object(ctx, network, id)
	if err != nil || o == nil {
		return r, false, false, err
	}
	if ok, err := s.canRead(ctx, rater, o); err != nil || !ok {
		return r, false, false, err
	}
	r, locked, err = s.Store.RateObject(ctx, store.Rate{ID: id, Slot: slot, Rater: rater, Value: value, Policy: p,
		Check: func(pol store.RatingPolicy) (int64, error) {
			if pol.NoSelf && rater == o.Owner {
				return 0, ErrSelfRating
			}
			v := value
			if pol.RoundMinus && v < 0 {
				v = 0
			}
			if (pol.Min != nil && v < *pol.Min) || (pol.Max != nil && v > *pol.Max) {
				return 0, ErrRange
			}
			return v, nil
		}})
	if err != nil {
		return r, false, false, err
	}
	return r, true, locked, nil
}

// GetRatings returns the rating slots of the objects the viewer can read (by id, then slot), with
// the rater's rows when rater is set.
func (s *Service) GetRatings(ctx context.Context, network, viewer string, ids []int64, slots []int32, rater string) ([]store.Rating, error) {
	if len(ids) == 0 || len(ids) > MaxLimit || len(slots) > MaxItems {
		return nil, ErrBadRequest
	}
	for _, sl := range slots {
		if !validSlot(sl) {
			return nil, ErrBadRequest
		}
	}
	if viewer != "" {
		if err := validAccount(viewer); err != nil {
			return nil, err
		}
	}
	if rater != "" {
		if err := validAccount(rater); err != nil {
			return nil, err
		}
	}
	visible, err := s.readableIDs(ctx, network, strings.ToLower(viewer), ids)
	if err != nil || len(visible) == 0 {
		return nil, err
	}
	return s.Store.Ratings(ctx, visible, slots, strings.ToLower(rater))
}

// readableIDs keeps the ids of live objects the viewer may read; the friend list is fetched once,
// only when the answer for one of them depends on it.
func (s *Service) readableIDs(ctx context.Context, network, viewer string, ids []int64) ([]int64, error) {
	objs, err := s.Store.ObjectHeads(ctx, ids)
	if err != nil {
		return nil, err
	}
	var friends map[string]bool
	var out []int64
	for _, o := range objs {
		if o.NS != "" && o.NS != network {
			continue
		}
		ok, err := Readable(viewer, Access{Owner: o.Owner, Visibility: o.Visibility, Recipients: o.Recipients, RecipientsFriends: o.RecipientsFriends},
			func() (bool, error) {
				if friends == nil {
					fs, err := s.Core.Friends(ctx, viewer)
					if err != nil {
						return false, err
					}
					friends = map[string]bool{}
					for _, f := range fs {
						friends[strings.ToLower(f)] = true
					}
				}
				return friends[o.Owner], nil
			})
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, o.ID)
		}
	}
	return out, nil
}

// ResetRatings puts an object's slots (all of them when slots is empty) back to their initial
// value with no ratings. The writer follows the write rule for the owner. found=false: no object.
func (s *Service) ResetRatings(ctx context.Context, network, writer string, id int64, slots []int32) (n int64, found bool, err error) {
	for _, sl := range slots {
		if !validSlot(sl) {
			return 0, false, ErrBadRequest
		}
	}
	if err := validAccount(writer); err != nil {
		return 0, false, err
	}
	o, err := s.object(ctx, network, id)
	if err != nil || o == nil {
		return 0, false, err
	}
	if err := s.authorizeWrite(ctx, writer, o.Owner); err != nil {
		return 0, false, err
	}
	n, err = s.Store.ResetRatings(ctx, id, slots)
	return n, true, err
}

// attachRatings fills Object.Ratings for objects the caller has already shown to be readable.
func (s *Service) attachRatings(ctx context.Context, os []store.Object) error {
	if len(os) == 0 {
		return nil
	}
	ids := make([]int64, len(os))
	at := make(map[int64]int, len(os))
	for i, o := range os {
		ids[i], at[o.ID] = o.ID, i
	}
	rs, err := s.Store.Ratings(ctx, ids, nil, "")
	if err != nil {
		return err
	}
	for _, r := range rs {
		i := at[r.ID]
		os[i].Ratings = append(os[i].Ratings, r)
	}
	return nil
}
