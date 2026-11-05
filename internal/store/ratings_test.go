package store

import (
	"testing"
	"time"
)

func TestRatingLocks(t *testing.T) {
	// Thursday 2024-03-07 15:00 UTC.
	at := time.Date(2024, 3, 7, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		p       RatingPolicy
		want    time.Time
		forever bool
	}{
		{RatingPolicy{Lock: LockNone}, time.Time{}, false},
		{RatingPolicy{Lock: LockPermanent}, time.Time{}, true},
		{RatingPolicy{Lock: LockInterval, IntervalSeconds: 90}, at.Add(90 * time.Second), false},
		// The next Tuesday after the rating day, at the hour (the NEX example: 2024-03-12 00:00).
		{RatingPolicy{Lock: LockPeriod, PeriodDay: -2}, time.Date(2024, 3, 12, 0, 0, 0, 0, time.UTC), false},
		// Rated on a Thursday with a Thursday boundary: the following week's, never the same day.
		{RatingPolicy{Lock: LockPeriod, PeriodDay: -4, PeriodHour: 18}, time.Date(2024, 3, 14, 18, 0, 0, 0, time.UTC), false},
		{RatingPolicy{Lock: LockPeriod, PeriodDay: -7, PeriodHour: 5}, time.Date(2024, 3, 10, 5, 0, 0, 0, time.UTC), false},
		{RatingPolicy{Lock: LockPeriod, PeriodDay: PeriodMonth, PeriodHour: 3}, time.Date(2024, 4, 1, 3, 0, 0, 0, time.UTC), false},
	}
	for _, c := range cases {
		got, forever := c.p.LockedUntil(at)
		if !got.Equal(c.want) || forever != c.forever {
			t.Errorf("%+v: %v %v, want %v %v", c.p, got, forever, c.want, c.forever)
		}
	}
	// December rolls into January.
	if got, _ := (RatingPolicy{Lock: LockPeriod, PeriodDay: PeriodMonth}).LockedUntil(time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC)); !got.Equal(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("december: %v", got)
	}
	// A lock in the past no longer holds; an unrated rater is never locked.
	r := Rating{Rated: true, RatedAt: at, Policy: RatingPolicy{Lock: LockInterval, IntervalSeconds: 60}}
	if u, f := r.Lock(at.Add(30 * time.Second)); u.IsZero() || f {
		t.Error("interval lock should hold")
	}
	if u, f := r.Lock(at.Add(60 * time.Second)); !u.IsZero() || f {
		t.Error("interval lock should have expired")
	}
	if u, f := (Rating{Policy: RatingPolicy{Lock: LockPermanent}}).Lock(at); !u.IsZero() || f {
		t.Error("unrated rater locked")
	}
}

func TestRatingPolicyValid(t *testing.T) {
	one, two := int64(1), int64(2)
	ok := []RatingPolicy{
		{Lock: LockNone}, {Lock: LockPermanent}, {Lock: LockInterval, IntervalSeconds: 1},
		{Lock: LockPeriod, PeriodDay: -1, PeriodHour: 23}, {Lock: LockPeriod, PeriodDay: PeriodMonth},
		{Lock: LockNone, Min: &one, Max: &two}, {Lock: LockNone, Min: &two},
	}
	for _, p := range ok {
		if !p.Valid() {
			t.Errorf("refused %+v", p)
		}
	}
	bad := []RatingPolicy{
		{Lock: ""}, {Lock: "daily"}, {Lock: LockInterval}, {Lock: LockPeriod, PeriodDay: 0},
		{Lock: LockPeriod, PeriodDay: -8}, {Lock: LockPeriod, PeriodDay: -1, PeriodHour: 24}, {Lock: LockNone, Min: &two, Max: &one},
	}
	for _, p := range bad {
		if p.Valid() {
			t.Errorf("accepted %+v", p)
		}
	}
}
