package service

import (
	"errors"
	"testing"
)

func TestParseMetaRange(t *testing.T) {
	r, err := ParseMetaRange(`{"plays":{"ge":10},"data_type":{"ge":1,"le":9223372036854775807}}`)
	if err != nil || len(r) != 2 {
		t.Fatalf("%v %+v", err, r)
	}
	// Sorted by key; bounds kept as decimal strings (no float rounding of int64).
	if r[0].Key != "data_type" || r[0].Min != "1" || r[0].Max != "9223372036854775807" || r[1].Key != "plays" || r[1].Min != "10" || r[1].Max != "" {
		t.Fatalf("%+v", r)
	}
	if r, err := ParseMetaRange(""); err != nil || r != nil {
		t.Fatalf("empty: %v %+v", err, r)
	}
	for _, bad := range []string{
		`[1]`, `{"a":1}`, `{"a":{"gt":1}}`, `{"a":{"ge":"1"}}`, `{"a b":{"ge":1}}`, `{"a":{"ge":1}} {}`,
		`{"a":{"ge":1e999999}}`, `{"x');--":{"ge":1}}`,
	} {
		if _, err := ParseMetaRange(bad); !errors.Is(err, ErrBadRequest) {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if _, err := ParseMetaRange(`{"a":{"ge":1},"b":{"ge":1},"c":{"ge":1},"d":{"ge":1},"e":{"ge":1},"f":{"ge":1},"g":{"ge":1},"h":{"ge":1},"i":{"ge":1}}`); !errors.Is(err, ErrTooLarge) {
		t.Errorf("nine keys: %v", err)
	}
}

func TestObjSort(t *testing.T) {
	for _, s := range []string{"", "updated", "created", "size", "name", "id"} {
		if sl, _, err := objSort(s); err != nil || sl != nil {
			t.Errorf("%q: %v %v", s, sl, err)
		}
	}
	if sl, avg, err := objSort("rating:3"); err != nil || sl == nil || *sl != 3 || avg {
		t.Errorf("rating:3: %v %v %v", sl, avg, err)
	}
	if sl, avg, err := objSort("rating_avg:255"); err != nil || sl == nil || *sl != 255 || !avg {
		t.Errorf("rating_avg:255: %v %v %v", sl, avg, err)
	}
	for _, bad := range []string{"bogus", "rating:", "rating:-1", "rating:256", "rating:1x", "rating_avg:99999999999"} {
		if _, _, err := objSort(bad); !errors.Is(err, ErrBadRequest) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}
