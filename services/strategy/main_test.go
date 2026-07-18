package main

import (
	"testing"
	"time"
)

func TestOpenGate(t *testing.T) {
	cases := []struct {
		name          string
		maxBasis      float64
		spot, perp    float64
		cooldownUntil time.Time
		want          string
	}{
		{"tight basis passes", 0.01, 100, 100.5, time.Time{}, ""},
		{"basis at the limit passes", 0.01, 100, 101, time.Time{}, ""},
		{"dislocated basis blocks", 0.01, 30, 73, time.Time{}, "basis"},
		{"perp under spot also counts", 0.01, 100, 98, time.Time{}, "basis"},
		{"no prices yet blocks", 0.01, 0, 0, time.Time{}, "no-price"},
		{"spot missing blocks", 0.01, 0, 100, time.Time{}, "no-price"},
		{"gate disabled passes anything", 0, 30, 73, time.Time{}, ""},
		{"cooldown blocks", 0.01, 100, 100.5, time.Now().Add(time.Hour), "cooldown"},
		{"cooldown outranks basis", 0.01, 30, 73, time.Now().Add(time.Hour), "cooldown"},
		{"expired cooldown passes", 0.01, 100, 100.5, time.Now().Add(-time.Hour), ""},
	}
	for _, tc := range cases {
		s := &service{
			cfg:           config{MaxBasis: tc.maxBasis},
			spotPrice:     tc.spot,
			perpPrice:     tc.perp,
			cooldownUntil: tc.cooldownUntil,
		}
		if got := s.openGate(); got != tc.want {
			t.Errorf("%s: openGate() = %q, want %q", tc.name, got, tc.want)
		}
	}
}
