package main

import (
	"math"
	"testing"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/events"
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

func TestRebalanceDelta(t *testing.T) {
	cfg := config{
		TargetMargin: 0.6, RebalanceLow: 0.25, RebalanceHigh: 1.2,
		RebalanceMinNtl: 11, RebalanceStep: 0.01,
	}
	noGrow := cfg
	noGrow.RebalanceHigh = 0

	cases := []struct {
		name       string
		cfg        config
		perpSize   float64
		collateral float64
		spot, perp float64
		want       float64
	}{
		// 0.3 short at 68.4 backed by 48.5: ratio 2.36, far above the band.
		{"idle margin grows the pair", cfg, -0.3, 48.485, 68.434, 68.406, 0.33},
		{"growing disabled", noGrow, -0.3, 48.485, 68.434, 68.406, 0},
		// 0.63 short opened at 68.4 with 25.9 of margin, price +28% → ratio 0.25.
		{"drained margin shrinks the pair", cfg, -0.63, 13.79, 87.6, 87.6, -0.14},
		{"inside the band", cfg, -0.63, 25.9, 68.4, 68.4, 0},
		{"move below the exchange minimum", cfg, -0.2, 3.3, 68.4, 68.4, 0},
		{"no short held", cfg, 0, 48.485, 68.4, 68.4, 0},
		{"no prices yet", cfg, -0.3, 48.485, 0, 0, 0},
	}
	for _, tc := range cases {
		a := events.AccountState{PerpSize: tc.perpSize, Collateral: tc.collateral}
		got, _ := rebalanceDelta(tc.cfg, a, tc.spot, tc.perp)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: delta = %v, want %v", tc.name, got, tc.want)
			continue
		}
		if got == 0 {
			continue
		}
		// A resize must land at the target, never past it on the unsafe side.
		after := (tc.collateral - got*tc.spot) / ((-tc.perpSize + got) * tc.perp)
		if after < tc.cfg.TargetMargin-1e-9 || after > tc.cfg.TargetMargin+0.05 {
			t.Errorf("%s: ratio after resize = %.4f, want just above %.2f", tc.name, after, tc.cfg.TargetMargin)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	ok := config{TargetMargin: 0.6, RebalanceLow: 0.25, RebalanceHigh: 1.2, RebalanceStep: 0.01}
	if err := ok.validate(); err != nil {
		t.Fatalf("valid band rejected: %v", err)
	}
	if err := (config{}).validate(); err != nil {
		t.Fatalf("disabled rebalancing rejected: %v", err)
	}
	for name, c := range map[string]config{
		"low above target":  {TargetMargin: 0.6, RebalanceLow: 0.7, RebalanceStep: 0.01},
		"high below target": {TargetMargin: 0.6, RebalanceLow: 0.25, RebalanceHigh: 0.5, RebalanceStep: 0.01},
		"zero step":         {TargetMargin: 0.6, RebalanceLow: 0.25},
	} {
		if err := c.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
