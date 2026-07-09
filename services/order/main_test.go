package main

import (
	"testing"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

func TestClassifyState(t *testing.T) {
	const qty = 1.0
	cases := []struct {
		name string
		st   exchange.PositionState
		want string
	}{
		{"flat", exchange.PositionState{}, events.ReconcileFlat},
		{"flat with dust", exchange.PositionState{PerpSize: -0.05, SpotSize: 0.05}, events.ReconcileFlat},
		{"open exact", exchange.PositionState{PerpSize: -1.0, SpotSize: 1.0}, events.ReconcileOpen},
		{"open with spot fee taken in base", exchange.PositionState{PerpSize: -1.0, SpotSize: 0.97}, events.ReconcileOpen},
		{"naked spot long", exchange.PositionState{SpotSize: 1.0}, events.ReconcileUnbalanced},
		{"naked perp short", exchange.PositionState{PerpSize: -1.0}, events.ReconcileUnbalanced},
		{"partial leg", exchange.PositionState{PerpSize: -1.0, SpotSize: 0.5}, events.ReconcileUnbalanced},
		{"perp long is never ours", exchange.PositionState{PerpSize: 1.0, SpotSize: 1.0}, events.ReconcileUnbalanced},
		{"resting order on a flat book", exchange.PositionState{OpenOrders: 1}, events.ReconcileUnbalanced},
	}
	for _, tc := range cases {
		if got := classifyState(&tc.st, qty); got != tc.want {
			t.Errorf("%s: classifyState(%+v) = %q, want %q", tc.name, tc.st, got, tc.want)
		}
	}
}
