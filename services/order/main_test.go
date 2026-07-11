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

func TestIsOrphanSpot(t *testing.T) {
	const qty = 1.0
	cases := []struct {
		name string
		st   exchange.PositionState
		want bool
	}{
		{"perp ADLed, spot whole", exchange.PositionState{SpotSize: 1.0}, true},
		{"spot fee taken in base", exchange.PositionState{SpotSize: 0.9993}, true},
		{"spot with fee dust on top", exchange.PositionState{SpotSize: 1.0079}, true},
		{"residual perp dust", exchange.PositionState{PerpSize: -0.05, SpotSize: 1.0}, true},
		{"spot above tolerance — not our position", exchange.PositionState{SpotSize: 1.2}, false},
		{"partial spot", exchange.PositionState{SpotSize: 0.5}, false},
		{"perp still held", exchange.PositionState{PerpSize: -1.0, SpotSize: 1.0}, false},
		{"perp long", exchange.PositionState{PerpSize: 1.0, SpotSize: 1.0}, false},
		{"resting order", exchange.PositionState{SpotSize: 1.0, OpenOrders: 1}, false},
	}
	for _, tc := range cases {
		if got := isOrphanSpot(&tc.st, qty); got != tc.want {
			t.Errorf("%s: isOrphanSpot(%+v) = %v, want %v", tc.name, tc.st, got, tc.want)
		}
	}
}
