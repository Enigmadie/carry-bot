package main

import (
	"context"
	"math"
	"testing"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

func openedService(t *testing.T, ex *hlLike) (*service, *recordJS) {
	t.Helper()
	s, js := newTestService(ex)
	if err := s.openPosition(context.Background(), events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	return s, js
}

func resize(qty float64) events.Intent {
	return events.Intent{ID: "r1", Symbol: "HYPEUSDC", Side: events.IntentResize, Qty: qty}
}

func TestResizeGrowAndShrinkKeepLegsEqual(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, js := openedService(t, ex)
	ctx := context.Background()

	if err := s.resizePosition(ctx, resize(0.5)); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if got := lastSubject(js); got != events.SubjPositionResized {
		t.Fatalf("last published %q, want %q", got, events.SubjPositionResized)
	}
	if r := lastReport(t, js); r.Qty != 0.5 {
		t.Fatalf("grow reported qty %v, want 0.5", r.Qty)
	}
	if ex.perp != -1.5 || s.heldQty != 1.5 {
		t.Fatalf("after grow: perp=%v held=%v, want -1.5 / 1.5", ex.perp, s.heldQty)
	}

	shrink := resize(-0.7)
	shrink.ID = "r2"
	if err := s.resizePosition(ctx, shrink); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if r := lastReport(t, js); r.Qty != -0.7 {
		t.Fatalf("shrink reported qty %v, want -0.7", r.Qty)
	}
	if math.Abs(ex.perp+0.8) > 1e-12 || math.Abs(s.heldQty-0.8) > 1e-12 {
		t.Fatalf("after shrink: perp=%v held=%v, want -0.8 / 0.8", ex.perp, s.heldQty)
	}

	// The close must take out the resized size, not ORDER_QTY.
	if err := s.closePosition(ctx, events.Intent{ID: "c1", Symbol: "HYPEUSDC", Side: events.IntentClose}); err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.halted || ex.perp != 0 || math.Abs(ex.spot) > 1e-12 {
		t.Fatalf("not flat after close: halted=%v perp=%v spot=%v", s.halted, ex.perp, ex.spot)
	}
}

func TestResizeGrowRollsBackSpotWhenPerpFails(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, js := openedService(t, ex)
	spotBefore := ex.spot
	ex.failPerp = true

	if err := s.resizePosition(context.Background(), resize(0.5)); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if s.halted {
		t.Fatal("halted: rollback left the spot leg oversized")
	}
	if got := lastSubject(js); got != events.SubjExecFailed {
		t.Fatalf("last published %q, want %q", got, events.SubjExecFailed)
	}
	// The rollback sells what was ordered; the buy fee stays behind as a deficit.
	if ex.perp != -1 || s.heldQty != 1 || spotBefore-ex.spot > 0.5*0.0007+1e-12 {
		t.Fatalf("after rollback: perp=%v held=%v spot=%v (was %v)", ex.perp, s.heldQty, ex.spot, spotBefore)
	}
}

func TestResizeReplayDoesNotTradeTwice(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, _ := openedService(t, ex)
	ctx := context.Background()

	if err := s.resizePosition(ctx, resize(0.5)); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if err := s.resizePosition(ctx, resize(0.5)); err != nil {
		t.Fatalf("replayed grow: %v", err)
	}
	if ex.perp != -1.5 {
		t.Fatalf("perp=%v after a replayed resize, want -1.5", ex.perp)
	}
}

func TestResizeRejectsWholePositionShrink(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, js := openedService(t, ex)

	if err := s.resizePosition(context.Background(), resize(-1)); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if got := lastSubject(js); got != events.SubjExecFailed {
		t.Fatalf("last published %q, want %q", got, events.SubjExecFailed)
	}
	if ex.perp != -1 {
		t.Fatalf("perp=%v, a rejected resize must not trade", ex.perp)
	}
}
