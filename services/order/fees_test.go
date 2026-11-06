package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

// feeHL adds Hyperliquid's after-the-fact fee reporting to hlLike: the first
// `lag` lookups find nothing yet, as if the fills weren't indexed.
type feeHL struct {
	*hlLike
	fees  map[string]float64 // by order id
	lag   int
	err   error
	calls int
}

func (f *feeHL) OrderFees(_ context.Context, ids []string) (map[string]float64, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]float64{}
	if f.calls <= f.lag {
		return out, nil
	}
	for _, id := range ids {
		if fee, ok := f.fees[id]; ok {
			out[id] = fee
		}
	}
	return out, nil
}

func lastReport(t *testing.T, js *recordJS) events.ExecReport {
	t.Helper()
	var r events.ExecReport
	if err := json.Unmarshal(js.payloads[len(js.payloads)-1], &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOpenReportCarriesLookedUpFees(t *testing.T) {
	feeLookupBackoff = time.Millisecond
	ex := &feeHL{
		hlLike: &hlLike{spotFee: 0.0007, seen: map[string]bool{}},
		fees:   map[string]float64{"oid-o1-s": 0.0166, "oid-o1-p": 0.0107},
		lag:    1,
	}
	s, js := newTestService(ex)

	if err := s.openPosition(context.Background(), events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := lastSubject(js); got != events.SubjPositionOpened {
		t.Fatalf("last published %q, want %q", got, events.SubjPositionOpened)
	}
	if r := lastReport(t, js); math.Abs(r.Fee-0.0273) > 1e-12 {
		t.Fatalf("report fee = %v, want 0.0273", r.Fee)
	}
	if ex.calls != 2 {
		t.Fatalf("lookups = %d, want 2 (one miss, one hit)", ex.calls)
	}
}

func TestFeeLookupFailureStillReportsClose(t *testing.T) {
	feeLookupBackoff = time.Millisecond
	ex := &feeHL{
		hlLike: &hlLike{spotFee: 0.0007, seen: map[string]bool{}},
		err:    errors.New("lookup api.hyperliquid.xyz: server misbehaving"),
	}
	s, js := newTestService(ex)
	ctx := context.Background()

	if err := s.openPosition(ctx, events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.closePosition(ctx, events.Intent{ID: "c1", Symbol: "HYPEUSDC", Side: events.IntentClose}); err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.halted {
		t.Fatal("a failed fee lookup must not halt trading")
	}
	if got := lastSubject(js); got != events.SubjPositionClosed {
		t.Fatalf("last published %q, want %q", got, events.SubjPositionClosed)
	}
	if r := lastReport(t, js); r.Fee != 0 {
		t.Fatalf("report fee = %v, want 0 when lookup fails", r.Fee)
	}
	if ex.calls != 2*feeLookupAttempts {
		t.Fatalf("lookups = %d, want %d", ex.calls, 2*feeLookupAttempts)
	}
}
