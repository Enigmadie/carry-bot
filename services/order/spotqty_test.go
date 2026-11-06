package main

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

var (
	errBalance  = errors.New("insufficient spot balance")
	errDupCloid = errors.New("cloid already used")
	errPerpDown = errors.New("perp rejected")
)

// hlLike models the Hyperliquid behaviour spot sizing depends on: the spot buy
// fee comes out of the received token, a sell above the balance is rejected,
// and a reused cloid is a duplicate.
type hlLike struct {
	spot, perp float64
	spotFee    float64
	failPerp   bool
	seen       map[string]bool
}

func (f *hlLike) PlaceOrder(_ context.Context, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	if f.seen[req.OrderLinkID] {
		return nil, errDupCloid
	}
	qty, err := strconv.ParseFloat(req.Qty, 64)
	if err != nil {
		return nil, err
	}
	switch {
	case req.Category == exchange.CategorySpot && req.Side == exchange.SideBuy:
		f.spot += qty * (1 - f.spotFee)
	case req.Category == exchange.CategorySpot:
		if qty > f.spot {
			return nil, errBalance
		}
		f.spot -= qty
	case f.failPerp:
		return nil, errPerpDown
	case req.Side == exchange.SideSell:
		f.perp -= qty
	default:
		f.perp += qty
	}
	f.seen[req.OrderLinkID] = true
	return &exchange.OrderResult{OrderID: "oid-" + req.OrderLinkID, OrderLinkID: req.OrderLinkID, FilledQty: qty}, nil
}

func (f *hlLike) Classify(err error) exchange.ErrorKind {
	switch {
	case errors.Is(err, errDupCloid):
		return exchange.ErrDuplicate
	case errors.Is(err, errBalance), errors.Is(err, errPerpDown):
		return exchange.ErrTerminal
	}
	return exchange.ErrOther
}

func (f *hlLike) Funding(context.Context, string, time.Time) ([]exchange.FundingPayment, error) {
	return nil, nil
}

func (f *hlLike) State(context.Context, string) (*exchange.PositionState, error) {
	return &exchange.PositionState{PerpSize: f.perp, SpotSize: f.spot}, nil
}

// recordJS records published subjects. The embedded nil interface satisfies
// the rest of jetstream.JetStream; any other call panics, which is what a test
// should do if the code under test starts using it.
type recordJS struct {
	jetstream.JetStream
	subjects []string
	payloads [][]byte
}

func (r *recordJS) Publish(_ context.Context, subj string, data []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	r.subjects = append(r.subjects, subj)
	r.payloads = append(r.payloads, data)
	return &jetstream.PubAck{}, nil
}

func newTestService(ex exchange.Exchange) (*service, *recordJS) {
	js := &recordJS{}
	return &service{
		log: slog.New(slog.DiscardHandler),
		js:  js,
		ex:  ex,
		cfg: config{OrderQty: "1"},
	}, js
}

func lastSubject(js *recordJS) string {
	if len(js.subjects) == 0 {
		return ""
	}
	return js.subjects[len(js.subjects)-1]
}

func TestSpotSellSize(t *testing.T) {
	cases := []struct {
		name          string
		held, ordered float64
		want          float64
	}{
		{"fee taken in base", 0.9993, 1, 0.9993},
		{"foreign balance on top stays", 1.5, 1, 1},
		{"already sold → replay ordered qty", 0, 1, 1},
		{"dust → replay ordered qty", 0.05, 1, 1},
	}
	for _, tc := range cases {
		if got := spotSellSize(tc.held, tc.ordered); got != tc.want {
			t.Errorf("%s: spotSellSize(%v, %v) = %v, want %v", tc.name, tc.held, tc.ordered, got, tc.want)
		}
	}
}

func TestRoundTripSellsHeldSpot(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, js := newTestService(ex)
	ctx := context.Background()

	if err := s.openPosition(ctx, events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.closePosition(ctx, events.Intent{ID: "c1", Symbol: "HYPEUSDC", Side: events.IntentClose}); err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.halted {
		t.Fatal("halted after a clean close")
	}
	if got := lastSubject(js); got != events.SubjPositionClosed {
		t.Fatalf("last published %q, want %q", got, events.SubjPositionClosed)
	}
	if ex.perp != 0 || math.Abs(ex.spot) > 1e-12 {
		t.Fatalf("not flat after close: perp=%v spot=%v", ex.perp, ex.spot)
	}
}

func TestCloseReplayAfterBothLegsDone(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, seen: map[string]bool{}}
	s, js := newTestService(ex)
	ctx := context.Background()
	close1 := events.Intent{ID: "c1", Symbol: "HYPEUSDC", Side: events.IntentClose}

	if err := s.openPosition(ctx, events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.closePosition(ctx, close1); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Redelivery after the legs went through but before the ack landed.
	s.positionOpen.Store(true)
	if err := s.closePosition(ctx, close1); err != nil {
		t.Fatalf("replayed close: %v", err)
	}
	if s.halted {
		t.Fatal("replayed close halted instead of deduping")
	}
	if got := lastSubject(js); got != events.SubjPositionClosed {
		t.Fatalf("last published %q, want %q", got, events.SubjPositionClosed)
	}
}

func TestRollbackSellsHeldSpot(t *testing.T) {
	ex := &hlLike{spotFee: 0.0007, failPerp: true, seen: map[string]bool{}}
	s, js := newTestService(ex)

	if err := s.openPosition(context.Background(), events.Intent{ID: "o1", Symbol: "HYPEUSDC", Side: events.IntentOpen}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if s.halted {
		t.Fatal("halted: rollback left a naked spot long")
	}
	if got := lastSubject(js); got != events.SubjExecFailed {
		t.Fatalf("last published %q, want %q", got, events.SubjExecFailed)
	}
	if math.Abs(ex.spot) > 1e-12 {
		t.Fatalf("spot not rolled back: %v", ex.spot)
	}
}
