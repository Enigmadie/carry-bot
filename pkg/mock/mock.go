// Package mock is a deterministic in-memory Exchange for building out portfolio,
// reconciliation, and observability without any network, keys, or geo-gate. It
// accepts legs and returns synthetic fills; a repeated OrderLinkID surfaces as a
// duplicate (so the idempotency path is exercised), and FailCategory injects a
// terminal failure to drive leg-risk rollback paths.
package mock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

// Synthetic fills. Both legs price at the same point so a delta-neutral round
// trip nets zero on price and the only realized cost is the fee — which is the
// honest picture: the strategy earns from funding (phase 2), not price moves.
const (
	mockPrice   = 65000.0
	mockFeeRate = 0.0005
)

// Synthetic funding. We hold the perp short, so a positive funding rate credits
// us — each poll books one settlement at this rate over a nominal notional, so
// P&L drifts the right way (up) over time. The mock keeps no position, so it
// emits funding unconditionally; portfolio attaches it to the open position and
// drops it when flat.
const (
	mockFundingRate = 0.0001 // 0.01% per settlement
	mockFundingQty  = 0.001  // nominal base-coin notional for the synthetic accrual
)

var (
	errDuplicate = errors.New("mock: duplicate orderLinkId")
	errInjected  = errors.New("mock: injected leg failure")
)

type Exchange struct {
	failCategory string        // CategorySpot | CategoryLinear | "" — leg to fail, for rollback testing
	adlAfter     time.Duration // >0: force-close the perp this long after it opens (ADL simulation)

	mu       sync.Mutex
	seen     map[string]string // orderLinkId -> orderId
	fundingN int               // monotonic settlement counter, for deterministic ids

	// Live position, mutated by fills, so State reflects what was traded — the
	// same contract a real exchange gives reconciliation. In-memory like the
	// rest of the mock: a restart forgets it and reconciles as flat.
	perpSize   float64 // signed; our short goes negative
	spotSize   float64
	perpOpenAt time.Time // when the short opened; adlAfter counts from here
}

// New builds a mock. failCategory ("spot"/"linear"/"") forces that leg to fail
// terminally so callers can exercise rollback/unbalanced handling. adlAfter > 0
// simulates the exchange force-closing the perp (Auto-Deleveraging) that long
// after it opens — the spot leg stays, exactly the orphaned shape runtime
// reconciliation must catch.
func New(failCategory string, adlAfter time.Duration) *Exchange {
	return &Exchange{failCategory: failCategory, adlAfter: adlAfter, seen: map[string]string{}}
}

func (m *Exchange) PlaceOrder(_ context.Context, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	if req.Category == m.failCategory {
		return nil, errInjected
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[req.OrderLinkID]; ok {
		return nil, errDuplicate
	}
	orderID := "mock-" + req.OrderLinkID
	m.seen[req.OrderLinkID] = orderID
	qty, _ := strconv.ParseFloat(req.Qty, 64)

	delta := qty
	if req.Side == exchange.SideSell {
		delta = -qty
	}
	if req.Category == exchange.CategoryLinear {
		wasFlat := m.perpSize == 0
		m.perpSize += delta
		if wasFlat && m.perpSize < 0 {
			m.perpOpenAt = time.Now()
		}
	} else {
		m.spotSize += delta
	}

	return &exchange.OrderResult{
		OrderID:     orderID,
		OrderLinkID: req.OrderLinkID,
		Price:       mockPrice,
		Fee:         qty * mockPrice * mockFeeRate,
		FilledQty:   qty,
	}, nil
}

// Funding returns one synthetic settlement per call, timestamped now (so it is
// always after `since`) with a monotonic id. The amount is a fixed positive
// credit (perp short receives positive funding), so portfolio's funding_total —
// and thus P&L — climbs across polls.
func (m *Exchange) Funding(_ context.Context, symbol string, _ time.Time) ([]exchange.FundingPayment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fundingN++
	return []exchange.FundingPayment{{
		ID:     fmt.Sprintf("mock-funding-%d", m.fundingN),
		Symbol: symbol,
		Amount: mockFundingQty * mockPrice * mockFundingRate,
		Time:   time.Now().UTC(),
	}}, nil
}

// State reports the fill-tracked position, so order-service's startup
// reconciliation sees the same shape of truth a real exchange would give it.
func (m *Exchange) State(_ context.Context, _ string) (*exchange.PositionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// ADL happens on the exchange's clock, not through PlaceOrder, so it is
	// applied lazily here — State is how the drift becomes observable anyway.
	if m.adlAfter > 0 && m.perpSize < 0 && time.Since(m.perpOpenAt) >= m.adlAfter {
		m.perpSize = 0
	}
	return &exchange.PositionState{
		PerpSize:   m.perpSize,
		SpotSize:   m.spotSize,
		Collateral: 1000,
	}, nil
}

func (m *Exchange) Classify(err error) exchange.ErrorKind {
	switch {
	case errors.Is(err, errDuplicate):
		return exchange.ErrDuplicate
	case errors.Is(err, errInjected):
		return exchange.ErrTerminal
	default:
		return exchange.ErrOther
	}
}
