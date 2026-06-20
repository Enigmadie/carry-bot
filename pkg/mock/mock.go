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
	failCategory string // CategorySpot | CategoryLinear | "" — leg to fail, for rollback testing

	mu       sync.Mutex
	seen     map[string]string // orderLinkId -> orderId
	fundingN int               // monotonic settlement counter, for deterministic ids
}

// New builds a mock. failCategory ("spot"/"linear"/"") forces that leg to fail
// terminally so callers can exercise rollback/unbalanced handling.
func New(failCategory string) *Exchange {
	return &Exchange{failCategory: failCategory, seen: map[string]string{}}
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
