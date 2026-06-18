// Package mock is a deterministic in-memory Exchange for building out portfolio,
// reconciliation, and observability without any network, keys, or geo-gate. It
// accepts legs and returns synthetic fills; a repeated OrderLinkID surfaces as a
// duplicate (so the idempotency path is exercised), and FailCategory injects a
// terminal failure to drive leg-risk rollback paths.
package mock

import (
	"context"
	"errors"
	"sync"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

var (
	errDuplicate = errors.New("mock: duplicate orderLinkId")
	errInjected  = errors.New("mock: injected leg failure")
)

type Exchange struct {
	failCategory string // CategorySpot | CategoryLinear | "" — leg to fail, for rollback testing

	mu   sync.Mutex
	seen map[string]string // orderLinkId -> orderId
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
	return &exchange.OrderResult{OrderID: orderID, OrderLinkID: req.OrderLinkID}, nil
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
