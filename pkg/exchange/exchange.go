// Package exchange is the provider-neutral contract order-service trades through.
// order-service depends on the Exchange interface; a concrete provider (mock,
// bybit, …) is injected at startup. The delta-neutral, two-leg choreography stays
// in order-service — an Exchange only places one order and classifies its errors.
package exchange

import "context"

const (
	CategorySpot   = "spot"
	CategoryLinear = "linear"
)

const (
	SideBuy  = "Buy"
	SideSell = "Sell"
)

// OrderRequest is one leg: a market order in a single category. Numeric fields are
// strings to carry exact decimals.
type OrderRequest struct {
	Category    string
	Symbol      string
	Side        string
	Qty         string
	OrderLinkID string // idempotency key; the provider dedupes on it
	ReduceOnly  bool   // perp only: may only shrink an existing position
}

type OrderResult struct {
	OrderID     string
	OrderLinkID string
	Price       float64 // fill price; 0 if the provider doesn't report it synchronously
	Fee         float64 // fee paid on this fill, in quote currency
	FilledQty   float64
}

// ErrorKind buckets a provider error into the handling cases order-service acts on,
// so the leg orchestration never reads provider-specific codes.
type ErrorKind int

const (
	ErrOther     ErrorKind = iota // unclassified; retried by default
	ErrDuplicate                  // OrderLinkID already placed — idempotent replay
	ErrTerminal                   // won't succeed on retry (permission, balance) → record + stop
	ErrTransient                  // transport blip → redelivery may succeed
)

type Exchange interface {
	PlaceOrder(ctx context.Context, req OrderRequest) (*OrderResult, error)
	Classify(err error) ErrorKind
}
