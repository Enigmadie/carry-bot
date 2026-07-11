// Package events defines the message contracts shared by every carry-bot
// service. Each NATS payload is one of these structs, encoded as JSON.
package events

import "time"

// NATS subjects. Hierarchical, dot-separated: <domain>.<entity>.<event>.
// v1 trades a single symbol, so the symbol is baked into the subject.
const (
	// Market data — published on core NATS (latest value wins, OK to drop).
	SubjPriceSpot        = "market.price.spot.BTCUSDT"
	SubjPricePerp        = "market.price.perp.BTCUSDT"
	SubjFundingPredicted = "market.funding.predicted.BTCUSDT"

	// Strategy intents — JetStream (durable, must not be lost).
	SubjIntentOpen  = "strategy.intent.open"
	SubjIntentClose = "strategy.intent.close"

	// Execution facts — JetStream.
	SubjPositionOpened  = "exec.position.opened"
	SubjPositionClosed  = "exec.position.closed"
	SubjExecFailed      = "exec.failed"
	SubjFundingReceived = "exec.funding.received"
	SubjReconciled      = "exec.reconciled"

	// Portfolio state — JetStream (last value retained).
	SubjPositionState = "portfolio.position.state"
)

// Price is a single price observation for one instrument.
type Price struct {
	Symbol string    `json:"symbol"`
	Price  float64   `json:"price"`
	Time   time.Time `json:"time"`
}

// FundingRate is the current predicted funding rate for a perpetual.
// Rate is a fraction: 0.0001 means 0.01% per funding interval.
type FundingRate struct {
	Symbol       string    `json:"symbol"`
	Rate         float64   `json:"rate"`
	NextSettleAt time.Time `json:"next_settle_at"`
	Time         time.Time `json:"time"`
}

// Intent sides, carried in Intent.Side.
const (
	IntentOpen  = "open"
	IntentClose = "close"
)

// Intent is a strategy decision to open or close the delta-neutral position.
// It travels over JetStream so it survives a crash; order-service dedupes on ID
// (a repeated at-least-once delivery must not open a second position).
type Intent struct {
	ID        string    `json:"id"` // unique per decision; downstream dedup key
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`   // IntentOpen | IntentClose
	Reason    string    `json:"reason"` // human-readable trigger
	Funding   float64   `json:"funding"`
	PerpPrice float64   `json:"perp_price"`
	SpotPrice float64   `json:"spot_price"`
	Time      time.Time `json:"time"`
}

// ExecReport is a fact emitted by order-service after acting on an Intent: the
// position was opened, closed, or the attempt failed (possibly leaving an
// unbalanced leg). It carries IntentID so portfolio-service can correlate it
// with the originating decision. Published on the exec.* subjects.
type ExecReport struct {
	IntentID    string    `json:"intent_id"`
	Symbol      string    `json:"symbol"`
	Side        string    `json:"side"` // IntentOpen | IntentClose
	Qty         float64   `json:"qty"`
	SpotOrderID string    `json:"spot_order_id,omitempty"`
	PerpOrderID string    `json:"perp_order_id,omitempty"`
	SpotPrice   float64   `json:"spot_price,omitempty"` // fill prices/fees per leg, for portfolio P&L
	PerpPrice   float64   `json:"perp_price,omitempty"`
	Fee         float64   `json:"fee,omitempty"`    // total fee across both legs, quote currency
	Error       string    `json:"error,omitempty"`  // set on SubjExecFailed
	Reason      string    `json:"reason,omitempty"` // why this happened without an intent (e.g. orphan auto-close)
	Time        time.Time `json:"time"`
}

// Reconciliation verdicts, carried in Reconciled.Verdict.
const (
	ReconcileFlat       = "flat"
	ReconcileOpen       = "open"
	ReconcileUnbalanced = "unbalanced"
)

// Reconciled is a snapshot of the exchange's live position, emitted by
// order-service after comparing it against the configured order size: at every
// startup, and at runtime when the periodic reconcile sees the exchange
// disagree with internal state (e.g. a position force-closed by ADL). It is
// the single point where exchange truth enters the event flow:
// strategy initialises its state machine from it and portfolio checks its
// ledger against it. An unbalanced verdict means order-service is halted.
type Reconciled struct {
	Symbol     string    `json:"symbol"`
	Verdict    string    `json:"verdict"`   // ReconcileFlat | ReconcileOpen | ReconcileUnbalanced
	PerpSize   float64   `json:"perp_size"` // signed; short negative
	SpotSize   float64   `json:"spot_size"`
	Collateral float64   `json:"collateral"`
	OpenOrders int       `json:"open_orders"`
	Time       time.Time `json:"time"`
}

// FundingReceived is a fact emitted by order-service when the exchange credits a
// funding settlement, published on SubjFundingReceived. PaymentID is the
// exchange's settlement id; portfolio dedupes on it so a redelivered payment is
// booked once. Amount is signed quote currency (positive = received).
type FundingReceived struct {
	PaymentID string    `json:"payment_id"`
	Symbol    string    `json:"symbol"`
	Amount    float64   `json:"amount"`
	Time      time.Time `json:"time"`
}
