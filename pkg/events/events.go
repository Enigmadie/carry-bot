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
