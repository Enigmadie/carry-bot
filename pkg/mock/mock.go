// Package mock is a deterministic in-memory Exchange for building out portfolio,
// reconciliation, and observability without any network, keys, or geo-gate. It
// accepts legs and returns synthetic fills; a repeated OrderLinkID surfaces as a
// duplicate (so the idempotency path is exercised), and FailCategory injects a
// terminal failure to drive leg-risk rollback paths.
//
// UsePrices switches it into paper mode: fills still never reach an exchange, but
// they print at the live mark and accrue the exchange's real funding rate, so a
// shadow run books honest economics without risking capital. The default stays
// offline and deterministic, which is what the tests rely on.
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

// Market is the live reference data a paper run fills against. It is the read-only
// half of an exchange client, so an unauthenticated one satisfies it.
type Market interface {
	Mark(ctx context.Context, category, symbol string) (float64, error)
	SettledFunding(ctx context.Context, symbol string, hour time.Time) (rate float64, ok bool, err error)
}

type Exchange struct {
	failCategory string        // CategorySpot | CategoryLinear | "" — leg to fail, for rollback testing
	adlAfter     time.Duration // >0: force-close the perp this long after it opens (ADL simulation)

	market   Market  // nil = synthetic constants; set = paper mode on live marks
	feeRate  float64 // taker fee charged on a paper fill
	slippage float64 // adverse offset from the mark a paper fill prints at

	mu       sync.Mutex
	seen     map[string]string // orderLinkId -> orderId
	fundingN int               // monotonic settlement counter, for deterministic ids
	lastPaid time.Time         // paper mode: settlement hour already booked, so a 30s poll cannot re-book it

	// Live position, mutated by fills, so State reflects what was traded — the
	// same contract a real exchange gives reconciliation. In-memory like the
	// rest of the mock: a restart forgets it and reconciles as flat.
	perpSize   float64 // signed; our short goes negative
	spotSize   float64
	perpOpenAt time.Time // when the short opened; adlAfter counts from here
	adlBitten  bool      // first (partial) ADL bite applied; keeps the bite idempotent
}

// adlPartialFraction is what remains of the perp after the first ADL bite — a
// shape that is neither an open leg nor dust, so reconciliation must judge it
// unbalanced (the live incidents left −0.29 of 1).
const adlPartialFraction = 0.3

// New builds a mock. failCategory ("spot"/"linear"/"") forces that leg to fail
// terminally so callers can exercise rollback/unbalanced handling. adlAfter > 0
// simulates the exchange force-closing the perp (Auto-Deleveraging) in two
// bites, the way the live incidents played out: adlAfter after the short opens
// a partial bite leaves an unbalanced stub (→ halt), and 2×adlAfter finishes
// the perp off, leaving the orphaned spot the halted tick must catch and
// repair.
func New(failCategory string, adlAfter time.Duration) *Exchange {
	return &Exchange{failCategory: failCategory, adlAfter: adlAfter, seen: map[string]string{}}
}

// UsePrices turns on paper mode: fills print at mkt's live mark, moved adverse by
// slippage (a buy pays up, a sell sells down) and charged fee on the notional, and
// funding accrues at the exchange's published rate against the open perp. Passing a
// nil Market is a no-op, so a misconfigured shadow run stays on the constants rather
// than silently pricing everything at zero.
func (m *Exchange) UsePrices(mkt Market, fee, slippage float64) {
	if mkt == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.market = mkt
	m.feeRate = fee
	m.slippage = slippage
}

func (m *Exchange) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	if req.Category == m.failCategory {
		return nil, errInjected
	}

	// Priced before the lock: paper mode reaches the network here, and holding the
	// mutex across an HTTP call would serialize both legs behind it.
	price, feeRate, err := m.fillPrice(ctx, req)
	if err != nil {
		return nil, err
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
			m.adlBitten = false
		}
	} else {
		m.spotSize += delta
	}

	return &exchange.OrderResult{
		OrderID:     orderID,
		OrderLinkID: req.OrderLinkID,
		Price:       price,
		Fee:         qty * price * feeRate,
		FilledQty:   qty,
	}, nil
}

// fillPrice resolves what this leg fills at and what fee rate it pays. Offline it
// is the flat constant both legs share; in paper mode it is the live mark pushed
// against us by slippage, which is the only execution cost a shadow run can model
// — real partial fills and book impact need a real order.
func (m *Exchange) fillPrice(ctx context.Context, req exchange.OrderRequest) (price, feeRate float64, err error) {
	m.mu.Lock()
	mkt, fee, slip := m.market, m.feeRate, m.slippage
	m.mu.Unlock()

	if mkt == nil {
		return mockPrice, mockFeeRate, nil
	}
	mark, err := mkt.Mark(ctx, req.Category, req.Symbol)
	if err != nil {
		return 0, 0, fmt.Errorf("mock: mark for %s %s: %w", req.Category, req.Symbol, err)
	}
	if req.Side == exchange.SideSell {
		return mark * (1 - slip), fee, nil
	}
	return mark * (1 + slip), fee, nil
}

// Funding returns one synthetic settlement per call, timestamped now (so it is
// always after `since`) with a monotonic id. The amount is a fixed positive
// credit (perp short receives positive funding), so portfolio's funding_total —
// and thus P&L — climbs across polls.
func (m *Exchange) Funding(ctx context.Context, symbol string, since time.Time) ([]exchange.FundingPayment, error) {
	m.mu.Lock()
	paper := m.market != nil
	m.mu.Unlock()
	if paper {
		return m.paperFunding(ctx, symbol, since)
	}

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
	if m.adlAfter > 0 && m.perpSize < 0 {
		switch elapsed := time.Since(m.perpOpenAt); {
		case elapsed >= 2*m.adlAfter:
			m.perpSize = 0
		case elapsed >= m.adlAfter && !m.adlBitten:
			m.perpSize *= adlPartialFraction
			m.adlBitten = true
		}
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

// paperFunding books at most one settlement per elapsed hour, and only while the
// perp short is actually open. Hyperliquid settles hourly on the hour, so the
// payment is dated to the last boundary and the boundary itself is the dedup key —
// without that the 30s funding poll would book the same hour 120 times over. The
// amount is the rate that actually settled at that boundary against the position's
// notional at the current mark; an hour whose settlement isn't published yet is
// left unbooked and retried on the next poll.
func (m *Exchange) paperFunding(ctx context.Context, symbol string, since time.Time) ([]exchange.FundingPayment, error) {
	m.mu.Lock()
	mkt, perp, paid := m.market, m.perpSize, m.lastPaid
	m.mu.Unlock()

	if perp >= 0 {
		return nil, nil // flat or long: nothing accrues to a short we don't hold
	}
	settled := time.Now().UTC().Truncate(time.Hour)
	if !settled.After(paid) || !settled.After(since) {
		return nil, nil
	}

	rate, ok, err := mkt.SettledFunding(ctx, symbol, settled)
	if err != nil {
		return nil, fmt.Errorf("mock: settled funding for %s: %w", symbol, err)
	}
	if !ok {
		return nil, nil
	}
	mark, err := mkt.Mark(ctx, exchange.CategoryLinear, symbol)
	if err != nil {
		return nil, fmt.Errorf("mock: mark for %s: %w", symbol, err)
	}

	m.mu.Lock()
	m.lastPaid = settled
	m.mu.Unlock()

	// Short pays when the rate is negative and receives when positive, hence the
	// sign flip against the (negative) perp size.
	return []exchange.FundingPayment{{
		ID:     fmt.Sprintf("paper-funding-%s-%d", symbol, settled.Unix()),
		Symbol: symbol,
		Amount: -perp * mark * rate,
		Time:   settled,
	}}, nil
}
