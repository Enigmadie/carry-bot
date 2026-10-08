package mock

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

type fakeMarket struct {
	mark    float64
	rate    float64
	pending bool // settlement not published yet
	calls   int
}

func (f *fakeMarket) Mark(context.Context, string, string) (float64, error) {
	f.calls++
	return f.mark, nil
}

func (f *fakeMarket) SettledFunding(context.Context, string, time.Time) (float64, bool, error) {
	return f.rate, !f.pending, nil
}

// A paper fill must print at the live mark moved against us — a buy pays up, a
// sell sells down — so the shadow run books the spread it would really cross
// rather than a frictionless round trip.
func TestPaperFillsMoveAdverse(t *testing.T) {
	m := New("", 0)
	m.UsePrices(&fakeMarket{mark: 100}, 0.001, 0.01)

	buy, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategorySpot, Symbol: "HYPEUSDC",
		Side: exchange.SideBuy, Qty: "2", OrderLinkID: "buy-1",
	})
	if err != nil {
		t.Fatalf("buy: %v", err)
	}
	if buy.Price != 101 {
		t.Errorf("buy filled at %v, want 101 (mark + slippage)", buy.Price)
	}
	if want := 2 * 101 * 0.001; buy.Fee != want {
		t.Errorf("buy fee = %v, want %v", buy.Fee, want)
	}

	sell, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategoryLinear, Symbol: "HYPEUSDC",
		Side: exchange.SideSell, Qty: "2", OrderLinkID: "sell-1",
	})
	if err != nil {
		t.Fatalf("sell: %v", err)
	}
	if sell.Price != 99 {
		t.Errorf("sell filled at %v, want 99 (mark − slippage)", sell.Price)
	}
}

// Without a Market the mock must stay on its offline constants: the tests and the
// keyless local run depend on it never touching the network.
func TestWithoutMarketStaysSynthetic(t *testing.T) {
	m := New("", 0)
	m.UsePrices(nil, 0.5, 0.5) // nil source must not arm paper mode

	res, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategorySpot, Symbol: "BTCUSDT",
		Side: exchange.SideBuy, Qty: "1", OrderLinkID: "x",
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if res.Price != mockPrice {
		t.Errorf("price = %v, want the synthetic %v", res.Price, mockPrice)
	}
}

// Funding settles hourly but the poll runs every 30s, so paper mode must book one
// payment per settlement hour and nothing in between — otherwise the ledger fills
// with duplicates and P&L runs away.
func TestPaperFundingBooksOncePerHour(t *testing.T) {
	m := New("", 0)
	m.UsePrices(&fakeMarket{mark: 50, rate: 0.0001}, 0, 0)

	// Flat: a short we don't hold accrues nothing.
	if got, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{}); err != nil || len(got) != 0 {
		t.Fatalf("flat funding = %v, %v; want none", got, err)
	}

	if _, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategoryLinear, Symbol: "HYPEUSDC",
		Side: exchange.SideSell, Qty: "3", OrderLinkID: "short-1",
	}); err != nil {
		t.Fatalf("short: %v", err)
	}

	first, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{})
	if err != nil {
		t.Fatalf("funding: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("got %d payments, want 1", len(first))
	}
	if want := 3 * 50 * 0.0001; math.Abs(first[0].Amount-want) > 1e-9 {
		t.Errorf("amount = %v, want %v (size × mark × rate, credited to the short)", first[0].Amount, want)
	}
	if !first[0].Time.Equal(first[0].Time.Truncate(time.Hour)) {
		t.Errorf("payment %s is not dated to a settlement boundary", first[0].Time)
	}

	again, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{})
	if err != nil {
		t.Fatalf("second funding: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("same hour booked twice: %+v", again)
	}
}

// A negative rate reverses the flow: the short pays instead of being paid, which
// is the condition the new exit rule is meant to detect.
func TestPaperFundingNegativeRateDebitsShort(t *testing.T) {
	m := New("", 0)
	m.UsePrices(&fakeMarket{mark: 50, rate: -0.0002}, 0, 0)

	if _, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategoryLinear, Symbol: "HYPEUSDC",
		Side: exchange.SideSell, Qty: "1", OrderLinkID: "short-1",
	}); err != nil {
		t.Fatalf("short: %v", err)
	}
	got, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{})
	if err != nil {
		t.Fatalf("funding: %v", err)
	}
	if len(got) != 1 || got[0].Amount >= 0 {
		t.Fatalf("got %+v, want a single negative accrual", got)
	}
}

// Right after the boundary the settlement may not be published yet; that hour
// must stay unbooked rather than be marked paid, or the poll would skip it.
func TestPaperFundingWaitsForSettlement(t *testing.T) {
	mkt := &fakeMarket{mark: 50, rate: 0.0001, pending: true}
	m := New("", 0)
	m.UsePrices(mkt, 0, 0)

	if _, err := m.PlaceOrder(context.Background(), exchange.OrderRequest{
		Category: exchange.CategoryLinear, Symbol: "HYPEUSDC",
		Side: exchange.SideSell, Qty: "1", OrderLinkID: "short-1",
	}); err != nil {
		t.Fatalf("short: %v", err)
	}
	if got, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{}); err != nil || len(got) != 0 {
		t.Fatalf("unpublished settlement booked: %v, %v", got, err)
	}

	mkt.pending = false
	got, err := m.Funding(context.Background(), "HYPEUSDC", time.Time{})
	if err != nil || len(got) != 1 {
		t.Fatalf("published settlement = %v, %v; want one payment", got, err)
	}
}
