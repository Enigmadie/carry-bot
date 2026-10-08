package hyperliquid

// Reference market data for paper trading: the live mark a simulated fill should
// print at, and the funding rate a simulated position should accrue. Both are
// unsigned /info reads, so an info-only client (no key) serves them — the shadow
// run needs real prices, not real orders.

import (
	"context"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

// Mark returns the reference price for one leg: a perp's oracle mark, or a spot
// pair's book mid. Requires LoadMeta first, like any symbol-addressed call.
func (c *Client) Mark(ctx context.Context, category, symbol string) (float64, error) {
	ref, err := c.resolveAsset(category, symbol)
	if err != nil {
		return 0, err
	}
	if category == exchange.CategoryLinear {
		return c.markPrice(ctx, ref)
	}
	return c.midPrice(ctx, ref, stripQuote(symbol))
}

// SettledFunding returns the rate the exchange actually settled at the hour
// boundary hour, as a signed fraction (positive = longs pay shorts, a credit to
// our short). ok is false while that settlement isn't published yet. The
// predicted rate in the asset ctx is no substitute: early in the hour it is
// averaged over seconds of premium and swings far past what ends up settling.
func (c *Client) SettledFunding(ctx context.Context, symbol string, hour time.Time) (rate float64, ok bool, err error) {
	ticks, err := c.FundingHistory(ctx, symbol, hour, hour.Add(time.Hour))
	if err != nil {
		return 0, false, err
	}
	for _, t := range ticks {
		if t.Time.Truncate(time.Hour).Equal(hour) {
			return t.Rate, true, nil
		}
	}
	return 0, false, nil
}
