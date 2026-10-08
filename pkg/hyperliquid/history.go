package hyperliquid

// FundingHistory is the public, per-coin funding schedule from /info
// "fundingHistory". It is the counterpart to Funding (userFunding): that one is
// per-account and only knows settlements we were actually paid, so it can never
// answer "what would we have earned had we held" — the question a backtest asks.
// This one is unsigned and needs no account, so it works with an info-only client.

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// FundingTick is one hourly settlement as the exchange published it. Rate is a
// fraction per settlement (0.0000125 = 0.00125% per hour), signed: positive means
// longs pay shorts, i.e. a credit to the bot's perp short.
type FundingTick struct {
	Time    time.Time
	Rate    float64
	Premium float64
}

// fundingHistoryPage is the API's cap on rows per reply. A full page means the
// window was truncated, so the caller must resume after the last row.
const fundingHistoryPage = 500

type fundingHistoryEntry struct {
	Coin        string `json:"coin"`
	FundingRate string `json:"fundingRate"`
	Premium     string `json:"premium"`
	Time        int64  `json:"time"`
}

// FundingHistory returns every settlement for symbol's coin in [start, end),
// walking the 500-row pages the API caps replies at. Rows come back ascending;
// each page resumes one millisecond past the last row so the boundary settlement
// is not counted twice.
func (c *Client) FundingHistory(ctx context.Context, symbol string, start, end time.Time) ([]FundingTick, error) {
	coin := stripQuote(symbol)
	var out []FundingTick
	cursor := start.UnixMilli()
	endMS := end.UnixMilli()

	for cursor < endMS {
		req := map[string]any{
			"type":      "fundingHistory",
			"coin":      coin,
			"startTime": cursor,
			"endTime":   endMS,
		}
		var page []fundingHistoryEntry
		if err := c.info(ctx, req, &page); err != nil {
			return nil, fmt.Errorf("funding history %s: %w", coin, err)
		}
		if len(page) == 0 {
			break
		}

		for _, e := range page {
			rate, err := strconv.ParseFloat(e.FundingRate, 64)
			if err != nil {
				return nil, fmt.Errorf("parse funding rate %q: %w", e.FundingRate, err)
			}
			premium, _ := strconv.ParseFloat(e.Premium, 64) // informational; absent on some rows
			out = append(out, FundingTick{
				Time:    time.UnixMilli(e.Time).UTC(),
				Rate:    rate,
				Premium: premium,
			})
		}

		if len(page) < fundingHistoryPage {
			break // short page = window exhausted
		}
		next := page[len(page)-1].Time + 1
		if next <= cursor {
			break // no forward progress; stop rather than spin
		}
		cursor = next
	}
	return out, nil
}

// settledLookback is how far back LastSettledFunding searches; a few hours
// covers an API hiccup without paging through history.
const settledLookback = 3 * time.Hour

// LastSettledFunding returns the most recent settlement up to now; ok is false
// when none landed within settledLookback.
func (c *Client) LastSettledFunding(ctx context.Context, symbol string, now time.Time) (tick FundingTick, ok bool, err error) {
	ticks, err := c.FundingHistory(ctx, symbol, now.Add(-settledLookback), now)
	if err != nil || len(ticks) == 0 {
		return FundingTick{}, false, err
	}
	return ticks[len(ticks)-1], true, nil
}
