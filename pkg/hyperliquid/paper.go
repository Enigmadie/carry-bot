package hyperliquid

// Reference market data for paper trading: the live mark a simulated fill should
// print at, and the funding rate a simulated position should accrue. Both are
// unsigned /info reads, so an info-only client (no key) serves them — the shadow
// run needs real prices, not real orders.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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

// FundingRate returns the perp's current hourly funding rate as a signed fraction
// (positive = longs pay shorts, a credit to our short). This is the rate the next
// settlement will pay, not a realized payment — Funding reads those.
func (c *Client) FundingRate(ctx context.Context, symbol string) (float64, error) {
	ref, err := c.resolveAsset(exchange.CategoryLinear, symbol)
	if err != nil {
		return 0, err
	}
	var raw []json.RawMessage
	if err := c.info(ctx, map[string]string{"type": "metaAndAssetCtxs"}, &raw); err != nil {
		return 0, fmt.Errorf("meta and asset ctxs: %w", err)
	}
	return parseFundingRate(raw, ref.Asset)
}

// parseFundingRate pulls the funding rate for perp index idx out of a decoded
// metaAndAssetCtxs reply. Split from the network call for the same reason as
// parseMarkPrice: the shape is the awkward part, and it is worth a unit test.
func parseFundingRate(raw []json.RawMessage, idx int) (float64, error) {
	if len(raw) < 2 {
		return 0, fmt.Errorf("metaAndAssetCtxs: expected [meta, ctxs], got %d elements", len(raw))
	}
	var ctxs []struct {
		Funding string `json:"funding"`
	}
	if err := json.Unmarshal(raw[1], &ctxs); err != nil {
		return 0, fmt.Errorf("decode asset ctxs: %w", err)
	}
	if idx < 0 || idx >= len(ctxs) {
		return 0, fmt.Errorf("no asset ctx for perp index %d (have %d)", idx, len(ctxs))
	}
	if ctxs[idx].Funding == "" {
		return 0, fmt.Errorf("no funding rate for perp index %d", idx)
	}
	v, err := strconv.ParseFloat(ctxs[idx].Funding, 64)
	if err != nil {
		return 0, fmt.Errorf("parse funding rate %q: %w", ctxs[idx].Funding, err)
	}
	return v, nil
}
