package hyperliquid

import (
	"context"
	"fmt"
	"strings"
)

// metaResp is the /info "meta" reply: the perp universe. An asset's index — what
// an order's `a` field carries — is its position in this array.
type metaResp struct {
	Universe []struct {
		Name       string `json:"name"`       // coin, e.g. "BTC"
		SzDecimals int    `json:"szDecimals"` // decimals the order size must round to
	} `json:"universe"`
}

// spotAssetOffset separates spot from perp asset ids: perps and spot share the
// order `a` field, so a spot asset's id is this offset plus its universe index.
const spotAssetOffset = 10000

// spotMetaResp is the /info "spotMeta" reply. A spot asset's order index is
// 10000 + its universe index (perps and spot share the `a` field). The pair's
// size precision comes from its base token, listed separately in Tokens.
type spotMetaResp struct {
	Universe []struct {
		Name   string `json:"name"`   // pair, e.g. "PURR/USDC"
		Index  int    `json:"index"`  // spot index; order asset = 10000 + Index
		Tokens []int  `json:"tokens"` // [baseTokenIndex, quoteTokenIndex]
	} `json:"universe"`
	Tokens []struct {
		Index      int    `json:"index"`
		Name       string `json:"name"`
		SzDecimals int    `json:"szDecimals"`
	} `json:"tokens"`
}

// LoadMeta fetches the perp and spot metadata and rebuilds the symbol→asset map.
// Called once at startup; safe to call again to refresh (metadata changes when
// the exchange lists instruments, rarely).
func (c *Client) LoadMeta(ctx context.Context) error {
	var perp metaResp
	if err := c.info(ctx, map[string]string{"type": "meta"}, &perp); err != nil {
		return fmt.Errorf("load meta: %w", err)
	}
	var spot spotMetaResp
	if err := c.info(ctx, map[string]string{"type": "spotMeta"}, &spot); err != nil {
		return fmt.Errorf("load spotMeta: %w", err)
	}

	assets := buildPerpAssets(perp)
	for k, v := range buildSpotAssets(spot) {
		assets[k] = v
	}

	c.mu.Lock()
	c.assets = assets
	c.mu.Unlock()
	return nil
}

// buildPerpAssets keys each perp by "linear:<coin>"; the asset id is the array
// position, matching how Hyperliquid numbers perps.
func buildPerpAssets(m metaResp) map[string]assetRef {
	out := make(map[string]assetRef, len(m.Universe))
	for i, u := range m.Universe {
		out["linear:"+u.Name] = assetRef{Asset: i, SzDecimals: u.SzDecimals}
	}
	return out
}

// buildSpotAssets keys each pair by "spot:<base>" (the part before the slash),
// with asset id 10000+index and the base token's size precision. Keying by base
// coin is best-effort: it assumes one listed pair per base on the quote we trade,
// which holds for the delta-neutral legs we place.
func buildSpotAssets(m spotMetaResp) map[string]assetRef {
	tokenSz := make(map[int]int, len(m.Tokens))
	for _, t := range m.Tokens {
		tokenSz[t.Index] = t.SzDecimals
	}

	out := make(map[string]assetRef, len(m.Universe))
	for _, u := range m.Universe {
		base := u.Name
		if i := strings.IndexByte(base, '/'); i >= 0 {
			base = base[:i]
		}
		sz := 0
		if len(u.Tokens) > 0 {
			sz = tokenSz[u.Tokens[0]]
		}
		out["spot:"+base] = assetRef{Asset: spotAssetOffset + u.Index, SzDecimals: sz}
	}
	return out
}

// resolveAsset maps an exchange-neutral (category, symbol) onto Hyperliquid's
// asset id and size precision. The neutral symbol carries a quote suffix
// ("BTCUSDT"); Hyperliquid indexes by coin, so the quote is stripped first.
func (c *Client) resolveAsset(category, symbol string) (assetRef, error) {
	key := category + ":" + stripQuote(symbol)

	c.mu.RLock()
	ref, ok := c.assets[key]
	c.mu.RUnlock()
	if !ok {
		return assetRef{}, fmt.Errorf("unknown instrument %q (resolved key %q); not listed or LoadMeta not called", symbol, key)
	}
	return ref, nil
}

// stripQuote drops a trailing quote-currency suffix so "BTCUSDT" → "BTC". Longest
// suffix first so "USDT" isn't shadowed by "USD".
func stripQuote(symbol string) string {
	for _, q := range []string{"USDT", "USDC", "USD"} {
		if len(symbol) > len(q) && strings.HasSuffix(symbol, q) {
			return strings.TrimSuffix(symbol, q)
		}
	}
	return symbol
}
