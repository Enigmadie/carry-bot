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

// buildSpotAssets keys each USDC-quoted pair by "spot:<baseToken>", with asset id
// 10000+index and the base token's size precision. The base comes from the pair's
// first token, not its name: on live Hyperliquid almost every spot pair is named
// "@<index>" (only PURR/USDC carries a human name), so parsing the name would
// yield "@107" instead of the coin. A base can list against several quotes
// (USDC, USDH, …); we keep only the USDC pair, which is the one a delta-neutral
// spot leg trades against the perp's USDC margin.
func buildSpotAssets(m spotMetaResp) map[string]assetRef {
	type token struct {
		name string
		sz   int
	}
	tokens := make(map[int]token, len(m.Tokens))
	usdc := -1
	for _, t := range m.Tokens {
		tokens[t.Index] = token{name: t.Name, sz: t.SzDecimals}
		if t.Name == "USDC" {
			usdc = t.Index
		}
	}

	out := make(map[string]assetRef, len(m.Universe))
	for _, u := range m.Universe {
		if len(u.Tokens) < 2 || u.Tokens[1] != usdc {
			continue // skip non-USDC-quoted pairs (and malformed entries)
		}
		base, ok := tokens[u.Tokens[0]]
		if !ok {
			continue
		}
		out["spot:"+base.name] = assetRef{Asset: spotAssetOffset + u.Index, SzDecimals: base.sz}
	}
	return out
}

// spotBaseAliases maps a neutral base coin to Hyperliquid's spot base token where
// they differ. HL lists spot BTC/ETH as the Unit-bridged "UBTC"/"UETH", while
// perps and our neutral symbols use the bare coin. Only spot resolution needs the
// alias — the perp leg keeps the plain coin.
var spotBaseAliases = map[string]string{
	"BTC": "UBTC",
	"ETH": "UETH",
}

func spotBaseAlias(coin string) string {
	if a, ok := spotBaseAliases[coin]; ok {
		return a
	}
	return coin
}

// resolveAsset maps an exchange-neutral (category, symbol) onto Hyperliquid's
// asset id and size precision. The neutral symbol carries a quote suffix
// ("BTCUSDT"); Hyperliquid indexes by coin, so the quote is stripped first.
func (c *Client) resolveAsset(category, symbol string) (assetRef, error) {
	base := stripQuote(symbol)
	if category == "spot" {
		base = spotBaseAlias(base) // HL spot wraps BTC/ETH as UBTC/UETH
	}
	key := category + ":" + base

	c.mu.RLock()
	ref, ok := c.assets[key]
	c.mu.RUnlock()
	if !ok {
		return assetRef{}, fmt.Errorf("unknown instrument %q (resolved key %q); not listed or LoadMeta not called", symbol, key)
	}
	return ref, nil
}

// MarketKeys resolves the keys a market-data feed needs for a neutral symbol on
// Hyperliquid's allMids channel: the perp coin (which is also the activeAssetCtx
// subscription coin) and the spot mid key "@<index>". It mirrors resolveAsset but
// returns WS-channel keys rather than order asset ids — allMids keys perps by coin
// name and spot pairs by "@<spotIndex>", where the spot index is the order asset
// id minus the offset. The perp coin is always available (just the stripped
// quote); the spot key needs a listed pair (best-effort, see buildSpotAssets), so
// spotOK is false when none is mapped. Requires LoadMeta first.
func (c *Client) MarketKeys(symbol string) (coin, spotKey string, spotOK bool) {
	coin = stripQuote(symbol)
	c.mu.RLock()
	ref, ok := c.assets["spot:"+spotBaseAlias(coin)] // HL spot wraps BTC/ETH as UBTC/UETH
	c.mu.RUnlock()
	if ok {
		spotKey = fmt.Sprintf("@%d", ref.Asset-spotAssetOffset)
	}
	return coin, spotKey, ok
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
