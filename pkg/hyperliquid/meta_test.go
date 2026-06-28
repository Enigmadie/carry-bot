package hyperliquid

import (
	"encoding/json"
	"testing"
)

// Trimmed but real-shaped /info replies: perp universe (asset = array position)
// and spot metadata (asset = 10000 + index, size precision from the base token).
const sampleMetaJSON = `{
  "universe": [
    {"name": "BTC", "szDecimals": 5},
    {"name": "ETH", "szDecimals": 4}
  ]
}`

// Real-shaped: live HL names spot pairs "@<index>" (only PURR/USDC has a human
// name), keys the base off the first token (BTC is the Unit-bridged "UBTC"), and
// can list a base against several quotes — only the USDC pair should map, so @9
// (UBTC/USDH) must be skipped in favour of @3 (UBTC/USDC).
const sampleSpotMetaJSON = `{
  "tokens": [
    {"index": 0, "name": "USDC", "szDecimals": 8},
    {"index": 1, "name": "PURR", "szDecimals": 0},
    {"index": 2, "name": "UBTC", "szDecimals": 5},
    {"index": 5, "name": "USDH", "szDecimals": 8}
  ],
  "universe": [
    {"name": "PURR/USDC", "index": 0, "tokens": [1, 0]},
    {"name": "@3",        "index": 3, "tokens": [2, 0]},
    {"name": "@9",        "index": 9, "tokens": [2, 5]}
  ]
}`

func loadSample(t *testing.T) *Client {
	t.Helper()
	var perp metaResp
	if err := json.Unmarshal([]byte(sampleMetaJSON), &perp); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	var spot spotMetaResp
	if err := json.Unmarshal([]byte(sampleSpotMetaJSON), &spot); err != nil {
		t.Fatalf("decode spotMeta: %v", err)
	}
	assets := buildPerpAssets(perp)
	for k, v := range buildSpotAssets(spot) {
		assets[k] = v
	}
	return &Client{assets: assets}
}

func TestResolveAsset(t *testing.T) {
	c := loadSample(t)

	cases := []struct {
		category, symbol string
		wantAsset        int
		wantSz           int
	}{
		{"linear", "BTCUSDT", 0, 5},    // perp index = position in universe
		{"linear", "ETHUSDT", 1, 4},    // quote suffix stripped: ETHUSDT → ETH
		{"spot", "BTCUSDC", 10003, 5},  // BTC→UBTC alias, USDC pair @3 (not @9), base szDecimals
		{"spot", "PURRUSDC", 10000, 0}, // spot index 0, no alias
	}
	for _, tc := range cases {
		ref, err := c.resolveAsset(tc.category, tc.symbol)
		if err != nil {
			t.Fatalf("resolve %s/%s: %v", tc.category, tc.symbol, err)
		}
		if ref.Asset != tc.wantAsset || ref.SzDecimals != tc.wantSz {
			t.Errorf("resolve %s/%s = {asset:%d sz:%d}, want {asset:%d sz:%d}",
				tc.category, tc.symbol, ref.Asset, ref.SzDecimals, tc.wantAsset, tc.wantSz)
		}
	}
}

func TestResolveAssetUnknown(t *testing.T) {
	c := loadSample(t)
	if _, err := c.resolveAsset("linear", "DOGEUSDT"); err == nil {
		t.Fatal("expected error for unlisted instrument, got nil")
	}
}

func TestMarketKeys(t *testing.T) {
	c := loadSample(t)

	// BTC resolves to the UBTC/USDC spot pair (@3) → allMids spot key "@3"; the
	// perp coin stays the bare "BTC" (no alias on the perp leg).
	coin, spotKey, ok := c.MarketKeys("BTCUSDT")
	if coin != "BTC" || spotKey != "@3" || !ok {
		t.Errorf("MarketKeys(BTCUSDT) = (%q, %q, %v), want (BTC, @3, true)", coin, spotKey, ok)
	}

	// ETH has a perp but no spot pair in the sample → coin resolves, spot does not.
	coin, spotKey, ok = c.MarketKeys("ETHUSDT")
	if coin != "ETH" || spotKey != "" || ok {
		t.Errorf("MarketKeys(ETHUSDT) = (%q, %q, %v), want (ETH, \"\", false)", coin, spotKey, ok)
	}
}

func TestStripQuote(t *testing.T) {
	cases := map[string]string{
		"BTCUSDT": "BTC",
		"BTCUSDC": "BTC",
		"ETHUSD":  "ETH",
		"PURR":    "PURR", // no known quote suffix → unchanged
	}
	for in, want := range cases {
		if got := stripQuote(in); got != want {
			t.Errorf("stripQuote(%q) = %q, want %q", in, got, want)
		}
	}
}
