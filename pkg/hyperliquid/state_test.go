package hyperliquid

import (
	"encoding/json"
	"testing"
)

// Real-shaped clearinghouseState: numbers are strings, a short position carries a
// negative szi, and other coins' positions must be ignored.
const sampleClearinghouseJSON = `{
  "assetPositions": [
    {"position": {"coin": "ETH", "szi": "0.25", "entryPx": "3300.0"}},
    {"position": {"coin": "HYPE", "szi": "-1.0", "entryPx": "33.865"}}
  ],
  "marginSummary": {"accountValue": "0.0"}
}`

// Real-shaped spotClearinghouseState: one row per held token; the base leg and
// the USDC collateral come out of the same list.
const sampleSpotClearinghouseJSON = `{
  "balances": [
    {"coin": "USDC", "token": 0, "total": "930.12", "hold": "0.0"},
    {"coin": "HYPE", "token": 150, "total": "0.998", "hold": "0.0"}
  ]
}`

func TestBuildState(t *testing.T) {
	var perp clearinghouseResp
	if err := json.Unmarshal([]byte(sampleClearinghouseJSON), &perp); err != nil {
		t.Fatalf("decode clearinghouseState: %v", err)
	}
	var spot spotClearinghouseResp
	if err := json.Unmarshal([]byte(sampleSpotClearinghouseJSON), &spot); err != nil {
		t.Fatalf("decode spotClearinghouseState: %v", err)
	}

	st, err := buildState("HYPEUSDC", perp, spot)
	if err != nil {
		t.Fatalf("buildState: %v", err)
	}
	if st.PerpSize != -1.0 {
		t.Errorf("PerpSize = %v, want -1.0 (short, ETH position ignored)", st.PerpSize)
	}
	if st.SpotSize != 0.998 {
		t.Errorf("SpotSize = %v, want 0.998", st.SpotSize)
	}
	if st.Collateral != 930.12 {
		t.Errorf("Collateral = %v, want 930.12", st.Collateral)
	}
}

// A flat account holds no perp position row and possibly no base-token balance:
// everything must come back zero, not error.
func TestBuildStateFlat(t *testing.T) {
	var spot spotClearinghouseResp
	if err := json.Unmarshal([]byte(`{"balances":[{"coin":"USDC","total":"999.0"}]}`), &spot); err != nil {
		t.Fatalf("decode: %v", err)
	}

	st, err := buildState("HYPEUSDC", clearinghouseResp{}, spot)
	if err != nil {
		t.Fatalf("buildState: %v", err)
	}
	if st.PerpSize != 0 || st.SpotSize != 0 {
		t.Errorf("flat account: PerpSize=%v SpotSize=%v, want 0/0", st.PerpSize, st.SpotSize)
	}
	if st.Collateral != 999.0 {
		t.Errorf("Collateral = %v, want 999.0", st.Collateral)
	}
}

// The spot leg must resolve through the Unit alias: neutral BTC holds UBTC.
func TestBuildStateSpotAlias(t *testing.T) {
	var spot spotClearinghouseResp
	if err := json.Unmarshal([]byte(`{"balances":[{"coin":"UBTC","total":"0.001"}]}`), &spot); err != nil {
		t.Fatalf("decode: %v", err)
	}

	st, err := buildState("BTCUSDT", clearinghouseResp{}, spot)
	if err != nil {
		t.Fatalf("buildState: %v", err)
	}
	if st.SpotSize != 0.001 {
		t.Errorf("SpotSize = %v, want 0.001 via BTC→UBTC alias", st.SpotSize)
	}
}
