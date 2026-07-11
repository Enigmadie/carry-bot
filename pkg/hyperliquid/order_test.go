package hyperliquid

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

func TestFormatSize(t *testing.T) {
	cases := []struct {
		sz         float64
		szDecimals int
		want       string
	}{
		{0.0012349, 5, "0.00123"}, // truncates to szDecimals places
		{1.5, 5, "1.5"},           // trailing zeros trimmed
		{2.0, 3, "2"},             // integer collapses, no dangling dot
		{0.001, 3, "0.001"},
		{1.0079, 2, "1"}, // balance-derived size truncates down, never up
		{0.0019, 3, "0.001"},
		{4.6, 1, "4.6"}, // float artifact (4.5999…96) must not truncate to 4.5
		{4.6, 0, "4"},
	}
	for _, tc := range cases {
		if got := formatSize(tc.sz, tc.szDecimals); got != tc.want {
			t.Errorf("formatSize(%v, %d) = %q, want %q", tc.sz, tc.szDecimals, got, tc.want)
		}
	}
}

func TestFormatPrice(t *testing.T) {
	cases := []struct {
		px         float64
		szDecimals int
		spot       bool
		want       string
	}{
		{65432.1, 5, false, "65432"},    // 5 sig figs; perp decimals cap = 6-5 = 1
		{1234.567, 4, false, "1234.6"},  // 5 sig figs → 1234.6, decimals cap = 2
		{0.0012345, 2, false, "0.0012"}, // decimals cap = 6-2 = 4
		{65000.0, 5, false, "65000"},    // integer price always allowed
	}
	for _, tc := range cases {
		if got := formatPrice(tc.px, tc.szDecimals, tc.spot); got != tc.want {
			t.Errorf("formatPrice(%v, %d, spot=%v) = %q, want %q", tc.px, tc.szDecimals, tc.spot, got, tc.want)
		}
	}
}

func TestCloidFromLinkID(t *testing.T) {
	a := cloidFromLinkID("intent-open-BTCUSDT-42")
	b := cloidFromLinkID("intent-open-BTCUSDT-42")
	if a != b {
		t.Fatalf("cloid not deterministic: %q vs %q", a, b)
	}
	if len(a) != 34 { // "0x" + 32 hex = 128 bits
		t.Errorf("cloid %q length = %d, want 34", a, len(a))
	}
	if c := cloidFromLinkID("intent-open-BTCUSDT-43"); c == a {
		t.Error("different link ids produced the same cloid")
	}
}

func TestSlippagePrice(t *testing.T) {
	mid := 100.0
	if buy := slippagePrice(mid, 0.05, true); buy <= mid {
		t.Errorf("buy slippage price %v should exceed mid %v", buy, mid)
	}
	if sell := slippagePrice(mid, 0.05, false); sell >= mid {
		t.Errorf("sell slippage price %v should be below mid %v", sell, mid)
	}
	// A wider slippage pushes the IOC price further past the mid (thin-book case).
	if wide, narrow := slippagePrice(mid, 0.5, true), slippagePrice(mid, 0.05, true); wide <= narrow {
		t.Errorf("wider slippage buy %v should exceed narrower %v", wide, narrow)
	}
}

func TestParseMarkPrice(t *testing.T) {
	// The [meta, ctxs] pair: ctx index aligns with perp universe index (the asset id).
	raw := []json.RawMessage{
		json.RawMessage(`{"universe":[{"name":"BTC"},{"name":"ETH"}]}`),
		json.RawMessage(`[{"markPx":"65000.0","oraclePx":"64950.0"},{"markPx":"3400.0","oraclePx":"3399.0"}]`),
	}
	// markPx wins for the asset at the requested index.
	if px, err := parseMarkPrice(raw, 1); err != nil || px != 3400.0 {
		t.Fatalf("parseMarkPrice(idx 1) = %v, %v; want 3400, nil", px, err)
	}
	// Empty markPx falls back to oraclePx.
	fallback := []json.RawMessage{
		raw[0],
		json.RawMessage(`[{"markPx":"","oraclePx":"64950.0"}]`),
	}
	if px, err := parseMarkPrice(fallback, 0); err != nil || px != 64950.0 {
		t.Fatalf("parseMarkPrice fallback = %v, %v; want 64950, nil", px, err)
	}
	// Out-of-range index is an error, not a panic.
	if _, err := parseMarkPrice(raw, 5); err == nil {
		t.Fatal("expected error for out-of-range index")
	}
	// A malformed top-level shape is an error.
	if _, err := parseMarkPrice(raw[:1], 0); err == nil {
		t.Fatal("expected error for missing ctxs element")
	}
}

func TestParseOrderRespFilled(t *testing.T) {
	resp := exchangeResp{
		Status:   "ok",
		Response: []byte(`{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.001","avgPx":"65010.0","oid":777}}]}}`),
	}
	res, err := parseOrderResp("link-1", resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.OrderID != "777" || res.OrderLinkID != "link-1" {
		t.Errorf("ids = {%q,%q}, want {777,link-1}", res.OrderID, res.OrderLinkID)
	}
	if res.Price != 65010.0 || res.FilledQty != 0.001 {
		t.Errorf("fill = {px:%v qty:%v}, want {65010 0.001}", res.Price, res.FilledQty)
	}
}

func TestParseOrderRespPerOrderError(t *testing.T) {
	resp := exchangeResp{
		Status:   "ok",
		Response: []byte(`{"type":"order","data":{"statuses":[{"error":"Insufficient margin to place order"}]}}`),
	}
	_, err := parseOrderResp("link-2", resp)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want *APIError, got %v", err)
	}
}

func TestParseOrderRespTopLevelErr(t *testing.T) {
	resp := exchangeResp{Status: "err", Response: []byte(`"Must deposit before trading"`)}
	_, err := parseOrderResp("link-3", resp)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want *APIError, got %v", err)
	}
	if apiErr.Msg != "Must deposit before trading" {
		t.Errorf("msg = %q, want unquoted error string", apiErr.Msg)
	}
}

func TestClassify(t *testing.T) {
	c := &Client{}
	cases := []struct {
		err  error
		want exchange.ErrorKind
	}{
		{errors.New("dial tcp: timeout"), exchange.ErrTransient},
		{&APIError{Msg: "Insufficient margin to place order"}, exchange.ErrTerminal},
		{&APIError{Msg: "Order has invalid reduce only flag"}, exchange.ErrTerminal},
		{&APIError{Msg: "Client order id already used"}, exchange.ErrDuplicate},
		// An IOC that crossed nothing is terminal: retrying the same priced IOC fails again.
		{&APIError{Msg: "Order could not immediately match against any resting orders"}, exchange.ErrTerminal},
	}
	for _, tc := range cases {
		if got := c.Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
