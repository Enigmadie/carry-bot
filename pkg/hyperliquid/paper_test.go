package hyperliquid

import (
	"encoding/json"
	"testing"
)

func TestParseFundingRate(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"universe":[{"name":"BTC"},{"name":"HYPE"}]}`),
		json.RawMessage(`[{"funding":"0.0000125"},{"funding":"-0.0000342"}]`),
	}
	if rate, err := parseFundingRate(raw, 0); err != nil || rate != 0.0000125 {
		t.Fatalf("parseFundingRate(idx 0) = %v, %v; want 0.0000125, nil", rate, err)
	}
	// A negative rate must survive the parse intact — it is the signal the exit
	// rule keys on, so silently reading it as zero would strand a losing position.
	if rate, err := parseFundingRate(raw, 1); err != nil || rate != -0.0000342 {
		t.Fatalf("parseFundingRate(idx 1) = %v, %v; want -0.0000342, nil", rate, err)
	}
	if _, err := parseFundingRate(raw, 9); err == nil {
		t.Fatal("expected error for out-of-range index")
	}
	if _, err := parseFundingRate(raw[:1], 0); err == nil {
		t.Fatal("expected error for missing ctxs element")
	}
	missing := []json.RawMessage{raw[0], json.RawMessage(`[{"funding":""}]`)}
	if _, err := parseFundingRate(missing, 0); err == nil {
		t.Fatal("expected error for absent funding rate")
	}
}
