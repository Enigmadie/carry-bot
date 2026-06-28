package main

import (
	"encoding/json"
	"testing"
)

// Real-shaped Hyperliquid WS frames. allMids keys perps by coin and spot pairs by
// "@<index>"; activeAssetCtx nests the funding rate under ctx. Every number is a
// decimal string, so the structs decode them as strings and parse downstream.
const (
	sampleAllMids     = `{"channel":"allMids","data":{"mids":{"BTC":"95012.5","ETH":"3401.2","@3":"94980.0"}}}`
	sampleActiveCtx   = `{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0000125","markPx":"95010.0","midPx":"95012.5"}}}`
	sampleSubResponse = `{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"allMids"}}}`
	samplePong        = `{"channel":"pong"}`
)

func TestDispatchEnvelope(t *testing.T) {
	cases := map[string]string{
		sampleAllMids:     "allMids",
		sampleActiveCtx:   "activeAssetCtx",
		sampleSubResponse: "subscriptionResponse",
		samplePong:        "pong",
	}
	for raw, want := range cases {
		var env wsEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			t.Fatalf("unmarshal %q: %v", raw, err)
		}
		if env.Channel != want {
			t.Errorf("channel = %q, want %q", env.Channel, want)
		}
	}
}

func TestParseMids(t *testing.T) {
	var env wsEnvelope
	if err := json.Unmarshal([]byte(sampleAllMids), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var d midsData
	if err := json.Unmarshal(env.Data, &d); err != nil {
		t.Fatalf("unmarshal mids: %v", err)
	}
	if got := d.Mids["BTC"]; got != "95012.5" { // perp keyed by coin
		t.Errorf("perp mid = %q, want 95012.5", got)
	}
	if got := d.Mids["@3"]; got != "94980.0" { // spot keyed by "@<index>"
		t.Errorf("spot mid = %q, want 94980.0", got)
	}
}

func TestParseAssetCtxFunding(t *testing.T) {
	var env wsEnvelope
	if err := json.Unmarshal([]byte(sampleActiveCtx), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var d assetCtxData
	if err := json.Unmarshal(env.Data, &d); err != nil {
		t.Fatalf("unmarshal assetCtx: %v", err)
	}
	if d.Coin != "BTC" {
		t.Errorf("coin = %q, want BTC", d.Coin)
	}
	if d.Ctx.Funding != "0.0000125" {
		t.Errorf("funding = %q, want 0.0000125", d.Ctx.Funding)
	}
}
