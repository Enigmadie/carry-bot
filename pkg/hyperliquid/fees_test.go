package hyperliquid

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Fill rows as live mainnet returned them for position 49's open (2026-11-06),
// plus a second fill on the perp oid (an IOC that swept two levels) and a fill
// from an order we didn't ask about.
const userFillsJSON = `[
 {"coin":"@107","px":"79.036","sz":"0.3","side":"B","oid":597184346840,"fee":"0.00021","feeToken":"HYPE","dir":"Buy"},
 {"coin":"HYPE","px":"79.039","sz":"0.2","side":"A","oid":597184378217,"fee":"0.00711","feeToken":"USDC","dir":"Open Short"},
 {"coin":"HYPE","px":"79.03","sz":"0.1","side":"A","oid":597184378217,"fee":"0.00356","feeToken":"USDC","dir":"Open Short"},
 {"coin":"HYPE","px":"80.1","sz":"1","side":"B","oid":111,"fee":"0.036","feeToken":"USDC","dir":"Close Short"}
]`

func TestSumFees(t *testing.T) {
	var fills []userFill
	if err := json.Unmarshal([]byte(userFillsJSON), &fills); err != nil {
		t.Fatal(err)
	}
	got, err := sumFees(fills, map[uint64]string{
		597184346840: "597184346840",
		597184378217: "597184378217",
		42:           "42",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"597184346840": 0.00021 * 79.036, // base-token fee → quote at fill px
		"597184378217": 0.00711 + 0.00356,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v (unfilled oid must be absent)", got, want)
	}
	for id, w := range want {
		if math.Abs(got[id]-w) > 1e-12 {
			t.Errorf("fee[%s] = %v, want %v", id, got[id], w)
		}
	}
}

func TestOrderFeesQueriesMasterAccount(t *testing.T) {
	const master = "0x6EBadE99E3840Ecc021580A5b5BFbE620c253F6b"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct{ Type, User string }
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Type != "userFills" || req.User != master {
			t.Errorf("unexpected request %+v", req)
		}
		io.WriteString(w, userFillsJSON)
	}))
	defer srv.Close()

	c, err := New(Config{BaseURL: srv.URL, Mainnet: true, Account: master})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.OrderFees(context.Background(), []string{"597184378217"})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got["597184378217"]-0.01067) > 1e-12 {
		t.Fatalf("perp fee = %v, want 0.01067", got["597184378217"])
	}
}
