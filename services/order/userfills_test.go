package main

import "testing"

// Frames follow the live Hyperliquid userFills shape (fills carry a cloid only
// when the order set one — ours always do).
func TestForeignFill(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantDir string
		wantOK  bool
	}{
		{
			"ADL fill has no cloid",
			`{"channel":"userFills","data":{"user":"0xabc","fills":[
				{"coin":"HYPE","px":"49.6","sz":"0.34","side":"B","time":1,"dir":"Auto-Deleveraging","oid":1,"fee":"0.0"}]}}`,
			"Auto-Deleveraging", true,
		},
		{
			"our fill carries a cloid",
			`{"channel":"userFills","data":{"user":"0xabc","fills":[
				{"coin":"HYPE","px":"38.0","sz":"1.0","side":"B","time":1,"dir":"Buy","oid":2,"cloid":"0xdeadbeef"}]}}`,
			"", false,
		},
		{
			"mixed frame: first foreign fill wins",
			`{"channel":"userFills","data":{"user":"0xabc","fills":[
				{"dir":"Buy","cloid":"0xdeadbeef"},{"dir":"Liquidation","oid":3}]}}`,
			"Liquidation", true,
		},
		{
			"snapshot replay is skipped",
			`{"channel":"userFills","data":{"isSnapshot":true,"user":"0xabc","fills":[{"dir":"Auto-Deleveraging"}]}}`,
			"", false,
		},
		{"subscription ack", `{"channel":"subscriptionResponse","data":{}}`, "", false},
		{"pong", `{"channel":"pong"}`, "", false},
		{"garbage", `not json`, "", false},
	}
	for _, tc := range cases {
		dir, ok := foreignFill([]byte(tc.raw))
		if dir != tc.wantDir || ok != tc.wantOK {
			t.Errorf("%s: foreignFill = (%q, %v), want (%q, %v)", tc.name, dir, ok, tc.wantDir, tc.wantOK)
		}
	}
}
