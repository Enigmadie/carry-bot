package hyperliquid

import "testing"

// Pinned on the live testnet form (2026-07-11): userFunding settlements carry the
// all-zero hash, so ids must fall back to time+coin or every hourly payment
// collides on one ledger PK and gets silently dropped after the first.
func TestFundingID(t *testing.T) {
	entry := func(hash string) userFundingEntry {
		var e userFundingEntry
		e.Time = 1752228006194
		e.Hash = hash
		e.Delta.Coin = "HYPE"
		return e
	}

	cases := []struct {
		name string
		hash string
		want string
	}{
		{"real hash", "0xabc123", "0xabc123:HYPE"},
		{"empty hash", "", "1752228006194:HYPE"},
		{"zero hash (live form)", "0x0000000000000000000000000000000000000000000000000000000000000000", "1752228006194:HYPE"},
		{"zero hash uppercase prefix", "0X0000", "1752228006194:HYPE"},
	}
	for _, tc := range cases {
		if got := fundingID(entry(tc.hash)); got != tc.want {
			t.Errorf("%s: fundingID = %q, want %q", tc.name, got, tc.want)
		}
	}
}
