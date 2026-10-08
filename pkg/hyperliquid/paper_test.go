package hyperliquid

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Settlements land a few dozen ms past the hour, so SettledFunding must match on
// the hour rather than the exact timestamp, and report not-yet-published hours
// as such rather than as a zero rate — a zero would book a fake settlement.
func TestSettledFunding(t *testing.T) {
	hour := time.Date(2026, 10, 1, 16, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]fundingHistoryEntry{
			{Coin: "HYPE", FundingRate: "-0.0000342", Time: hour.UnixMilli() + 130},
		})
	}))
	defer srv.Close()

	c, err := New(Config{BaseURL: srv.URL, Mainnet: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A negative rate must survive intact — it is the signal the exit rule keys
	// on, so silently reading it as zero would strand a losing position.
	rate, ok, err := c.SettledFunding(context.Background(), "HYPEUSDC", hour)
	if err != nil || !ok || rate != -0.0000342 {
		t.Fatalf("SettledFunding(16:00) = %v, %v, %v; want -0.0000342, true, nil", rate, ok, err)
	}
	if _, ok, err := c.SettledFunding(context.Background(), "HYPEUSDC", hour.Add(time.Hour)); err != nil || ok {
		t.Fatalf("SettledFunding(17:00) ok = %v, err = %v; want not yet published", ok, err)
	}
}
