package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The API caps a reply at 500 rows, so a full window arrives in pages and the
// client must resume past the last row rather than re-request the same start —
// otherwise it either loops forever or double-counts the boundary settlement.
func TestFundingHistoryPaginates(t *testing.T) {
	const total = fundingHistoryPage + 7
	base := time.UnixMilli(1_700_000_000_000).UTC()

	var starts []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Type      string `json:"type"`
			Coin      string `json:"coin"`
			StartTime int64  `json:"startTime"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Type != "fundingHistory" || req.Coin != "HYPE" {
			t.Errorf("unexpected request %+v", req)
		}
		starts = append(starts, req.StartTime)

		rows := make([]fundingHistoryEntry, 0, fundingHistoryPage)
		for i := 0; i < total; i++ {
			ms := base.UnixMilli() + int64(i)*int64(time.Hour/time.Millisecond)
			if ms < req.StartTime {
				continue
			}
			rows = append(rows, fundingHistoryEntry{
				Coin:        "HYPE",
				FundingRate: fmt.Sprintf("0.0000%03d", i),
				Premium:     "0.0001",
				Time:        ms,
			})
			if len(rows) == fundingHistoryPage {
				break
			}
		}
		json.NewEncoder(w).Encode(rows)
	}))
	defer srv.Close()

	c, err := New(Config{BaseURL: srv.URL, Mainnet: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := c.FundingHistory(context.Background(), "HYPEUSDC", base, base.Add(total*time.Hour))
	if err != nil {
		t.Fatalf("FundingHistory: %v", err)
	}
	if len(got) != total {
		t.Fatalf("got %d ticks, want %d", len(got), total)
	}
	if len(starts) != 2 {
		t.Fatalf("got %d requests, want 2 pages", len(starts))
	}
	// Second page resumes one millisecond past the last row of the first.
	wantResume := base.UnixMilli() + int64(fundingHistoryPage-1)*int64(time.Hour/time.Millisecond) + 1
	if starts[1] != wantResume {
		t.Errorf("page 2 startTime = %d, want %d", starts[1], wantResume)
	}
	for i := 1; i < len(got); i++ {
		if !got[i].Time.After(got[i-1].Time) {
			t.Fatalf("tick %d repeats or rewinds time: %s after %s", i, got[i].Time, got[i-1].Time)
		}
	}
}

// A short page means the window is exhausted; the client must stop instead of
// asking again from the last row and spinning on an empty tail.
func TestFundingHistoryStopsOnShortPage(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode([]fundingHistoryEntry{
			{Coin: "HYPE", FundingRate: "0.0000125", Premium: "0", Time: 1_700_000_000_000},
		})
	}))
	defer srv.Close()

	c, err := New(Config{BaseURL: srv.URL, Mainnet: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	start := time.UnixMilli(1_700_000_000_000).UTC()
	got, err := c.FundingHistory(context.Background(), "HYPE", start, start.Add(720*time.Hour))
	if err != nil {
		t.Fatalf("FundingHistory: %v", err)
	}
	if calls != 1 {
		t.Errorf("made %d requests, want 1", calls)
	}
	if len(got) != 1 || got[0].Rate != 0.0000125 {
		t.Errorf("got %+v, want one tick at 0.0000125", got)
	}
}
