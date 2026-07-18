package mock

import (
	"context"
	"testing"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

// TestADLTwoBites pins the two-stage ADL simulation: a partial stub after
// adlAfter (idempotent across reads — repeated States must not keep shrinking
// it), the perp fully gone after 2×adlAfter, the spot untouched throughout.
func TestADLTwoBites(t *testing.T) {
	const adlAfter = 30 * time.Millisecond
	m := New("", adlAfter)
	ctx := context.Background()

	for _, req := range []exchange.OrderRequest{
		{Category: exchange.CategorySpot, Side: exchange.SideBuy, Qty: "1", OrderLinkID: "s"},
		{Category: exchange.CategoryLinear, Side: exchange.SideSell, Qty: "1", OrderLinkID: "p"},
	} {
		if _, err := m.PlaceOrder(ctx, req); err != nil {
			t.Fatalf("place %s: %v", req.Category, err)
		}
	}

	st, _ := m.State(ctx, "X")
	if st.PerpSize != -1 || st.SpotSize != 1 {
		t.Fatalf("fresh position: perp=%v spot=%v, want -1/1", st.PerpSize, st.SpotSize)
	}

	time.Sleep(adlAfter + 10*time.Millisecond)
	st, _ = m.State(ctx, "X")
	if st.PerpSize != -adlPartialFraction {
		t.Fatalf("after first bite: perp=%v, want %v", st.PerpSize, -adlPartialFraction)
	}
	st, _ = m.State(ctx, "X") // must not bite twice
	if st.PerpSize != -adlPartialFraction {
		t.Fatalf("second read after first bite: perp=%v, want %v", st.PerpSize, -adlPartialFraction)
	}

	time.Sleep(adlAfter + 10*time.Millisecond)
	st, _ = m.State(ctx, "X")
	if st.PerpSize != 0 || st.SpotSize != 1 {
		t.Fatalf("after full ADL: perp=%v spot=%v, want 0/1", st.PerpSize, st.SpotSize)
	}
}
