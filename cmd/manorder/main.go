// Command manorder places one manual order — the "manual intervention" tool for
// an unbalanced halt: flatten a naked leg, then restart order-service to
// reconcile clean. Uses the same env as order-service plus MAN_CATEGORY
// (spot|linear), MAN_SIDE (Buy|Sell), MAN_QTY.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/Enigmadie/carry-bot/pkg/hyperliquid"
)

func main() {
	category := os.Getenv("MAN_CATEGORY")
	side := os.Getenv("MAN_SIDE")
	qty := os.Getenv("MAN_QTY")
	symbol := getenv("SYMBOL", "BTCUSDT")
	if category == "" || side == "" || qty == "" {
		fmt.Fprintln(os.Stderr, "MAN_CATEGORY (spot|linear), MAN_SIDE (Buy|Sell), MAN_QTY are required")
		os.Exit(2)
	}

	c, err := hyperliquid.New(hyperliquid.Config{
		BaseURL:    os.Getenv("HL_API"),
		Mainnet:    os.Getenv("HL_MAINNET") == "true",
		PrivateKey: os.Getenv("HL_PRIVATE_KEY"),
		Vault:      os.Getenv("HL_VAULT"),
		Account:    os.Getenv("HL_ACCOUNT"),
		Slippage:   getfloat("HL_SLIPPAGE"),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.LoadMeta(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "load meta:", err)
		os.Exit(1)
	}

	res, err := c.PlaceOrder(ctx, exchange.OrderRequest{
		Category:    category,
		Symbol:      symbol,
		Side:        side,
		Qty:         qty,
		OrderLinkID: fmt.Sprintf("manorder-%d", time.Now().UnixNano()),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "place order:", err)
		os.Exit(1)
	}
	fmt.Printf("placed: order_id=%s price=%v fee=%v\n", res.OrderID, res.Price, res.Fee)

	st, err := c.State(ctx, symbol)
	if err != nil {
		fmt.Fprintln(os.Stderr, "state:", err)
		os.Exit(1)
	}
	fmt.Printf("state after: perp=%v spot=%v collateral=%v open_orders=%d\n",
		st.PerpSize, st.SpotSize, st.Collateral, st.OpenOrders)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getfloat(key string) float64 {
	var f float64
	fmt.Sscanf(os.Getenv(key), "%g", &f)
	return f
}
