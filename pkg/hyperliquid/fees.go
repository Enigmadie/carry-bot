package hyperliquid

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/ethereum/go-ethereum/common"
)

var _ exchange.FeeReporter = (*Client)(nil)

// quoteToken is the fee token that needs no conversion. Any other fee token is
// the base coin a spot buy received (Hyperliquid charges the buy fee in it).
const quoteToken = "USDC"

// userFill is the slice of a /info "userFills" row that fee accounting needs.
type userFill struct {
	Oid      uint64 `json:"oid"`
	Px       string `json:"px"`
	Fee      string `json:"fee"`
	FeeToken string `json:"feeToken"`
}

// OrderFees reads the account's recent fills (userFills returns the latest
// 2000) and sums the fee of each requested order, converted to quote currency.
func (c *Client) OrderFees(ctx context.Context, orderIDs []string) (map[string]float64, error) {
	if c.account == (common.Address{}) {
		return nil, errors.New("hyperliquid: no account address for fills query")
	}
	want := make(map[uint64]string, len(orderIDs))
	for _, id := range orderIDs {
		oid, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse order id %q: %w", id, err)
		}
		want[oid] = id
	}
	if len(want) == 0 {
		return map[string]float64{}, nil
	}

	var fills []userFill
	req := map[string]string{"type": "userFills", "user": c.account.Hex()}
	if err := c.info(ctx, req, &fills); err != nil {
		return nil, fmt.Errorf("user fills: %w", err)
	}
	return sumFees(fills, want)
}

// sumFees adds up fees per wanted order id. An IOC that swept several book
// levels comes back as several fills under one oid.
func sumFees(fills []userFill, want map[uint64]string) (map[string]float64, error) {
	out := make(map[string]float64, len(want))
	for _, f := range fills {
		id, ok := want[f.Oid]
		if !ok {
			continue
		}
		fee, err := strconv.ParseFloat(f.Fee, 64)
		if err != nil {
			return nil, fmt.Errorf("parse fee %q: %w", f.Fee, err)
		}
		if f.FeeToken != quoteToken {
			px, err := strconv.ParseFloat(f.Px, 64)
			if err != nil {
				return nil, fmt.Errorf("parse fill px %q: %w", f.Px, err)
			}
			fee *= px
		}
		out[id] += fee
	}
	return out, nil
}
