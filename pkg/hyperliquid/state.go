package hyperliquid

// State reads the account's live position for reconciliation. Three unsigned
// /info queries against the master account: perp positions from
// clearinghouseState, spot balances (and, under a unified account, the USDC
// collateral — the per-perp-dex clearinghouseState shows a meaningless 0 there)
// from spotClearinghouseState, and openOrders as an anomaly check — the bot
// trades IOC only, so anything resting was not placed by this code path.

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/ethereum/go-ethereum/common"
)

// clearinghouseResp is the /info "clearinghouseState" reply, trimmed to the perp
// positions. szi is the signed position size in base coin (short negative).
type clearinghouseResp struct {
	AssetPositions []struct {
		Position struct {
			Coin string `json:"coin"`
			Szi  string `json:"szi"`
		} `json:"position"`
	} `json:"assetPositions"`
}

// spotClearinghouseResp is the /info "spotClearinghouseState" reply: one balance
// row per token the account holds. total includes amounts on hold in orders.
type spotClearinghouseResp struct {
	Balances []struct {
		Coin  string `json:"coin"` // token name, e.g. "USDC", "HYPE", "UBTC"
		Total string `json:"total"`
	} `json:"balances"`
}

func (c *Client) State(ctx context.Context, symbol string) (*exchange.PositionState, error) {
	if c.account == (common.Address{}) {
		return nil, errors.New("hyperliquid: no account address for state query")
	}
	user := c.account.Hex()

	var perp clearinghouseResp
	if err := c.info(ctx, map[string]string{"type": "clearinghouseState", "user": user}, &perp); err != nil {
		return nil, fmt.Errorf("clearinghouse state: %w", err)
	}
	var spot spotClearinghouseResp
	if err := c.info(ctx, map[string]string{"type": "spotClearinghouseState", "user": user}, &spot); err != nil {
		return nil, fmt.Errorf("spot clearinghouse state: %w", err)
	}
	var orders []struct {
		Coin string `json:"coin"`
	}
	if err := c.info(ctx, map[string]string{"type": "openOrders", "user": user}, &orders); err != nil {
		return nil, fmt.Errorf("open orders: %w", err)
	}

	st, err := buildState(symbol, perp, spot)
	if err != nil {
		return nil, err
	}
	st.OpenOrders = len(orders)
	return st, nil
}

// buildState folds the two clearinghouse replies into a PositionState for one
// symbol. The perp leg is keyed by the bare coin; the spot leg by the (possibly
// Unit-aliased) base token; collateral is the USDC balance.
func buildState(symbol string, perp clearinghouseResp, spot spotClearinghouseResp) (*exchange.PositionState, error) {
	coin := stripQuote(symbol)
	spotToken := spotBaseAlias(coin)
	st := &exchange.PositionState{}

	for _, p := range perp.AssetPositions {
		if p.Position.Coin != coin {
			continue
		}
		szi, err := strconv.ParseFloat(p.Position.Szi, 64)
		if err != nil {
			return nil, fmt.Errorf("parse perp szi %q: %w", p.Position.Szi, err)
		}
		st.PerpSize = szi
	}

	for _, b := range spot.Balances {
		total, err := strconv.ParseFloat(b.Total, 64)
		if err != nil {
			return nil, fmt.Errorf("parse spot balance %q for %s: %w", b.Total, b.Coin, err)
		}
		switch b.Coin {
		case spotToken:
			st.SpotSize = total
		case "USDC":
			st.Collateral = total
		}
	}
	return st, nil
}
