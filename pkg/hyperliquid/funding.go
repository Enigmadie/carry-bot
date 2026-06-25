package hyperliquid

// Funding reads realized funding settlements from /info "userFunding". We hold the
// perp short, so under positive funding delta.usdc is a positive credit — the bot's
// actual revenue. The query is per-account (not per-key): with an agent wallet the
// signing key differs from the master account, so it targets c.account (Config.Account
// or its fallback), and an empty result usually means the account address is wrong.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/ethereum/go-ethereum/common"
)

// userFundingEntry is one row of the userFunding reply. The settlement amount is
// delta.usdc (signed quote currency); time is a millisecond timestamp; hash is the
// on-chain event hash several coins may share in one funding tick.
type userFundingEntry struct {
	Time  int64  `json:"time"`
	Hash  string `json:"hash"`
	Delta struct {
		Type        string `json:"type"`
		Coin        string `json:"coin"`
		Usdc        string `json:"usdc"`
		FundingRate string `json:"fundingRate"`
	} `json:"delta"`
}

func (c *Client) Funding(ctx context.Context, symbol string, since time.Time) ([]exchange.FundingPayment, error) {
	if c.account == (common.Address{}) {
		return nil, errors.New("hyperliquid: no account address for funding query")
	}

	// startTime is inclusive on the API; +1ms makes our `since` exclusive so the
	// boundary settlement isn't re-emitted on the next poll.
	req := map[string]any{
		"type":      "userFunding",
		"user":      c.account.Hex(),
		"startTime": since.UnixMilli() + 1,
	}
	var entries []userFundingEntry
	if err := c.info(ctx, req, &entries); err != nil {
		return nil, fmt.Errorf("user funding: %w", err)
	}

	coin := stripQuote(symbol)
	out := make([]exchange.FundingPayment, 0, len(entries))
	for _, e := range entries {
		if e.Delta.Coin != coin {
			continue // userFunding spans all positions; keep only the asked symbol
		}
		amt, err := strconv.ParseFloat(e.Delta.Usdc, 64)
		if err != nil {
			return nil, fmt.Errorf("parse funding usdc %q: %w", e.Delta.Usdc, err)
		}
		out = append(out, exchange.FundingPayment{
			ID:     fundingID(e),
			Symbol: symbol,
			Amount: amt,
			Time:   time.UnixMilli(e.Time).UTC(),
		})
	}
	return out, nil
}

// fundingID is the dedup key downstream books on. Hyperliquid has no single
// settlement id, so we compose the event hash with the coin (one hash can settle
// several coins) — stable across redelivery. Falls back to time+coin if no hash.
func fundingID(e userFundingEntry) string {
	if e.Hash != "" {
		return e.Hash + ":" + e.Delta.Coin
	}
	return strconv.FormatInt(e.Time, 10) + ":" + e.Delta.Coin
}
