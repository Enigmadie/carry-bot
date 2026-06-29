package hyperliquid

// Hyperliquid opens a fresh perp position in isolated margin at the account's
// default leverage (10x) unless the margin mode is set first. For a delta-neutral
// carry the perp short must be backed by the spot USDC, which only happens in
// cross margin — in isolated the short's loss is capped by its own posted margin,
// so closing through a book detached from the oracle mark can realize more loss
// than that margin and the close is rejected ("Insufficient margin"). So before
// opening the perp leg we switch the asset to cross margin at a low leverage with
// an updateLeverage L1 action, signed the same way as an order (sign.go).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

// leverageAction is the on-wire shape of an updateLeverage action. Field order and
// msgpack tags must match the reference SDK byte-for-byte — the action is
// msgpack-hashed for the signature, so any reordering changes the hash and the
// action is rejected.
type leverageAction struct {
	Type     string `msgpack:"type" json:"type"`
	Asset    uint32 `msgpack:"asset" json:"asset"`
	IsCross  bool   `msgpack:"isCross" json:"isCross"`
	Leverage uint32 `msgpack:"leverage" json:"leverage"`
}

// UpdateLeverage sets the margin mode and leverage for a perp instrument. It is not
// part of the exchange.Exchange contract (a CEX sets this out of band); order-service
// calls it on the Hyperliquid client directly at startup, the same way it calls
// LoadMeta. The setting persists on the account, so a single call before trading is
// enough, and the action is idempotent — re-sending the same mode/leverage is a no-op.
func (c *Client) UpdateLeverage(ctx context.Context, symbol string, isCross bool, leverage int) error {
	if c.key == nil {
		return errors.New("hyperliquid: no signing key configured")
	}
	if leverage <= 0 {
		return fmt.Errorf("hyperliquid: leverage must be positive, got %d", leverage)
	}
	ref, err := c.resolveAsset(exchange.CategoryLinear, symbol)
	if err != nil {
		return err
	}
	if ref.Asset >= spotAssetOffset {
		return fmt.Errorf("hyperliquid: %q resolves to a spot asset; leverage is perp-only", symbol)
	}

	action := leverageAction{
		Type:     "updateLeverage",
		Asset:    uint32(ref.Asset),
		IsCross:  isCross,
		Leverage: uint32(leverage),
	}

	nonce := uint64(time.Now().UnixMilli())
	sig, err := signL1Action(c.key, action, c.vault, nonce, c.mainnet)
	if err != nil {
		return fmt.Errorf("sign updateLeverage: %w", err)
	}

	body := exchangeReq{Action: action, Nonce: nonce, Signature: sig}
	if c.vault != nil {
		v := c.vault.Hex()
		body.VaultAddress = &v
	}

	var resp exchangeResp
	if err := c.post(ctx, "/exchange", body, &resp); err != nil {
		return err
	}
	if resp.Status != "ok" {
		var msg string
		if json.Unmarshal(resp.Response, &msg) != nil || msg == "" {
			msg = string(resp.Response)
		}
		return &APIError{Msg: msg}
	}
	return nil
}
