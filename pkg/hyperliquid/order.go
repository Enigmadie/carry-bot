package hyperliquid

// PlaceOrder implements the exchange.Exchange contract for Hyperliquid. The hard
// parts that differ from a CEX:
//
//   - No market order. Hyperliquid only has limit orders, so a "market" buy/sell
//     is an IOC (immediate-or-cancel) limit priced through the book — we fetch the
//     mid price and shove the limit defaultSlippage past it so it crosses and fills
//     now, cancelling any unfilled remainder instead of resting.
//   - Tick/lot rules. Size rounds to the instrument's szDecimals; price rounds to 5
//     significant figures and at most (6 − szDecimals) decimals for perps, (8 −
//     szDecimals) for spot. A price that breaks these is silently rejected.
//   - Idempotency via cloid. JetStream redelivers, so we derive a deterministic
//     128-bit client order id from OrderLinkID; a replay carries the same cloid.
//
// The order action is msgpack-hashed and EIP-712 signed (sign.go), then POSTed to
// /exchange as {action, nonce, signature}. The same action object goes out as JSON,
// so the bytes the exchange re-hashes match what we signed.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/ethereum/go-ethereum/crypto"
)

// Compile-time proof the client satisfies the provider contract order-service trades through.
var _ exchange.Exchange = (*Client)(nil)

// defaultSlippage is how far past the mid an IOC "market" leg is priced so it
// crosses the book and fills immediately. 5% mirrors the reference SDK's
// market-order default — generous enough to fill, the realized price is the book's.
const defaultSlippage = 0.05

// wire* are the exact on-wire shape of an order action. Field order and msgpack
// tags must match the reference SDK byte-for-byte: the action is msgpack-hashed
// for the signature, so any reordering changes the hash and the order is rejected.
type wireLimit struct {
	Tif string `msgpack:"tif" json:"tif"` // "Ioc" = immediate-or-cancel
}

type wireOrderType struct {
	Limit wireLimit `msgpack:"limit" json:"limit"`
}

type wireOrder struct {
	Asset      uint32        `msgpack:"a" json:"a"`
	IsBuy      bool          `msgpack:"b" json:"b"`
	Price      string        `msgpack:"p" json:"p"`
	Size       string        `msgpack:"s" json:"s"`
	ReduceOnly bool          `msgpack:"r" json:"r"`
	Type       wireOrderType `msgpack:"t" json:"t"`
	Cloid      string        `msgpack:"c,omitempty" json:"c,omitempty"` // 128-bit client id; last, like the SDK
}

type orderAction struct {
	Type     string      `msgpack:"type" json:"type"`
	Orders   []wireOrder `msgpack:"orders" json:"orders"`
	Grouping string      `msgpack:"grouping" json:"grouping"` // "na" = independent orders
}

// exchangeReq is the /exchange request envelope: the signed action, the nonce that
// was folded into the signature, and the r/s/v. VaultAddress is set only for
// sub-account/vault trading.
type exchangeReq struct {
	Action       any       `json:"action"`
	Nonce        uint64    `json:"nonce"`
	Signature    Signature `json:"signature"`
	VaultAddress *string   `json:"vaultAddress,omitempty"`
}

// exchangeResp is the /exchange reply. status is "ok" or "err"; on "err" response
// is a plain error string, on "ok" it is a typed object we decode per action.
type exchangeResp struct {
	Status   string          `json:"status"`
	Response json.RawMessage `json:"response"`
}

// orderStatus is one entry of an "ok" order response's statuses array. Exactly one
// of resting/filled is set, or error carries a per-order rejection message.
type orderStatus struct {
	Resting *struct {
		Oid uint64 `json:"oid"`
	} `json:"resting"`
	Filled *struct {
		TotalSz string `json:"totalSz"`
		AvgPx   string `json:"avgPx"`
		Oid     uint64 `json:"oid"`
	} `json:"filled"`
	Error string `json:"error"`
}

// APIError is a Hyperliquid-reported rejection (a top-level "err" status or a
// per-order error). Classify maps its message onto an ErrorKind, since Hyperliquid
// returns human strings rather than Bybit-style numeric codes.
type APIError struct {
	Msg string
}

func (e *APIError) Error() string { return "hyperliquid: " + e.Msg }

func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	if c.key == nil {
		return nil, errors.New("hyperliquid: no signing key configured")
	}
	ref, err := c.resolveAsset(req.Category, req.Symbol)
	if err != nil {
		return nil, err
	}
	qty, err := strconv.ParseFloat(req.Qty, 64)
	if err != nil {
		return nil, fmt.Errorf("parse qty %q: %w", req.Qty, err)
	}
	isBuy := req.Side == exchange.SideBuy
	isSpot := ref.Asset >= spotAssetOffset

	// "Market" leg → IOC limit priced through the book off the current mid.
	mid, err := c.midPrice(ctx, ref, stripQuote(req.Symbol))
	if err != nil {
		return nil, err
	}
	limitPx := slippagePrice(mid, isBuy)

	action := orderAction{
		Type: "order",
		Orders: []wireOrder{{
			Asset:      uint32(ref.Asset),
			IsBuy:      isBuy,
			Price:      formatPrice(limitPx, ref.SzDecimals, isSpot),
			Size:       formatSize(qty, ref.SzDecimals),
			ReduceOnly: req.ReduceOnly,
			Type:       wireOrderType{Limit: wireLimit{Tif: "Ioc"}},
			Cloid:      cloidFromLinkID(req.OrderLinkID),
		}},
		Grouping: "na",
	}

	nonce := uint64(time.Now().UnixMilli())
	sig, err := signL1Action(c.key, action, c.vault, nonce, c.mainnet)
	if err != nil {
		return nil, fmt.Errorf("sign order: %w", err)
	}

	body := exchangeReq{Action: action, Nonce: nonce, Signature: sig}
	if c.vault != nil {
		v := c.vault.Hex()
		body.VaultAddress = &v
	}

	var resp exchangeResp
	if err := c.post(ctx, "/exchange", body, &resp); err != nil {
		return nil, err // transport / non-200 → Classify treats as transient
	}
	return parseOrderResp(req.OrderLinkID, resp)
}

// parseOrderResp turns an /exchange order reply into an OrderResult, or an
// *APIError. An IOC that crossed comes back "filled"; "resting" means nothing
// matched at our price (treated as a failure so leg-risk can act); a per-order or
// top-level error surfaces its message for Classify.
func parseOrderResp(linkID string, resp exchangeResp) (*exchange.OrderResult, error) {
	if resp.Status != "ok" {
		var msg string
		if json.Unmarshal(resp.Response, &msg) != nil || msg == "" {
			msg = string(resp.Response)
		}
		return nil, &APIError{Msg: msg}
	}

	var inner struct {
		Data struct {
			Statuses []orderStatus `json:"statuses"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Response, &inner); err != nil {
		return nil, fmt.Errorf("decode order response: %w", err)
	}
	if len(inner.Data.Statuses) == 0 {
		return nil, &APIError{Msg: "no order status returned"}
	}

	st := inner.Data.Statuses[0]
	switch {
	case st.Error != "":
		return nil, &APIError{Msg: st.Error}
	case st.Filled != nil:
		px, _ := strconv.ParseFloat(st.Filled.AvgPx, 64)
		sz, _ := strconv.ParseFloat(st.Filled.TotalSz, 64)
		// Fee is not on the order ack — it needs a userFills follow-up — so it's
		// left at 0, the same deferral as bybit's fill data.
		return &exchange.OrderResult{
			OrderID:     strconv.FormatUint(st.Filled.Oid, 10),
			OrderLinkID: linkID,
			Price:       px,
			FilledQty:   sz,
		}, nil
	case st.Resting != nil:
		return nil, &APIError{Msg: fmt.Sprintf("order resting (oid %d), did not fill", st.Resting.Oid)}
	default:
		return nil, &APIError{Msg: "unrecognized order status"}
	}
}

// Classify maps a PlaceOrder error onto an ErrorKind. A non-APIError is a transport
// blip (transient). Hyperliquid returns human strings, so terminal cases (balance,
// margin, reduce-only) and duplicate-cloid are matched on substrings — verify the
// exact wording on testnet (срез 5), the same discipline as bybit's classifier.
func (c *Client) Classify(err error) exchange.ErrorKind {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return exchange.ErrTransient
	}
	msg := strings.ToLower(apiErr.Msg)
	switch {
	case strings.Contains(msg, "already") && (strings.Contains(msg, "cloid") || strings.Contains(msg, "client order")):
		return exchange.ErrDuplicate
	case strings.Contains(msg, "insufficient"),
		strings.Contains(msg, "margin"),
		strings.Contains(msg, "reduce only"),
		strings.Contains(msg, "reduce-only"):
		return exchange.ErrTerminal
	}
	return exchange.ErrOther
}

// midPrice fetches the current mid for an instrument from /info "allMids". Perps
// are keyed by coin; spot pairs by "@<index>" where index is the asset id minus
// spotAssetOffset.
func (c *Client) midPrice(ctx context.Context, ref assetRef, coin string) (float64, error) {
	var mids map[string]string
	if err := c.info(ctx, map[string]string{"type": "allMids"}, &mids); err != nil {
		return 0, fmt.Errorf("all mids: %w", err)
	}
	key := coin
	if ref.Asset >= spotAssetOffset {
		key = "@" + strconv.Itoa(ref.Asset-spotAssetOffset)
	}
	s, ok := mids[key]
	if !ok {
		return 0, fmt.Errorf("no mid price for %q (key %q)", coin, key)
	}
	px, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse mid %q: %w", s, err)
	}
	return px, nil
}

// slippagePrice pushes the mid defaultSlippage in the aggressive direction so an
// IOC limit crosses: above the mid to buy, below to sell.
func slippagePrice(mid float64, isBuy bool) float64 {
	if isBuy {
		return mid * (1 + defaultSlippage)
	}
	return mid * (1 - defaultSlippage)
}

// cloidFromLinkID derives a deterministic 128-bit client order id (0x + 32 hex)
// from OrderLinkID, so a redelivered intent carries the identical cloid and the
// exchange can dedupe it — the same idempotency role orderLinkId plays on bybit.
func cloidFromLinkID(linkID string) string {
	h := crypto.Keccak256([]byte(linkID))
	return "0x" + hex.EncodeToString(h[:16])
}

// formatSize rounds a size to the instrument's szDecimals and renders it without
// trailing zeros, the wire form Hyperliquid expects.
func formatSize(sz float64, szDecimals int) string {
	return trimZeros(strconv.FormatFloat(sz, 'f', szDecimals, 64))
}

// formatPrice applies Hyperliquid's two price rules: at most 5 significant figures
// and at most (maxDecimals − szDecimals) decimal places, where maxDecimals is 6 for
// perps and 8 for spot. Integer prices are always allowed.
func formatPrice(px float64, szDecimals int, spot bool) string {
	maxDec := 6
	if spot {
		maxDec = 8
	}
	dec := maxDec - szDecimals
	if dec < 0 {
		dec = 0
	}
	// Clamp to 5 significant figures first, then to the decimal-place cap.
	sig, _ := strconv.ParseFloat(strconv.FormatFloat(px, 'g', 5, 64), 64)
	return trimZeros(strconv.FormatFloat(sig, 'f', dec, 64))
}

// trimZeros drops trailing zeros (and a dangling decimal point) from a fixed-point
// string: "65000.0" → "65000", "1.2300" → "1.23".
func trimZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	return strings.TrimRight(strings.TrimRight(s, "0"), ".")
}
