// Package bybit is a thin, signed Bybit V5 REST client implementing
// exchange.Exchange. Kept as a rudiment behind the interface after the pivot to
// Hyperliquid — it works end-to-end on testnet and may be useful again.
//
// V5 auth: each private request carries the API key, a ms timestamp, a recv
// window, and an HMAC-SHA256 signature over timestamp+apiKey+recvWindow+body.
// The signed bytes must be the exact bytes sent, so the body is marshaled once.
package bybit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/exchange"
)

const TestnetREST = "https://api-testnet.bybit.com"

type Client struct {
	http       *http.Client
	baseURL    string
	apiKey     string
	apiSecret  string
	recvWindow string
}

// New builds a client. recvWindow caps server-clock drift before Bybit rejects a
// request, so clocks matter on the deploy box.
func New(baseURL, apiKey, apiSecret string) (*Client, error) {
	return &Client{
		http:       &http.Client{Timeout: 10 * time.Second},
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		recvWindow: "5000",
	}, nil
}

// APIError is a non-zero retCode from Bybit. Classify maps Code to an ErrorKind.
type APIError struct {
	Code int
	Msg  string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("bybit retCode %d: %s", e.Code, e.Msg)
}

func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	body := map[string]any{
		"category":    req.Category,
		"symbol":      req.Symbol,
		"side":        req.Side,
		"orderType":   "Market",
		"qty":         req.Qty,
		"orderLinkId": req.OrderLinkID,
	}
	// Spot market orders read qty as the quote amount by default; force base-coin
	// so the spot leg matches the perp leg one-to-one.
	if req.Category == exchange.CategorySpot {
		body["marketUnit"] = "baseCoin"
	}
	if req.ReduceOnly {
		body["reduceOnly"] = true
	}

	// V5 order/create returns only ids, not fill price/fee — those need a follow-up
	// execution query. Left at zero here; portfolio P&L on bybit waits on that.
	var res exchange.OrderResult
	if err := c.signedPost(ctx, "/v5/order/create", body, &struct {
		OrderID     *string `json:"orderId"`
		OrderLinkID *string `json:"orderLinkId"`
	}{&res.OrderID, &res.OrderLinkID}); err != nil {
		return nil, err
	}
	return &res, nil
}

// Funding is a stub: real funding history needs a signed GET to
// /v5/account/transaction-log (type=SETTLEMENT), but this client only speaks
// signed POST, and bybit is a rudiment behind the interface after the pivot.
// Reporting no funding keeps the contract satisfied without inflating P&L —
// the same "left until a follow-up query" treatment as PlaceOrder's fill data.
func (c *Client) Funding(_ context.Context, _ string, _ time.Time) ([]exchange.FundingPayment, error) {
	return nil, nil
}

// Classify maps a PlaceOrder error onto an exchange.ErrorKind. A non-APIError is a
// transport blip (transient); duplicate orderLinkId, regulatory bans, and
// insufficient balance are pinned by code with a message fallback, since Bybit's
// exact codes drift across products and releases. Verify codes on testnet.
func (c *Client) Classify(err error) exchange.ErrorKind {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return exchange.ErrTransient
	}
	switch apiErr.Code {
	case 110079, 170130, 110072:
		return exchange.ErrDuplicate
	case 10024: // regulatory/permission — never resolves on retry
		return exchange.ErrTerminal
	case 170131, 110007: // insufficient balance — needs funding, not a retry
		return exchange.ErrTerminal
	}
	msg := strings.ToLower(apiErr.Msg)
	switch {
	case strings.Contains(msg, "duplicate"),
		strings.Contains(msg, "orderlinkid") && strings.Contains(msg, "exist"):
		return exchange.ErrDuplicate
	case strings.Contains(msg, "insufficient balance"),
		strings.Contains(msg, "regulat"):
		return exchange.ErrTerminal
	}
	return exchange.ErrOther
}

// signedPost marshals body once, signs those exact bytes, sends them, and decodes
// result into out. A non-zero retCode becomes an *APIError.
func (c *Client) signedPost(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	sign := c.sign(timestamp + c.apiKey + c.recvWindow + string(payload))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-BAPI-API-KEY", c.apiKey)
	httpReq.Header.Set("X-BAPI-TIMESTAMP", timestamp)
	httpReq.Header.Set("X-BAPI-RECV-WINDOW", c.recvWindow)
	httpReq.Header.Set("X-BAPI-SIGN", sign)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %s", resp.StatusCode, raw)
	}

	var envelope struct {
		RetCode int             `json:"retCode"`
		RetMsg  string          `json:"retMsg"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.RetCode != 0 {
		return &APIError{Code: envelope.RetCode, Msg: envelope.RetMsg}
	}
	if out != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}
	return nil
}

func (c *Client) sign(payload string) string {
	mac := hmac.New(sha256.New, []byte(c.apiSecret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
