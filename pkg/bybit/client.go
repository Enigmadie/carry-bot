// Package bybit is a thin REST client for Bybit's V5 trading API. It signs
// requests and exposes only what order-service needs today (placing market
// orders); reconciliation and account queries come later.
//
// Bybit V5 auth: every private request carries four headers — the API key, a
// millisecond timestamp, a recv-window, and an HMAC-SHA256 signature. The
// signature is hex(HMAC(secret, timestamp + apiKey + recvWindow + payload)),
// where payload is the raw request body for POST (or the query string for GET).
// The body bytes that are signed must be byte-for-byte the bytes that are sent,
// so we marshal once and reuse the result.
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
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// TestnetREST is Bybit's testnet REST host; the bot defaults to it.
const TestnetREST = "https://api-testnet.bybit.com"

// Order categories.
const (
	CategorySpot   = "spot"
	CategoryLinear = "linear"
)

// Order sides.
const (
	SideBuy  = "Buy"
	SideSell = "Sell"
)

// Client is a signed Bybit V5 REST client. It is safe for concurrent use.
type Client struct {
	http       *http.Client
	baseURL    string
	apiKey     string
	apiSecret  string
	recvWindow string
}

// New builds a client. recvWindow caps how far the server timestamp may drift
// from ours before Bybit rejects the request, so accurate clocks matter on the
// deploy box. bindAddr selects the outbound source IP; "" uses the default route.
func New(baseURL, apiKey, apiSecret, bindAddr string) (*Client, error) {
	hc, err := BoundHTTPClient(bindAddr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		http:       hc,
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		recvWindow: "5000",
	}, nil
}

// BoundHTTPClient returns an *http.Client whose outbound connections use
// bindAddr as their source IP; empty bindAddr keeps the default route, so the
// bind is opt-in. Pass timeout 0 for long-lived connections like WebSockets — a
// non-zero http.Client.Timeout caps the whole connection, not just the handshake.
//
// Binding the source IP lets Bybit traffic egress on a chosen interface
// independently of the rest of the process.
func BoundHTTPClient(bindAddr string, timeout time.Duration) (*http.Client, error) {
	if bindAddr == "" {
		return &http.Client{Timeout: timeout}, nil
	}
	ip := net.ParseIP(bindAddr)
	if ip == nil {
		return nil, fmt.Errorf("invalid bind addr %q", bindAddr)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		LocalAddr: &net.TCPAddr{IP: ip},
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	return &http.Client{Timeout: timeout, Transport: transport}, nil
}

// OrderRequest is the subset of /v5/order/create fields the bot uses. Bybit
// wants every numeric value as a string.
type OrderRequest struct {
	Category    string // CategorySpot | CategoryLinear
	Symbol      string
	Side        string // SideBuy | SideSell
	Qty         string // base-coin amount (see MarketUnit for spot)
	OrderLinkID string // client-side id; Bybit dedupes on it (idempotency key)
	ReduceOnly  bool   // perp only: this order may only shrink an existing position
}

// OrderResult is the useful slice of a successful order/create response.
type OrderResult struct {
	OrderID     string
	OrderLinkID string
}

// APIError is a non-zero retCode from Bybit. The caller inspects Code to tell a
// retryable transport-level problem (there is none here — that surfaces as a
// plain error) from a business rejection like a duplicate order link id.
type APIError struct {
	Code int
	Msg  string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("bybit retCode %d: %s", e.Code, e.Msg)
}

// IsDuplicate reports whether err means "an order with this orderLinkId already
// exists". That is the idempotency signal: a JetStream redelivery replays the
// same intent, we rebuild the same orderLinkId, and Bybit refuses the second
// copy — which for us is success, not failure. Bybit's exact code varies by
// product and has shifted across releases, so we also fall back to the message
// text. Verify the precise code on testnet before trusting real money.
func IsDuplicate(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case 10001: // generic params error — disambiguated by message below
	case 110079, 170130, 110072: // observed "orderLinkId duplicate" variants
		return true
	}
	msg := strings.ToLower(apiErr.Msg)
	return strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "orderlinkid") && strings.Contains(msg, "exist")
}

// PlaceOrder submits a market order. A duplicate orderLinkId is reported as an
// APIError; callers use IsDuplicate to treat it as an idempotent no-op.
func (c *Client) PlaceOrder(ctx context.Context, req OrderRequest) (*OrderResult, error) {
	body := map[string]any{
		"category":    req.Category,
		"symbol":      req.Symbol,
		"side":        req.Side,
		"orderType":   "Market",
		"qty":         req.Qty,
		"orderLinkId": req.OrderLinkID,
	}
	// Spot market orders default to interpreting qty as the quote amount; we
	// want a base-coin amount so the spot leg matches the perp leg one-to-one.
	if req.Category == CategorySpot {
		body["marketUnit"] = "baseCoin"
	}
	if req.ReduceOnly {
		body["reduceOnly"] = true
	}

	var res OrderResult
	if err := c.signedPost(ctx, "/v5/order/create", body, &struct {
		OrderID     *string `json:"orderId"`
		OrderLinkID *string `json:"orderLinkId"`
	}{&res.OrderID, &res.OrderLinkID}); err != nil {
		return nil, err
	}
	return &res, nil
}

// signedPost marshals body once, signs those exact bytes, sends them, and
// decodes result into out. A non-zero retCode becomes an *APIError.
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
