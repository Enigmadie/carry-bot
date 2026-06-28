// Package hyperliquid is a thin client for the Hyperliquid exchange implementing
// exchange.Exchange. Unlike Bybit (an HMAC-signed REST request), Hyperliquid
// authorizes every trading action with an EIP-712 wallet signature (see sign.go)
// and addresses instruments by a numeric asset index rather than a symbol, so the
// client first loads exchange metadata (meta.go) to resolve symbols and decimals.
//
// Two endpoints carry everything: POST /info (public market and account data,
// unsigned) and POST /exchange (signed actions — orders, cancels). There is no
// REST envelope like Bybit's retCode; /info returns the result directly and
// /exchange returns a {status, response} object.
package hyperliquid

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	MainnetAPI = "https://api.hyperliquid.xyz"
	TestnetAPI = "https://api.hyperliquid-testnet.xyz"
)

// Config builds a Client. PrivateKey is the agent-wallet key (hex, 0x optional)
// that signs actions; it may be empty for an info-only client. Mainnet must match
// BaseURL — it selects the phantom-agent source, and a mismatch makes every
// signature invalid.
type Config struct {
	BaseURL    string // REST host; "" → MainnetAPI/TestnetAPI per Mainnet
	Mainnet    bool   // phantom-agent source: true = "a", false = "b"
	PrivateKey string // agent-wallet signing key; empty = no signed actions
	Vault      string // optional sub-account/vault address; "" = trade as self
	Account    string // master account for user queries (funding/fills); "" → derived
}

type Client struct {
	http    *http.Client
	baseURL string
	mainnet bool
	key     *ecdsa.PrivateKey   // nil if no signing key was provided
	vault   *common.Address     // nil = trade as self
	account common.Address      // address user queries target (funding/fills)
	mu      sync.RWMutex        // guards the asset maps, refreshable at runtime
	assets  map[string]assetRef // key: "<category>:<symbol>", e.g. "linear:BTCUSDT"
}

// assetRef is what an order needs about an instrument: its numeric id and the
// decimal precision its size (and, derived, its price) must be rounded to.
type assetRef struct {
	Asset      int
	SzDecimals int
}

func New(cfg Config) (*Client, error) {
	// Default the REST host to match the network the signature targets: defaulting
	// to testnet while Mainnet is set would silently point /info and /exchange at a
	// different network than the one the phantom-agent source signs for.
	base := cfg.BaseURL
	if base == "" {
		base = TestnetAPI
		if cfg.Mainnet {
			base = MainnetAPI
		}
	}

	c := &Client{
		http:    &http.Client{Timeout: 10 * time.Second},
		baseURL: strings.TrimRight(base, "/"),
		mainnet: cfg.Mainnet,
		assets:  map[string]assetRef{},
	}

	if cfg.PrivateKey != "" {
		key, err := crypto.HexToECDSA(strings.TrimPrefix(cfg.PrivateKey, "0x"))
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		c.key = key
	}
	if cfg.Vault != "" {
		if !common.IsHexAddress(cfg.Vault) {
			return nil, fmt.Errorf("invalid vault address %q", cfg.Vault)
		}
		addr := common.HexToAddress(cfg.Vault)
		c.vault = &addr
	}

	// account for user queries (funding/fills): an explicit master address wins;
	// otherwise the vault, otherwise the signing key's own address. The fallback is
	// correct only when the key IS the account key — with a separate agent wallet
	// the master differs, so Account must be set or funding queries come back empty.
	switch {
	case cfg.Account != "":
		if !common.IsHexAddress(cfg.Account) {
			return nil, fmt.Errorf("invalid account address %q", cfg.Account)
		}
		c.account = common.HexToAddress(cfg.Account)
	case c.vault != nil:
		c.account = *c.vault
	case c.key != nil:
		c.account = crypto.PubkeyToAddress(c.key.PublicKey)
	}

	return c, nil
}

// info POSTs an unsigned request to /info and decodes the result into out. The
// request body is a JSON object whose "type" selects the query (e.g. "meta").
func (c *Client) info(ctx context.Context, req any, out any) error {
	return c.post(ctx, "/info", req, out)
}

// post sends body as JSON to path and decodes the response into out. A non-200
// is returned as an error carrying the body; callers that need to classify
// /exchange failures parse out from the decoded response instead.
func (c *Client) post(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

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
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
