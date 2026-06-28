// Command market-data connects to Hyperliquid's public WebSocket and republishes
// perp/spot prices and the perp funding rate onto NATS for the rest of the system.
//
// Hyperliquid multiplexes feeds as named channels over a single socket, unlike
// Bybit's one-subscription-per-connection model: we open one WebSocket and
// subscribe to allMids (mid prices for every instrument, keyed by coin for perps
// and "@<index>" for spot) and activeAssetCtx (per-coin context carrying the
// funding rate). Hyperliquid keys spot mids by a numeric index, so the instrument
// keys are resolved once at startup from /info metadata (see pkg/hyperliquid).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/hyperliquid"
	"github.com/Enigmadie/carry-bot/pkg/metrics"
)

const (
	// Hyperliquid drops an idle socket after ~60s; ping well inside that window.
	pingInterval   = 15 * time.Second
	reconnectDelay = 3 * time.Second

	mainnetWS = "wss://api.hyperliquid.xyz/ws"
	testnetWS = "wss://api.hyperliquid-testnet.xyz/ws"
)

var (
	ticksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "market",
		Name: "ticks_total", Help: "Market ticks republished onto NATS, by kind (perp|spot|funding).",
	}, []string{"kind"})
	lastPrice = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "market",
		Name: "last_price", Help: "Most recent price seen, by instrument.",
	}, []string{"instrument"})
	fundingRate = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "market",
		Name: "funding_rate", Help: "Most recent predicted funding rate (fraction).",
	})
	wsReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "market",
		Name: "ws_reconnects_total", Help: "WebSocket reconnects across all feeds.",
	})
)

type config struct {
	NATSURL     string
	WSURL       string
	Mainnet     bool   // selects default WS/REST host; matches order-service's HL_MAINNET
	RESTURL     string // /info host for metadata; "" → testnet
	Symbol      string
	MetricsAddr string
}

func loadConfig() config {
	mainnet := getbool("HL_MAINNET", false)
	return config{
		NATSURL:     getenv("NATS_URL", nats.DefaultURL),
		WSURL:       getenv("HL_WS", defaultWSURL(mainnet)),
		Mainnet:     mainnet,
		RESTURL:     os.Getenv("HL_API"),
		Symbol:      getenv("SYMBOL", "BTCUSDT"),
		MetricsAddr: getenv("METRICS_ADDR", ":2112"),
	}
}

func defaultWSURL(mainnet bool) string {
	if mainnet {
		return mainnetWS
	}
	return testnetWS
}

type service struct {
	log *slog.Logger
	nc  *nats.Conn
	cfg config
	// ws dials the public WebSocket. Timeout 0 — a WebSocket outlives any
	// request clock.
	ws *http.Client
	// Resolved once at startup: the allMids key / activeAssetCtx coin for the perp,
	// and the allMids key for spot. spotKey is "" when the symbol has no listed
	// spot pair, in which case we run perp + funding only.
	coin    string
	spotKey string
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	// Root context is cancelled on SIGINT/SIGTERM so every goroutine unwinds
	// cleanly instead of being killed mid-publish.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		log.Error("connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()
	log.Info("connected to NATS", "url", cfg.NATSURL)

	metrics.Serve(ctx, cfg.MetricsAddr, log)

	// Hyperliquid keys spot mids by a numeric index, so resolve the instrument
	// keys from /info metadata before subscribing. An info-only client needs no
	// signing key. ctx-bound so a shutdown during startup cancels rather than hangs.
	hl, err := hyperliquid.New(hyperliquid.Config{BaseURL: cfg.RESTURL, Mainnet: cfg.Mainnet})
	if err != nil {
		log.Error("build hyperliquid client", "err", err)
		os.Exit(1)
	}
	if err := hl.LoadMeta(ctx); err != nil {
		log.Error("load hyperliquid meta", "err", err)
		os.Exit(1)
	}
	coin, spotKey, spotOK := hl.MarketKeys(cfg.Symbol)
	if !spotOK {
		log.Warn("no spot listing for symbol; running perp + funding only", "symbol", cfg.Symbol)
	}

	s := &service{log: log, nc: nc, cfg: cfg, ws: &http.Client{}, coin: coin, spotKey: spotKey}

	// One WebSocket carries both feeds (Hyperliquid multiplexes channels), so a
	// single reconnect loop owns the connection and unwinds with the root context.
	if err := s.run(ctx); err != nil && ctx.Err() == nil {
		log.Error("stream failed", "err", err)
	}
	log.Info("shutdown complete")
}

// run keeps the single WebSocket alive, reconnecting with a fixed delay until the
// context is cancelled.
func (s *service) run(ctx context.Context) error {
	for {
		err := s.streamOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err() // shutting down — not an error
		}
		wsReconnects.Inc()
		s.log.Warn("stream dropped, reconnecting", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

func (s *service) streamOnce(ctx context.Context) error {
	c, _, err := websocket.Dial(ctx, s.cfg.WSURL, &websocket.DialOptions{HTTPClient: s.ws})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	c.SetReadLimit(1 << 20)

	// Subscribe to every feed on this one socket: allMids for prices (all
	// instruments) and activeAssetCtx for the perp's funding rate. Re-sent on each
	// (re)connect so a reconnect resubscribes.
	subs := []map[string]any{
		{"method": "subscribe", "subscription": map[string]any{"type": "allMids"}},
		{"method": "subscribe", "subscription": map[string]any{"type": "activeAssetCtx", "coin": s.coin}},
	}
	for _, sub := range subs {
		if err := wsjson.Write(ctx, c, sub); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
	}
	s.log.Info("subscribed", "url", s.cfg.WSURL, "coin", s.coin, "spot", s.spotKey)

	// Ping on a child context so the ping goroutine stops as soon as this
	// connection does.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.pingLoop(connCtx, c)

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := s.dispatch(data); err != nil {
			s.log.Warn("handle message", "err", err)
		}
	}
}

func (s *service) pingLoop(ctx context.Context, c *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := wsjson.Write(ctx, c, map[string]any{"method": "ping"}); err != nil {
				return // connection is going away; the read loop will surface it
			}
		}
	}
}

// wsEnvelope is the outer frame every Hyperliquid WS message shares: a channel
// name and a channel-specific data payload, left raw until the channel is known.
type wsEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// dispatch routes a raw WS frame to its channel handler. Non-data frames
// (subscriptionResponse, pong) carry nothing to publish and fall through.
func (s *service) dispatch(raw []byte) error {
	var env wsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}
	switch env.Channel {
	case "allMids":
		return s.handleMids(env.Data)
	case "activeAssetCtx":
		return s.handleAssetCtx(env.Data)
	default:
		return nil
	}
}

// midsData is the allMids payload: instrument key → mid price. Hyperliquid sends
// prices as decimal strings. Perps are keyed by coin ("BTC"), spot by "@<index>".
type midsData struct {
	Mids map[string]string `json:"mids"`
}

func (s *service) handleMids(raw json.RawMessage) error {
	var d midsData
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("unmarshal mids: %w", err)
	}
	now := time.Now().UTC()
	if px, ok := d.Mids[s.coin]; ok {
		s.publishPrice(events.SubjPricePerp, "perp", px, now)
	}
	if s.spotKey != "" {
		if px, ok := d.Mids[s.spotKey]; ok {
			s.publishPrice(events.SubjPriceSpot, "spot", px, now)
		}
	}
	return nil
}

func (s *service) publishPrice(subj, instrument, raw string, now time.Time) {
	price, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		s.log.Warn("parse price", "instrument", instrument, "value", raw, "err", err)
		return
	}
	s.publish(subj, events.Price{Symbol: s.cfg.Symbol, Price: price, Time: now})
	ticksTotal.WithLabelValues(instrument).Inc()
	lastPrice.WithLabelValues(instrument).Set(price)
}

// assetCtxData is the activeAssetCtx payload. We take only the funding rate; the
// ctx also carries mark/oracle/mid prices we already get from allMids.
type assetCtxData struct {
	Coin string `json:"coin"`
	Ctx  struct {
		Funding string `json:"funding"` // funding rate, decimal-string fraction
	} `json:"ctx"`
}

func (s *service) handleAssetCtx(raw json.RawMessage) error {
	var d assetCtxData
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("unmarshal assetCtx: %w", err)
	}
	if d.Ctx.Funding == "" {
		return nil
	}
	rate, err := strconv.ParseFloat(d.Ctx.Funding, 64)
	if err != nil {
		return fmt.Errorf("funding %q: %w", d.Ctx.Funding, err)
	}
	// Hyperliquid funds hourly (Bybit settled every 8h); the rate is per-hour. We
	// pass it through as-is — the entry/exit thresholds it's compared against are a
	// strategy-side concern. NextSettleAt is left unset: the ctx carries no next
	// settlement time, and strategy keys off the rate alone.
	s.publish(events.SubjFundingPredicted, events.FundingRate{
		Symbol: s.cfg.Symbol, Rate: rate, Time: time.Now().UTC(),
	})
	ticksTotal.WithLabelValues("funding").Inc()
	fundingRate.Set(rate)
	return nil
}

func (s *service) publish(subject string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		s.log.Error("marshal", "subject", subject, "err", err)
		return
	}
	if err := s.nc.Publish(subject, data); err != nil {
		s.log.Error("publish", "subject", subject, "err", err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getbool parses a bool env (1/true/yes/on and their negatives, case-insensitive);
// a missing or malformed value falls back to def. HL_MAINNET must match the REST
// host, so the safe default is testnet (false).
func getbool(key string, def bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}
