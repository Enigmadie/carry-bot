// Command market-data connects to Bybit's public WebSocket and republishes
// perp prices and funding rates onto NATS for the rest of the system.
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
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/nats-io/nats.go"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

const (
	pingInterval   = 15 * time.Second
	reconnectDelay = 3 * time.Second
)

type config struct {
	NATSURL     string
	LinearWSURL string
	SpotWSURL   string
	Symbol      string
}

func loadConfig() config {
	return config{
		NATSURL:     getenv("NATS_URL", nats.DefaultURL),
		LinearWSURL: getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream-testnet.bybit.com/v5/public/linear"),
		SpotWSURL:   getenv("BYBIT_WS_PUBLIC_SPOT", "wss://stream-testnet.bybit.com/v5/public/spot"),
		Symbol:      getenv("SYMBOL", "BTCUSDT"),
	}
}

type service struct {
	log *slog.Logger
	nc  *nats.Conn
	cfg config
	// ws dials the public WebSocket. Timeout 0 — a WebSocket outlives any
	// request clock.
	ws *http.Client
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

	s := &service{log: log, nc: nc, cfg: cfg, ws: &http.Client{}}

	// Two independent feeds, one WebSocket each. The linear (perp) tickers
	// stream carries the perp price and the predicted funding rate; the spot
	// tickers stream carries only the spot price. Both reconnect on their own
	// and unwind together when the root context is cancelled.
	topic := "tickers." + cfg.Symbol
	feeds := []struct {
		url    string
		handle func([]byte) error
	}{
		{cfg.LinearWSURL, s.handlePerpTicker},
		{cfg.SpotWSURL, s.handleSpotTicker},
	}

	var wg sync.WaitGroup
	for _, f := range feeds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// runStream only returns once the context is cancelled, so any
			// non-context error here is genuinely fatal for this feed.
			if err := s.runStream(ctx, f.url, topic, f.handle); err != nil && ctx.Err() == nil {
				log.Error("stream failed", "url", f.url, "err", err)
				stop() // bring the whole service down rather than run half-blind
			}
		}()
	}
	wg.Wait()
	log.Info("shutdown complete")
}

// runStream keeps a single WebSocket subscription alive, reconnecting with a
// fixed delay until the context is cancelled.
func (s *service) runStream(ctx context.Context, url, topic string, handle func([]byte) error) error {
	for {
		err := s.streamOnce(ctx, url, topic, handle)
		if ctx.Err() != nil {
			return ctx.Err() // shutting down — not an error
		}
		s.log.Warn("stream dropped, reconnecting", "topic", topic, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

func (s *service) streamOnce(ctx context.Context, url, topic string, handle func([]byte) error) error {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: s.ws})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	c.SetReadLimit(1 << 20)

	if err := wsjson.Write(ctx, c, map[string]any{"op": "subscribe", "args": []string{topic}}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.log.Info("subscribed", "url", url, "topic", topic)

	// Ping on a child context so the ping goroutine stops as soon as this
	// connection does. Bybit drops the socket without a ping every ~20s.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.pingLoop(connCtx, c)

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := handle(data); err != nil {
			s.log.Warn("handle message", "topic", topic, "err", err)
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
			if err := wsjson.Write(ctx, c, map[string]any{"op": "ping"}); err != nil {
				return // connection is going away; the read loop will surface it
			}
		}
	}
}

// tickerMessage is the slice of Bybit's tickers payload we care about. Bybit
// sends numbers as strings, and after the first snapshot only changed fields
// are present, so every field is optional.
type tickerMessage struct {
	Topic string `json:"topic"`
	Data  struct {
		Symbol          string `json:"symbol"`
		LastPrice       string `json:"lastPrice"`
		FundingRate     string `json:"fundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
	} `json:"data"`
}

func (s *service) handlePerpTicker(raw []byte) error {
	return s.handleTicker(raw, events.SubjPricePerp, true)
}

func (s *service) handleSpotTicker(raw []byte) error {
	return s.handleTicker(raw, events.SubjPriceSpot, false)
}

// handleTicker parses a Bybit tickers payload and republishes the price to
// priceSubj. Only the perp feed carries a funding rate, so withFunding gates
// the funding publish; spot tickers never include one.
func (s *service) handleTicker(raw []byte, priceSubj string, withFunding bool) error {
	var msg tickerMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if msg.Topic == "" {
		return nil // subscribe ack / pong — nothing to publish
	}

	symbol := msg.Data.Symbol
	if symbol == "" {
		symbol = s.cfg.Symbol
	}
	now := time.Now().UTC()

	if msg.Data.LastPrice != "" {
		price, err := strconv.ParseFloat(msg.Data.LastPrice, 64)
		if err != nil {
			return fmt.Errorf("lastPrice %q: %w", msg.Data.LastPrice, err)
		}
		s.publish(priceSubj, events.Price{Symbol: symbol, Price: price, Time: now})
	}

	if withFunding && msg.Data.FundingRate != "" {
		rate, err := strconv.ParseFloat(msg.Data.FundingRate, 64)
		if err != nil {
			return fmt.Errorf("fundingRate %q: %w", msg.Data.FundingRate, err)
		}
		fr := events.FundingRate{Symbol: symbol, Rate: rate, Time: now}
		if ms, err := strconv.ParseInt(msg.Data.NextFundingTime, 10, 64); err == nil && ms > 0 {
			fr.NextSettleAt = time.UnixMilli(ms).UTC()
		}
		s.publish(events.SubjFundingPredicted, fr)
	}
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
