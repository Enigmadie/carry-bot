// Command strategy listens to market-data ticks on core NATS and decides when
// to open or close the delta-neutral position. Decisions ("intents") are
// published to JetStream so they are durable: a lost "close" signal is real
// money. This is the first service that uses JetStream rather than core NATS.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

const (
	streamName   = "STRATEGY"
	streamMaxAge = 72 * time.Hour
)

type config struct {
	NATSURL        string
	Symbol         string
	EntryThreshold float64 // open when funding >= this (fraction: 0.0001 = 0.01%)
	ExitThreshold  float64 // close when funding <= this; keep ExitThreshold < EntryThreshold
}

func loadConfig() config {
	return config{
		NATSURL:        getenv("NATS_URL", nats.DefaultURL),
		Symbol:         getenv("SYMBOL", "BTCUSDT"),
		EntryThreshold: getfloat("ENTRY_THRESHOLD", 0.0001),
		ExitThreshold:  getfloat("EXIT_THRESHOLD", 0.00005),
	}
}

// position is the in-memory state machine. v1 keeps it in memory only: a
// restart forgets whether we hold a position. That is acceptable here because
// reconciliation against the exchange at startup is a separate, later step —
// the exchange is the source of truth, not this process.
type position int

const (
	flat position = iota
	open
)

// tick is one normalised market update fanned in from the three subscriptions.
// Funneling every event through a single channel lets one goroutine own all
// mutable state, so we never need a mutex. The Go idiom: don't communicate by
// sharing memory; share memory by communicating.
type tick struct {
	kind    string // "perp" | "spot" | "funding"
	price   float64
	funding events.FundingRate
}

type service struct {
	log *slog.Logger
	js  jetstream.JetStream
	cfg config

	// State below is touched only by run()'s goroutine.
	state     position
	spotPrice float64
	perpPrice float64
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		log.Error("connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()
	log.Info("connected to NATS", "url", cfg.NATSURL)

	// jetstream.New is the current API (the old nc.JetStream() is legacy).
	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream init", "err", err)
		os.Exit(1)
	}

	s := &service{log: log, js: js, cfg: cfg}

	// CreateOrUpdateStream is idempotent: safe to call on every startup. The
	// stream durably stores every intent under strategy.intent.*. LimitsPolicy
	// (vs WorkQueue) keeps messages for MaxAge so several consumers — order- and
	// portfolio-service — can each read and replay them independently.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"strategy.intent.*"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    streamMaxAge,
	}); err != nil {
		log.Error("ensure stream", "stream", streamName, "err", err)
		os.Exit(1)
	}
	log.Info("stream ready", "stream", streamName)

	if err := s.run(ctx, nc); err != nil && ctx.Err() == nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

// run wires up the three subscriptions and owns all mutable state in its select
// loop. Subscription callbacks run on their own goroutines, so they only parse
// and forward onto ticks; nothing outside this goroutine mutates state.
func (s *service) run(ctx context.Context, nc *nats.Conn) error {
	ticks := make(chan tick, 64)

	subs := []struct {
		subj  string
		parse func([]byte) (tick, bool)
	}{
		{events.SubjPricePerp, parsePrice("perp")},
		{events.SubjPriceSpot, parsePrice("spot")},
		{events.SubjFundingPredicted, parseFunding},
	}
	for _, sub := range subs {
		parse := sub.parse
		natsSub, err := nc.Subscribe(sub.subj, func(m *nats.Msg) {
			t, ok := parse(m.Data)
			if !ok {
				return
			}
			// Drop ticks rather than block the NATS callback if we ever fall
			// behind; prices are latest-value-wins and funding repeats.
			select {
			case ticks <- t:
			default:
				s.log.Warn("ticks buffer full, dropping", "subj", m.Subject)
			}
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", sub.subj, err)
		}
		defer natsSub.Unsubscribe()
	}
	s.log.Info("subscribed to market data",
		"entry", s.cfg.EntryThreshold, "exit", s.cfg.ExitThreshold)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-ticks:
			s.onTick(ctx, t)
		}
	}
}

func (s *service) onTick(ctx context.Context, t tick) {
	switch t.kind {
	case "perp":
		s.perpPrice = t.price
	case "spot":
		s.spotPrice = t.price
	case "funding":
		s.evaluate(ctx, t.funding)
	}
}

// evaluate is the entry/exit state machine. Hysteresis (Exit < Entry) stops the
// position from flapping when funding wobbles around a single threshold.
func (s *service) evaluate(ctx context.Context, fr events.FundingRate) {
	switch s.state {
	case flat:
		if fr.Rate >= s.cfg.EntryThreshold {
			reason := fmt.Sprintf("funding %.4f%% >= %.4f%%",
				fr.Rate*100, s.cfg.EntryThreshold*100)
			s.emit(ctx, events.SubjIntentOpen, events.IntentOpen, reason, fr)
		}
	case open:
		if fr.Rate <= s.cfg.ExitThreshold {
			reason := fmt.Sprintf("funding %.4f%% <= %.4f%%",
				fr.Rate*100, s.cfg.ExitThreshold*100)
			s.emit(ctx, events.SubjIntentClose, events.IntentClose, reason, fr)
		}
	}
}

// emit publishes an intent to JetStream and only flips the state machine once
// the server acks the write. js.Publish is synchronous: unlike core NATS'
// fire-and-forget Publish (used in market-data), it waits for the server to
// persist the message. If the ack fails we keep the old state and retry on the
// next funding tick — better to re-emit than to silently lose a close signal.
// WithMsgID sets the JetStream dedup key, so a retried publish of the same
// intent ID is collapsed server-side within the dedup window.
func (s *service) emit(ctx context.Context, subj, side, reason string, fr events.FundingRate) {
	intent := events.Intent{
		ID:        nuid.Next(),
		Symbol:    s.cfg.Symbol,
		Side:      side,
		Reason:    reason,
		Funding:   fr.Rate,
		PerpPrice: s.perpPrice,
		SpotPrice: s.spotPrice,
		Time:      time.Now().UTC(),
	}
	data, err := json.Marshal(intent)
	if err != nil {
		s.log.Error("marshal intent", "err", err)
		return
	}

	ack, err := s.js.Publish(ctx, subj, data, jetstream.WithMsgID(intent.ID))
	if err != nil {
		s.log.Error("publish intent", "side", side, "err", err)
		return // state unchanged → retried on the next funding tick
	}

	if side == events.IntentOpen {
		s.state = open
	} else {
		s.state = flat
	}
	s.log.Info("intent published",
		"side", side, "reason", reason, "id", intent.ID,
		"stream", ack.Stream, "seq", ack.Sequence)
}

// parsePrice returns a parser that tags decoded Price ticks with kind.
func parsePrice(kind string) func([]byte) (tick, bool) {
	return func(data []byte) (tick, bool) {
		var p events.Price
		if err := json.Unmarshal(data, &p); err != nil {
			return tick{}, false
		}
		return tick{kind: kind, price: p.Price}, true
	}
}

func parseFunding(data []byte) (tick, bool) {
	var fr events.FundingRate
	if err := json.Unmarshal(data, &fr); err != nil {
		return tick{}, false
	}
	return tick{kind: "funding", funding: fr}, true
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getfloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
