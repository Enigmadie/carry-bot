// Command strategy listens to market-data ticks on core NATS and decides when
// to open or close the delta-neutral position. Decisions ("intents") are
// published to JetStream so they are durable: a lost "close" signal is real
// money.
//
// The position state machine is driven by execution facts, not by our own
// intents: publishing an intent moves the state to pending, and only the
// exec.position.opened/closed fact from order-service confirms the flip (an
// exec.failed reverts it). A durable consumer on the EXEC stream replays those
// facts across restarts, and order-service's startup exec.reconciled snapshot
// anchors them to the exchange's live position — so strategy never believes in
// a position the exchange doesn't hold, which is exactly how the first live
// run desynced (intent acked, legs failed, state said open).
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/metrics"
)

const (
	streamName   = "STRATEGY"
	execStream   = "EXEC" // produced by order-service; we consume the facts
	streamMaxAge = 72 * time.Hour
)

var (
	intentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "strategy",
		Name: "intents_total", Help: "Intents published to JetStream, by side (open|close).",
	}, []string{"side"})
	stateGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "strategy",
		Name: "state", Help: "Position state machine: 0 flat, 1 open, 2 pending open, 3 pending close, -1 halted.",
	})
	publishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "strategy",
		Name: "publish_errors_total", Help: "Intent publishes that failed their JetStream ack.",
	})
)

type config struct {
	NATSURL        string
	Symbol         string
	EntryThreshold float64 // open when funding >= this (fraction: 0.0001 = 0.01%)
	ExitThreshold  float64 // close when funding <= this; keep ExitThreshold < EntryThreshold
	MetricsAddr    string
}

func loadConfig() config {
	return config{
		NATSURL:        getenv("NATS_URL", nats.DefaultURL),
		Symbol:         getenv("SYMBOL", "BTCUSDT"),
		EntryThreshold: getfloat("ENTRY_THRESHOLD", 0.0001),
		ExitThreshold:  getfloat("EXIT_THRESHOLD", 0.00005),
		MetricsAddr:    getenv("METRICS_ADDR", ":2113"),
	}
}

// position is the in-memory state machine, rebuilt on every start by replaying
// the durable EXEC stream (facts + order-service's reconcile snapshot). The
// pending states cover the window between publishing an intent and hearing the
// execution fact back: while pending, evaluate() stays quiet, so a funding tick
// can't fire a second intent at an in-flight one. halted mirrors an unbalanced
// reconcile — the exchange position needs a human, so no intents at all.
type position int

const (
	flat position = iota
	open
	pendingOpen
	pendingClose
	halted
)

func (p position) String() string {
	switch p {
	case flat:
		return "flat"
	case open:
		return "open"
	case pendingOpen:
		return "pending-open"
	case pendingClose:
		return "pending-close"
	case halted:
		return "halted"
	}
	return "unknown"
}

// gauge values match the stateGauge help text.
var stateGaugeValue = map[position]float64{
	flat: 0, open: 1, pendingOpen: 2, pendingClose: 3, halted: -1,
}

func (s *service) setState(p position, why string) {
	if s.state == p {
		return
	}
	s.log.Info("state", "from", s.state, "to", p, "why", why)
	s.state = p
	stateGauge.Set(stateGaugeValue[p])
}

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

	// Seeding: on startup the EXEC replay walks the state machine to the current
	// truth; until it has caught up, evaluate() must not emit — a funding tick
	// racing the replay would fire an intent from a stale (default flat) state.
	seeded      bool
	seedPending uint64 // facts the replay still owes us at startup
	seedSeen    uint64
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

	metrics.Serve(ctx, cfg.MetricsAddr, log)

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

// run wires up the three market-data subscriptions plus the EXEC fact consumer
// and owns all mutable state in its select loop. Subscription and consumer
// callbacks run on their own goroutines, so they only parse and forward onto
// channels; nothing outside this goroutine mutates state.
func (s *service) run(ctx context.Context, nc *nats.Conn) error {
	// EXEC is owned by order-service; ensure it here too (same config) so
	// strategy can start independently of start order, like portfolio does.
	if _, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      execStream,
		Subjects:  []string{"exec.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    streamMaxAge,
	}); err != nil {
		return fmt.Errorf("ensure exec stream: %w", err)
	}

	// Ephemeral, deliver-all: every start replays the stream's retained facts
	// from the beginning, walking the state machine to the current truth. A
	// durable would be wrong here — its cursor sits past the already-consumed
	// history, so a restart would learn nothing and boot as flat. Transitions
	// are idempotent, so re-applying old facts is free; AckNone because we keep
	// no cursor to advance. If the whole window aged out (MaxAge) while a
	// position is held, we still boot flat — order-service's stale-intent guard
	// answers the resulting intent with a fresh reconcile snapshot (see its
	// open/closePosition), so the machine self-corrects before any trade.
	cons, err := s.js.CreateOrUpdateConsumer(ctx, execStream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubjects: []string{
			events.SubjPositionOpened, events.SubjPositionClosed,
			events.SubjExecFailed, events.SubjReconciled,
		},
	})
	if err != nil {
		return fmt.Errorf("ensure exec consumer: %w", err)
	}

	// How much history the replay owes us; evaluate() stays quiet until the
	// backlog has been applied (see onExec), so a live funding tick can't race
	// the replay and fire an intent off a stale state.
	info, err := cons.Info(ctx)
	if err != nil {
		return fmt.Errorf("exec consumer info: %w", err)
	}
	s.seedPending = info.NumPending
	if s.seedPending == 0 {
		s.seeded = true
	}
	s.log.Info("seeding state from exec history", "facts", s.seedPending)

	execs := make(chan jetstream.Msg, 16)
	cc, err := cons.Consume(func(m jetstream.Msg) { execs <- m })
	if err != nil {
		return fmt.Errorf("start exec consume: %w", err)
	}
	defer cc.Stop()

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
		case m := <-execs:
			s.onExec(m)
		}
	}
}

// onExec applies one execution fact to the state machine. Facts are truth, so
// opened/closed apply from any state (a replayed history walks the machine
// forward); a failure only reverts the pending state it belongs to; a reconcile
// snapshot overrides everything — it is the exchange's own word. No ack
// bookkeeping: the consumer is AckNone, a decode error just skips the fact.
func (s *service) onExec(m jetstream.Msg) {
	defer s.countSeed()
	switch m.Subject() {
	case events.SubjReconciled:
		var r events.Reconciled
		if err := json.Unmarshal(m.Data(), &r); err != nil {
			s.log.Error("unmarshal reconciled", "err", err)
			return
		}
		switch r.Verdict {
		case events.ReconcileOpen:
			s.setState(open, "reconciled")
		case events.ReconcileFlat:
			s.setState(flat, "reconciled")
		default:
			s.log.Error("exchange unbalanced at reconcile — halting strategy",
				"perp", r.PerpSize, "spot", r.SpotSize)
			s.setState(halted, "reconciled unbalanced")
		}
	case events.SubjPositionOpened, events.SubjPositionClosed, events.SubjExecFailed:
		var r events.ExecReport
		if err := json.Unmarshal(m.Data(), &r); err != nil {
			s.log.Error("unmarshal exec report", "err", err)
			return
		}
		switch m.Subject() {
		case events.SubjPositionOpened:
			s.setState(open, "exec opened "+r.IntentID)
		case events.SubjPositionClosed:
			s.setState(flat, "exec closed "+r.IntentID)
		case events.SubjExecFailed:
			if s.state == pendingOpen && r.Side == events.IntentOpen {
				s.setState(flat, "open failed "+r.IntentID)
			} else if s.state == pendingClose && r.Side == events.IntentClose {
				s.setState(open, "close failed "+r.IntentID)
			}
		}
	}
}

// countSeed marks the startup backlog as applied fact by fact; once the replay
// has caught up, evaluate() is unmuted.
func (s *service) countSeed() {
	if s.seeded {
		return
	}
	s.seedSeen++
	if s.seedSeen >= s.seedPending {
		s.seeded = true
		s.log.Info("state seeded from exec history", "facts", s.seedSeen, "state", s.state)
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

// evaluate is the entry/exit trigger. Hysteresis (Exit < Entry) stops the
// position from flapping when funding wobbles around a single threshold. Only
// the two confirmed states act; pending waits for the execution fact and
// halted waits for a human.
func (s *service) evaluate(ctx context.Context, fr events.FundingRate) {
	if !s.seeded {
		return // replay still walking the state machine to the current truth
	}
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

// emit publishes an intent to JetStream and moves the state machine to the
// matching pending state once the server acks the write — the confirmed flip
// only happens when the execution fact comes back (see onExec). js.Publish is
// synchronous: unlike core NATS' fire-and-forget Publish (used in market-data),
// it waits for the server to persist the message. If the ack fails we keep the
// old state and retry on the next funding tick — better to re-emit than to
// silently lose a close signal. WithMsgID sets the JetStream dedup key, so a
// retried publish of the same intent ID is collapsed server-side within the
// dedup window.
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
		publishErrors.Inc()
		s.log.Error("publish intent", "side", side, "err", err)
		return // state unchanged → retried on the next funding tick
	}

	if side == events.IntentOpen {
		s.setState(pendingOpen, "intent published "+intent.ID)
	} else {
		s.setState(pendingClose, "intent published "+intent.ID)
	}
	intentsTotal.WithLabelValues(side).Inc()
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
