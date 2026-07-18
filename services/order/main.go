// Command order executes strategy intents against Bybit. It is the only service
// that touches real money, so two properties dominate its design:
//
//   - Idempotency. JetStream is at-least-once: the same intent can arrive twice
//     (redelivery after a crash, or a missed ack). We derive a deterministic
//     orderLinkId from the intent ID for each leg, and Bybit refuses a second
//     order with an id it has already seen — so a replay is a safe no-op rather
//     than a doubled position.
//
//   - Leg risk. The spot long and the perp short cannot open atomically. We open
//     them in sequence; if the second leg fails we roll the first one back so we
//     never sit on a naked, directional position. A failure we cannot undo
//     cleanly (a half-closed position) is reported and that intent is halted
//     instead of retried, since clearing it needs manual intervention —
//     automated reconciliation against the exchange is a later step.
//
// Like strategy, all mutable handling runs in a single worker goroutine, so the
// leg orchestration never races with itself across concurrent redeliveries.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Enigmadie/carry-bot/pkg/bybit"
	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/exchange"
	"github.com/Enigmadie/carry-bot/pkg/hyperliquid"
	"github.com/Enigmadie/carry-bot/pkg/metrics"
	"github.com/Enigmadie/carry-bot/pkg/mock"
)

const (
	intentStream = "STRATEGY" // produced by strategy-service; we consume it
	execStream   = "EXEC"     // we produce it; portfolio-service will consume it
	durableName  = "order-service"
	execMaxAge   = 72 * time.Hour
	ackWait      = 30 * time.Second
	maxDeliver   = 5
)

// errRetry marks a failure that warrants JetStream redelivery (a transport
// blip before anything was placed). Terminal outcomes — a rolled-back or
// halted intent — return nil after emitting an exec fact, so we ack and move
// on rather than thrash the same doomed order.
var errRetry = errors.New("retry")

var (
	placeSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "place_seconds", Help: "Latency of a single PlaceOrder call to the exchange.",
		Buckets: prometheus.DefBuckets,
	})
	legsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "legs_total", Help: "Order legs by category, side, and result (placed|duplicate|failed).",
	}, []string{"category", "side", "result"})
	intentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "intents_total", Help: "Intents settled, by side and outcome (opened|closed|failed).",
	}, []string{"side", "outcome"})
	rollbacksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "rollbacks_total", Help: "Spot legs rolled back after a failed perp leg.",
	})
	alertsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "alerts_total", Help: "Unrecoverable outcomes needing manual intervention.",
	})
	fundingReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "funding_received_total", Help: "Funding settlements emitted to the EXEC stream.",
	})
	haltedGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "halted", Help: "1 when trading is halted (unbalanced position at reconcile); manual intervention required.",
	})
	reconcileDriftTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "reconcile_drift_total", Help: "Runtime reconcile ticks where the exchange disagreed with internal state, by verdict.",
	}, []string{"verdict"})
	orphanClosesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "orphan_closes_total", Help: "Orphaned spot legs auto-closed after the exchange force-closed the perp.",
	})
	userWSReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "user_ws_reconnects_total", Help: "User-fills WebSocket reconnects.",
	})
	userWSNudges = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "order",
		Name: "user_ws_nudges_total", Help: "Reconciles triggered by a user fill the bot did not place.",
	})
)

type config struct {
	NATSURL  string
	Symbol   string
	Provider string // EXCHANGE: mock | bybit | hyperliquid
	OrderQty string // base-coin amount per leg; intents carry no size

	FundingPoll     time.Duration // how often to poll the exchange for funding; 0 disables
	ReconcilePoll   time.Duration // how often to re-check exchange state at runtime; 0 disables
	OrphanAutoClose bool          // sell an orphaned spot leg (perp force-closed by exchange) instead of halting

	BybitREST    string
	APIKey       string
	APISecret    string
	MockFailLeg  string        // mock only: category whose leg fails, for rollback testing
	MockADLAfter time.Duration // mock only: force-close the perp this long after open (ADL simulation)

	HLAPI        string  // hyperliquid REST host; "" → testnet
	HLWS         string  // hyperliquid WS host for user fills; "" → follows HLMainnet
	HLMainnet    bool    // hyperliquid: phantom-agent source; must match HLAPI
	HLPrivateKey string  // hyperliquid agent-wallet signing key
	HLVault      string  // hyperliquid: optional vault/sub-account address
	HLAccount    string  // hyperliquid: master account for funding queries
	HLSlippage   float64 // hyperliquid: IOC price offset past mid; 0 → client default (5%)
	HLLeverage   int     // hyperliquid: perp leverage set at startup; 0 → skip
	HLCross      bool    // hyperliquid: cross margin (true) vs isolated; some assets are isolated-only

	MetricsAddr string
}

func loadConfig() config {
	return config{
		NATSURL:  getenv("NATS_URL", nats.DefaultURL),
		Symbol:   getenv("SYMBOL", "BTCUSDT"),
		Provider: getenv("EXCHANGE", "mock"),
		OrderQty: getenv("ORDER_QTY", "0.001"),

		FundingPoll:     getdur("FUNDING_POLL", 30*time.Second),
		ReconcilePoll:   getdur("RECONCILE_POLL", time.Minute),
		OrphanAutoClose: getbool("ORPHAN_AUTO_CLOSE", true),

		BybitREST:    getenv("BYBIT_REST", bybit.TestnetREST),
		APIKey:       os.Getenv("BYBIT_API_KEY"),
		APISecret:    os.Getenv("BYBIT_API_SECRET"),
		MockFailLeg:  os.Getenv("MOCK_FAIL_LEG"),
		MockADLAfter: getdur("MOCK_ADL_AFTER", 0),

		HLAPI:        os.Getenv("HL_API"),
		HLWS:         os.Getenv("HL_WS"),
		HLMainnet:    getbool("HL_MAINNET", false),
		HLPrivateKey: os.Getenv("HL_PRIVATE_KEY"),
		HLVault:      os.Getenv("HL_VAULT"),
		HLAccount:    os.Getenv("HL_ACCOUNT"),
		HLSlippage:   getfloat("HL_SLIPPAGE", 0),
		HLLeverage:   getint("HL_LEVERAGE", 3),
		HLCross:      getbool("HL_CROSS", true),

		MetricsAddr: getenv("METRICS_ADDR", ":2114"),
	}
}

type service struct {
	log *slog.Logger
	js  jetstream.JetStream
	ex  exchange.Exchange
	cfg config

	// halted stops all trading until the exchange position is clean again: set at
	// startup reconciliation (unbalanced snapshot) or at runtime by alert() (a
	// failed rollback / half-closed pair). Every intent is dropped while set. The
	// halt lifts on a restart that reconciles clean, or when a halted tick
	// observes the account provably flat (see haltedTick). Written and read
	// solely on the intent worker goroutine (startup runs before the consumer
	// starts).
	halted bool

	// haltedShape is the last exchange shape published while halted, so halted
	// ticks report each *change* of the position exactly once instead of either
	// going blind or re-alerting the same unbalanced state every tick.
	haltedShape string

	// positionOpen mirrors whether the pair is currently held. Written by the
	// intent worker (reconcile/opened/closed), read by the funding poller on its
	// own goroutine — hence atomic. It gates funding emission so the ledger
	// doesn't accumulate unattached payments while flat.
	positionOpen atomic.Bool

	// nudge carries the dir of a user fill the bot did not place (ADL,
	// liquidation, a manual order), sent by the user-fills WS watcher
	// (userfills.go) to trigger an immediate reconcile on the worker loop.
	// Buffered 1 + non-blocking send: coalescing bursts is fine, one reconcile
	// covers them all.
	nudge chan string
}

// buildExchange selects the provider from config. mock-first: the default needs no
// keys or network, so `make order` runs locally; bybit and hyperliquid are opt-in
// via EXCHANGE. ctx is threaded in only for hyperliquid, which must reach the
// exchange at startup to resolve its numeric asset ids (see the case below).
func buildExchange(ctx context.Context, cfg config) (exchange.Exchange, error) {
	switch cfg.Provider {
	case "mock":
		return mock.New(cfg.MockFailLeg, cfg.MockADLAfter), nil
	case "bybit":
		if cfg.APIKey == "" || cfg.APISecret == "" {
			return nil, errors.New("EXCHANGE=bybit requires BYBIT_API_KEY and BYBIT_API_SECRET")
		}
		return bybit.New(cfg.BybitREST, cfg.APIKey, cfg.APISecret)
	case "hyperliquid":
		if cfg.HLPrivateKey == "" {
			return nil, errors.New("EXCHANGE=hyperliquid requires HL_PRIVATE_KEY")
		}
		c, err := hyperliquid.New(hyperliquid.Config{
			BaseURL:    cfg.HLAPI,
			Mainnet:    cfg.HLMainnet,
			PrivateKey: cfg.HLPrivateKey,
			Vault:      cfg.HLVault,
			Account:    cfg.HLAccount,
			Slippage:   cfg.HLSlippage,
		})
		if err != nil {
			return nil, err
		}
		// Hyperliquid addresses instruments by a numeric asset id, not a symbol, so
		// the client can't place an order until it has loaded exchange metadata. The
		// Exchange interface has no such step, so we do it here, behind the provider
		// switch, before handing the client back ready to trade.
		if err := c.LoadMeta(ctx); err != nil {
			return nil, fmt.Errorf("load hyperliquid meta: %w", err)
		}
		// Hyperliquid opens a perp in isolated 10x by default, whose thin posted margin
		// can block the close (see leverage.go). Lowering the leverage posts more margin
		// (and cross, where allowed, lets the spot USDC back the short too) so the close
		// absorbs the slippage loss. Cross is preferred but some assets are isolated-only
		// (HYPE testnet rejects cross) — HL_CROSS=false selects isolated low-leverage,
		// which posts enough margin on its own. Persists on the account, so once at
		// startup is enough; HL_LEVERAGE=0 opts out (leave the account as-is).
		if cfg.HLLeverage > 0 {
			if err := c.UpdateLeverage(ctx, cfg.Symbol, cfg.HLCross, cfg.HLLeverage); err != nil {
				return nil, fmt.Errorf("set hyperliquid leverage: %w", err)
			}
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unknown EXCHANGE provider %q", cfg.Provider)
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Built under ctx because hyperliquid loads exchange metadata over the network
	// here; a shutdown signal during startup cancels it instead of hanging.
	ex, err := buildExchange(ctx, cfg)
	if err != nil {
		log.Error("build exchange", "err", err)
		os.Exit(1)
	}

	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		log.Error("connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()
	log.Info("connected to NATS", "url", cfg.NATSURL)

	metrics.Serve(ctx, cfg.MetricsAddr, log)

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream init", "err", err)
		os.Exit(1)
	}

	s := &service{
		log:   log,
		js:    js,
		ex:    ex,
		cfg:   cfg,
		nudge: make(chan string, 1),
	}

	if err := s.run(ctx); err != nil && ctx.Err() == nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

func (s *service) run(ctx context.Context) error {
	// The EXEC stream durably records what actually happened on the exchange,
	// independent of the STRATEGY stream of what we intended. Idempotent to
	// (re)declare on every startup, same as strategy does for STRATEGY.
	if _, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      execStream,
		Subjects:  []string{"exec.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    execMaxAge,
	}); err != nil {
		return fmt.Errorf("ensure exec stream: %w", err)
	}

	// Reconcile against the exchange before consuming a single intent: local
	// state is in-memory and a restart forgot it, so the exchange is the only
	// source of truth about what we hold. Failing here exits the service — the
	// restart policy retries, and trading blind is worse than not starting.
	if err := s.reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}

	// A durable, explicit-ack consumer. Durable means our read position survives
	// a restart, so we resume where we left off instead of replaying history.
	// Explicit ack means a message is redelivered until we ack it; MaxDeliver
	// caps that so a permanently poisonous message is eventually dropped rather
	// than looping forever.
	cons, err := s.js.CreateOrUpdateConsumer(ctx, intentStream, jetstream.ConsumerConfig{
		Durable:        durableName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{events.SubjIntentOpen, events.SubjIntentClose},
		AckWait:        ackWait,
		MaxDeliver:     maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}
	s.log.Info("consuming intents", "stream", intentStream, "durable", durableName,
		"exchange", s.cfg.Provider, "qty", s.cfg.OrderQty, "symbol", s.cfg.Symbol)

	// Consume delivers on its own goroutines and may run callbacks concurrently.
	// Funnel messages into one channel so a single worker processes intents
	// strictly in order — a single-position bot must never run two legs at once.
	msgs := make(chan jetstream.Msg, 16)
	cc, err := cons.Consume(func(m jetstream.Msg) { msgs <- m })
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}
	defer cc.Stop()

	// Funding is account state, not an intent, so it rides its own clock rather
	// than the intent consumer. It runs alongside the worker; both unwind on ctx.
	go s.pollFunding(ctx)

	// The fast lane of runtime reconciliation: user fills we didn't place nudge
	// an immediate reconcile instead of waiting out the poll interval. Only
	// hyperliquid has the feed; everywhere else the poll alone carries it.
	if hc, ok := s.ex.(*hyperliquid.Client); ok {
		go s.watchUserFills(ctx, userWSURL(s.cfg), hc.AccountAddress())
	}

	// The runtime reconcile ticks on the worker loop itself, not a goroutine:
	// it reads and writes the same state as intent handling (halted,
	// positionOpen) and must never observe the exchange mid-leg.
	var recTick <-chan time.Time
	if s.cfg.ReconcilePoll > 0 {
		t := time.NewTicker(s.cfg.ReconcilePoll)
		defer t.Stop()
		recTick = t.C
		s.log.Info("runtime reconcile enabled", "every", s.cfg.ReconcilePoll)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-msgs:
			s.handle(ctx, m)
		case <-recTick:
			s.reconcileTick(ctx)
		case dir := <-s.nudge:
			s.log.Warn("user fill outside our orders, reconciling now", "dir", dir)
			s.reconcileTick(ctx)
		}
	}
}

// Leg-size tolerances for reconciliation, as fractions of ORDER_QTY. A leg
// counts as held from 90% — the spot balance runs slightly under the ordered
// qty because Hyperliquid takes the spot fee in the received token — and as
// absent up to 10% (dust). Anything in between matches neither picture.
const (
	legFullFraction = 0.9
	legDustFraction = 0.1
)

// classifyState maps the exchange's live position onto a reconcile verdict.
// Anything that is neither cleanly flat nor cleanly both-legs-open — including
// a perp long (we only ever short) and resting orders (we trade IOC only) —
// is unbalanced and must halt trading for a human.
func classifyState(st *exchange.PositionState, qty float64) string {
	short := -st.PerpSize // held short comes back positive
	perpOpen := short >= qty*legFullFraction
	perpFlat := short <= qty*legDustFraction && st.PerpSize <= qty*legDustFraction
	spotOpen := st.SpotSize >= qty*legFullFraction
	spotFlat := st.SpotSize <= qty*legDustFraction

	switch {
	case st.OpenOrders > 0:
		return events.ReconcileUnbalanced
	case perpFlat && spotFlat:
		return events.ReconcileFlat
	case perpOpen && spotOpen:
		return events.ReconcileOpen
	default:
		return events.ReconcileUnbalanced
	}
}

// isOrphanSpot matches the one unbalanced shape the exchange can create
// without another actor: ADL/liquidation force-closed the perp short, the spot
// leg is intact within the same tolerances an open leg is judged by (a small
// upper margin covers accumulated fee dust), and nothing is resting. Every
// other unbalanced shape implies a human or a bug and stays on the halt path.
func isOrphanSpot(st *exchange.PositionState, qty float64) bool {
	return st.OpenOrders == 0 &&
		math.Abs(st.PerpSize) <= qty*legDustFraction &&
		st.SpotSize >= qty*legFullFraction &&
		st.SpotSize <= qty*(1+legDustFraction)
}

// observeState reads and classifies the live position. An unbalanced verdict
// that is exactly the orphaned-spot shape gets repaired in place (unless
// ORPHAN_AUTO_CLOSE opted out); the caller then sees the refreshed state, and
// a failed repair falls through as the original unbalanced verdict → halt.
func (s *service) observeState(ctx context.Context) (*exchange.PositionState, string, error) {
	st, err := s.ex.State(ctx, s.cfg.Symbol)
	if err != nil {
		return nil, "", err
	}
	verdict := classifyState(st, s.qty())
	if verdict == events.ReconcileUnbalanced && s.cfg.OrphanAutoClose && isOrphanSpot(st, s.qty()) {
		if fresh, ok := s.autoCloseOrphan(ctx, st); ok {
			st = fresh
			verdict = classifyState(st, s.qty())
		}
	}
	return st, verdict, nil
}

// autoCloseOrphan sells the orphaned spot leg to bring the account back to
// flat — the one trade the bot makes without an intent. Selling the held
// balance can only reduce exposure; the closed fact settles portfolio's ledger
// and walks strategy to flat. The exact balance is sold (the provider
// truncates it to the instrument's lot size), which also sweeps accumulated
// fee dust.
func (s *service) autoCloseOrphan(ctx context.Context, st *exchange.PositionState) (*exchange.PositionState, bool) {
	linkID := fmt.Sprintf("orphan-close-%d", time.Now().UTC().UnixNano())
	s.log.Warn("orphaned spot leg — perp force-closed by exchange, auto-closing",
		"spot", st.SpotSize, "link", linkID)
	res, err := s.placeLeg(ctx, events.Intent{ID: linkID}, exchange.OrderRequest{
		Category:    exchange.CategorySpot,
		Symbol:      s.cfg.Symbol,
		Side:        exchange.SideSell,
		Qty:         strconv.FormatFloat(st.SpotSize, 'f', -1, 64),
		OrderLinkID: linkID,
	})
	if err != nil {
		s.log.Error("orphan auto-close failed, falling back to halt", "err", err)
		return nil, false
	}
	orphanClosesTotal.Inc()
	s.emit(ctx, events.SubjPositionClosed, events.ExecReport{
		IntentID: linkID, Symbol: s.cfg.Symbol, Side: events.IntentClose, Qty: st.SpotSize,
		SpotOrderID: res.OrderID, SpotPrice: res.Price, Fee: res.Fee,
		Reason: "orphaned spot auto-closed: perp force-closed by exchange (ADL/liquidation)",
		Time:   time.Now().UTC(),
	})
	fresh, err := s.ex.State(ctx, s.cfg.Symbol)
	if err != nil {
		// Sold but can't confirm — report failure so the caller halts on the old
		// snapshot; the restart's clean reconcile will see the flat truth.
		s.log.Error("orphan auto-close: re-read state", "err", err)
		return nil, false
	}
	s.log.Info("orphaned spot auto-closed", "sold", st.SpotSize, "spot_left", fresh.SpotSize)
	return fresh, true
}

// reconcile reads the live position from the exchange, publishes the snapshot
// as a durable exec.reconciled fact (strategy seeds its state machine from it,
// portfolio checks its ledger against it), and arms the local flags: the
// funding gate on an open position, the halt on an unbalanced one.
func (s *service) reconcile(ctx context.Context) error {
	st, verdict, err := s.observeState(ctx)
	if err != nil {
		return err
	}
	if err := s.publishReconciled(ctx, st, verdict); err != nil {
		return err
	}
	s.applyVerdict(st, verdict)
	s.log.Info("reconciled against exchange", "verdict", verdict,
		"perp", st.PerpSize, "spot", st.SpotSize,
		"collateral", st.Collateral, "open_orders", st.OpenOrders)
	return nil
}

// reconcileTick is the runtime safety loop: the exchange can change our
// position without an order from us (ADL, liquidation), and the WS feed can
// silently miss it — polling catches any drift regardless of events. It only
// publishes on disagreement, so the EXEC stream doesn't fill with no-op
// snapshots. While halted the tick keeps watching read-only (haltedTick): the
// exchange doesn't stop moving just because we did.
func (s *service) reconcileTick(ctx context.Context) {
	if s.halted {
		s.haltedTick(ctx)
		return
	}
	st, verdict, err := s.observeState(ctx)
	if err != nil {
		// Transient by assumption: unlike startup we already hold trusted state,
		// so keep trading on it and let the next tick retry.
		s.log.Warn("runtime reconcile: read state", "err", err)
		return
	}
	want := events.ReconcileFlat
	if s.positionOpen.Load() {
		want = events.ReconcileOpen
	}
	if verdict == want {
		return
	}

	reconcileDriftTotal.WithLabelValues(verdict).Inc()
	s.log.Error("runtime reconcile: exchange disagrees with internal state",
		"internal", want, "verdict", verdict,
		"perp", st.PerpSize, "spot", st.SpotSize, "open_orders", st.OpenOrders)
	// Publish before applying: the snapshot is what walks strategy back to the
	// exchange's truth, so a failed publish must be retried — leaving local
	// state as-is keeps the drift visible to the next tick.
	if err := s.publishReconciled(ctx, st, verdict); err != nil {
		s.log.Error("runtime reconcile: publish", "err", err)
		return
	}
	s.applyVerdict(st, verdict)
}

// haltedTick keeps a halt observable instead of blind. The 2026-07-13 incident:
// a partial liquidation halted trading, then the exchange finished off the perp
// — and the halted bot neither saw it, nor reported it, nor repaired the
// resulting orphaned spot. So while halted the tick still observes the
// exchange, with two deliberate reactions and nothing else:
//
//   - Each *change* of the exchange's shape is published as exec.reconciled
//     (notification relays unbalanced ones), gated by haltedShape so a static
//     position doesn't re-alert every tick.
//   - observeState may repair an orphaned spot leg even now — selling it can
//     only reduce exposure — and a verdict that comes back flat lifts the halt:
//     a provably empty account is safe to trade from. This is the agreed
//     weakening of "a halt exits only via restart"; every other shape keeps the
//     halt and keeps waiting.
func (s *service) haltedTick(ctx context.Context) {
	st, verdict, err := s.observeState(ctx)
	if err != nil {
		s.log.Warn("halted reconcile: read state", "err", err)
		return
	}
	if verdict == events.ReconcileFlat {
		if err := s.publishReconciled(ctx, st, verdict); err != nil {
			s.log.Error("halted reconcile: publish", "err", err)
			return
		}
		s.halted = false
		s.haltedShape = ""
		haltedGauge.Set(0)
		s.applyVerdict(st, verdict)
		s.log.Info("halt lifted: exchange is flat",
			"perp", st.PerpSize, "spot", st.SpotSize, "collateral", st.Collateral)
		return
	}
	shape := haltShape(st, verdict)
	if shape == s.haltedShape {
		return
	}
	s.log.Error("halted: exchange position changed", "verdict", verdict,
		"perp", st.PerpSize, "spot", st.SpotSize, "open_orders", st.OpenOrders)
	if err := s.publishReconciled(ctx, st, verdict); err != nil {
		s.log.Error("halted reconcile: publish", "err", err)
		return
	}
	s.haltedShape = shape
}

// haltShape fingerprints an exchange snapshot for the halted change-gate.
func haltShape(st *exchange.PositionState, verdict string) string {
	return fmt.Sprintf("%s|%v|%v|%d", verdict, st.PerpSize, st.SpotSize, st.OpenOrders)
}

func (s *service) publishReconciled(ctx context.Context, st *exchange.PositionState, verdict string) error {
	rep := events.Reconciled{
		Symbol: s.cfg.Symbol, Verdict: verdict,
		PerpSize: st.PerpSize, SpotSize: st.SpotSize,
		Collateral: st.Collateral, OpenOrders: st.OpenOrders,
		Time: time.Now().UTC(),
	}
	data, err := json.Marshal(rep)
	if err != nil {
		return fmt.Errorf("marshal reconciled: %w", err)
	}
	// Every snapshot is distinct, so the dedup id carries the timestamp — two
	// snapshots inside the dedup window must both be seen.
	msgID := fmt.Sprintf("reconciled:%d", rep.Time.UnixNano())
	if _, err := s.js.Publish(ctx, events.SubjReconciled, data, jetstream.WithMsgID(msgID)); err != nil {
		return fmt.Errorf("publish reconciled: %w", err)
	}
	return nil
}

// applyVerdict arms the local flags from a published snapshot: the funding
// gate follows the position, an unbalanced position halts trading. Runs only
// on the worker goroutine (startup runs before the consumer starts).
func (s *service) applyVerdict(st *exchange.PositionState, verdict string) {
	switch verdict {
	case events.ReconcileOpen:
		s.positionOpen.Store(true)
	case events.ReconcileFlat:
		s.positionOpen.Store(false)
	case events.ReconcileUnbalanced:
		s.halted = true
		// Seed the change-gate with the shape that caused the halt: it was just
		// published, so the first halted tick stays quiet until something moves.
		s.haltedShape = haltShape(st, verdict)
		haltedGauge.Set(1)
		alertsTotal.Inc()
		s.log.Error("ALERT: exchange position unbalanced at reconcile — halting, manual intervention required",
			"perp", st.PerpSize, "spot", st.SpotSize, "open_orders", st.OpenOrders)
	}
}

// handle processes one intent and settles its JetStream delivery: ack on a
// terminal outcome (done, or recorded as failed), nak to retry a transient
// problem. Bybit's orderLinkId dedup makes a retry safe.
func (s *service) handle(ctx context.Context, m jetstream.Msg) {
	var intent events.Intent
	if err := json.Unmarshal(m.Data(), &intent); err != nil {
		// Unparseable message: it will never succeed, so don't redeliver it.
		s.log.Error("unmarshal intent", "err", err)
		m.Term()
		return
	}

	// Halted: the exchange position needs a human. Term, not nak — an intent
	// decided before the halt must not fire after the position is fixed.
	if s.halted {
		s.log.Error("halted, dropping intent", "id", intent.ID, "side", intent.Side)
		m.Term()
		return
	}

	var err error
	switch intent.Side {
	case events.IntentOpen:
		err = s.openPosition(ctx, intent)
	case events.IntentClose:
		err = s.closePosition(ctx, intent)
	default:
		s.log.Error("unknown intent side", "side", intent.Side, "id", intent.ID)
		m.Term()
		return
	}

	if errors.Is(err, errRetry) {
		// On the final delivery a nak is a silent drop: JetStream won't redeliver
		// past MaxDeliver, and strategy would wait forever in pending for a fact
		// that never comes. Emit exec.failed so the drop is a recorded outcome.
		if md, mdErr := m.Metadata(); mdErr == nil && md.NumDelivered >= maxDeliver {
			s.emitFailed(ctx, intent, "retries exhausted: "+err.Error())
			m.Term()
			return
		}
		s.log.Warn("transient failure, will retry", "id", intent.ID, "err", err)
		m.Nak()
		return
	}
	m.Ack()
}

// openPosition opens the delta-neutral pair: spot long first, then perp short.
// If the perp leg fails we sell the spot back to flatten, so a failed open
// leaves us flat rather than long. The spot leg is placed first because it is
// the cheaper, more liquid one to unwind if we have to back out.
func (s *service) openPosition(ctx context.Context, in events.Intent) error {
	// Position-level idempotency, on top of the per-leg cloid dedup: an open on
	// top of a held position would double it. Reaches here only through a state
	// desync (e.g. strategy lost its exec history and re-decided from flat), so
	// drop rather than trade — positionOpen is reconcile-seeded, so it is
	// trusted — and answer with a fresh reconcile snapshot, which is what walks
	// the desynced strategy back to the exchange's truth.
	if s.positionOpen.Load() {
		s.log.Warn("open intent while position already open, dropping and re-reconciling", "id", in.ID)
		if err := s.reconcile(ctx); err != nil {
			s.log.Warn("re-reconcile after stale intent", "err", err)
		}
		return nil
	}
	spot, err := s.placeLeg(ctx, in, exchange.OrderRequest{
		Category:    exchange.CategorySpot,
		Symbol:      in.Symbol,
		Side:        exchange.SideBuy,
		Qty:         s.cfg.OrderQty,
		OrderLinkID: legID(in.ID, "s"),
	})
	if err != nil {
		// Nothing opened yet. A terminal reject (permission, balance) won't fix
		// itself, so record it and stop; anything else gets a redelivery.
		if s.ex.Classify(err) == exchange.ErrTerminal {
			s.emitFailed(ctx, in, "spot leg failed: "+err.Error())
			return nil
		}
		return fmt.Errorf("open spot leg: %w: %w", err, errRetry)
	}

	perp, err := s.placeLeg(ctx, in, exchange.OrderRequest{
		Category:    exchange.CategoryLinear,
		Symbol:      in.Symbol,
		Side:        exchange.SideSell,
		Qty:         s.cfg.OrderQty,
		OrderLinkID: legID(in.ID, "p"),
	})
	if err != nil {
		// Leg risk: spot is long but the perp short failed. Roll spot back so we
		// don't hold a naked long. Terminal regardless of the error kind — a retry
		// would re-open the spot leg.
		s.log.Error("perp leg failed, rolling back spot", "id", in.ID, "err", err)
		if _, rbErr := s.placeLeg(ctx, in, exchange.OrderRequest{
			Category:    exchange.CategorySpot,
			Symbol:      in.Symbol,
			Side:        exchange.SideSell,
			Qty:         s.cfg.OrderQty,
			OrderLinkID: legID(in.ID, "rb"),
		}); rbErr != nil {
			s.alert(ctx, in, "ROLLBACK FAILED — naked spot long, manual intervention required: "+rbErr.Error())
			return nil
		}
		rollbacksTotal.Inc()
		s.emitFailed(ctx, in, "perp leg failed, spot rolled back: "+err.Error())
		return nil
	}

	s.emitOpened(ctx, in, spot, perp)
	return nil
}

// closePosition unwinds the pair: buy back the perp short (reduce-only so it can
// only shrink the position), then sell the spot long. Unlike an open there is no
// clean rollback — re-opening to rebalance is its own risk — so if the second
// leg fails we are left unbalanced and halt the intent for a human.
func (s *service) closePosition(ctx context.Context, in events.Intent) error {
	// Mirror of openPosition's guard: closing nothing would place a naked spot
	// sell (the reduce-only perp leg protects itself, the spot leg doesn't).
	// The fresh snapshot walks a desynced strategy back to flat.
	if !s.positionOpen.Load() {
		s.log.Warn("close intent with no open position, dropping and re-reconciling", "id", in.ID)
		if err := s.reconcile(ctx); err != nil {
			s.log.Warn("re-reconcile after stale intent", "err", err)
		}
		return nil
	}
	perp, err := s.placeLeg(ctx, in, exchange.OrderRequest{
		Category:    exchange.CategoryLinear,
		Symbol:      in.Symbol,
		Side:        exchange.SideBuy,
		Qty:         s.cfg.OrderQty,
		OrderLinkID: legID(in.ID, "p"),
		ReduceOnly:  true,
	})
	if err != nil {
		// Nothing changed yet. Terminal → record and stop; otherwise redeliver.
		if s.ex.Classify(err) == exchange.ErrTerminal {
			s.emitFailed(ctx, in, "close perp leg failed: "+err.Error())
			return nil
		}
		return fmt.Errorf("close perp leg: %w: %w", err, errRetry)
	}

	spot, err := s.placeLeg(ctx, in, exchange.OrderRequest{
		Category:    exchange.CategorySpot,
		Symbol:      in.Symbol,
		Side:        exchange.SideSell,
		Qty:         s.cfg.OrderQty,
		OrderLinkID: legID(in.ID, "s"),
	})
	if err != nil {
		s.alert(ctx, in, "UNBALANCED — perp closed but spot sell failed, manual intervention required: "+err.Error())
		return nil
	}

	s.emitClosed(ctx, in, spot, perp)
	return nil
}

// placeLeg submits one order. A duplicate orderLinkId means a previous delivery
// already placed this exact leg, so we treat it as success (idempotent replay)
// rather than an error.
func (s *service) placeLeg(ctx context.Context, in events.Intent, req exchange.OrderRequest) (*exchange.OrderResult, error) {
	start := time.Now()
	res, err := s.ex.PlaceOrder(ctx, req)
	placeSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		if s.ex.Classify(err) == exchange.ErrDuplicate {
			legsTotal.WithLabelValues(req.Category, req.Side, "duplicate").Inc()
			s.log.Info("leg already placed (idempotent replay)",
				"id", in.ID, "link", req.OrderLinkID, "category", req.Category)
			return &exchange.OrderResult{OrderLinkID: req.OrderLinkID}, nil
		}
		legsTotal.WithLabelValues(req.Category, req.Side, "failed").Inc()
		return nil, err
	}
	legsTotal.WithLabelValues(req.Category, req.Side, "placed").Inc()
	s.log.Info("leg placed", "id", in.ID, "category", req.Category,
		"side", req.Side, "qty", req.Qty, "order_id", res.OrderID)
	return res, nil
}

// legID derives a deterministic, per-leg client order id from the intent id.
// Same intent + same leg => same id => Bybit dedup => no doubled fills on replay.
func legID(intentID, leg string) string {
	return intentID + "-" + leg
}

// pollFunding periodically asks the exchange for funding credited since the last
// poll and emits each settlement as a durable exec.funding.received fact. It is
// gated on positionOpen (seeded by reconcile, flipped by opened/closed): funding
// only accrues on a held position, and polling while flat would fill the ledger
// with unattached payments (the mock emits one per poll unconditionally). One
// trailing poll runs after a close to catch a settlement that landed between the
// last open-poll and the close itself. `since` advances past the newest
// settlement seen so a payment is emitted once.
func (s *service) pollFunding(ctx context.Context) {
	if s.cfg.FundingPoll <= 0 {
		s.log.Info("funding polling disabled")
		return
	}
	t := time.NewTicker(s.cfg.FundingPoll)
	defer t.Stop()
	since := time.Now().UTC()
	s.log.Info("polling funding", "every", s.cfg.FundingPoll, "symbol", s.cfg.Symbol)

	wasOpen := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			open := s.positionOpen.Load()
			if !open && !wasOpen {
				continue
			}
			wasOpen = open
			payments, err := s.ex.Funding(ctx, s.cfg.Symbol, since)
			if err != nil {
				s.log.Warn("poll funding", "err", err)
				continue
			}
			for _, p := range payments {
				s.emitFunding(ctx, p)
				if p.Time.After(since) {
					since = p.Time
				}
			}
		}
	}
}

func (s *service) emitFunding(ctx context.Context, p exchange.FundingPayment) {
	data, err := json.Marshal(events.FundingReceived{
		PaymentID: p.ID, Symbol: p.Symbol, Amount: p.Amount, Time: p.Time,
	})
	if err != nil {
		s.log.Error("marshal funding", "id", p.ID, "err", err)
		return
	}
	// Dedup on the settlement id so a redelivered or re-emitted payment is
	// published once within the window; portfolio's ledger backstops it durably.
	if _, err := s.js.Publish(ctx, events.SubjFundingReceived, data, jetstream.WithMsgID(p.ID)); err != nil {
		s.log.Error("publish funding", "id", p.ID, "err", err)
		return
	}
	fundingReceivedTotal.Inc()
	s.log.Info("funding received", "id", p.ID, "symbol", p.Symbol, "amount", p.Amount)
}

func (s *service) emitOpened(ctx context.Context, in events.Intent, spot, perp *exchange.OrderResult) {
	s.emit(ctx, events.SubjPositionOpened, events.ExecReport{
		IntentID: in.ID, Symbol: in.Symbol, Side: in.Side, Qty: s.qty(),
		SpotOrderID: spot.OrderID, PerpOrderID: perp.OrderID,
		SpotPrice: spot.Price, PerpPrice: perp.Price, Fee: spot.Fee + perp.Fee,
		Time: time.Now().UTC(),
	})
	s.positionOpen.Store(true)
	intentsTotal.WithLabelValues(in.Side, "opened").Inc()
	s.log.Info("position opened", "id", in.ID, "reason", in.Reason)
}

func (s *service) emitClosed(ctx context.Context, in events.Intent, spot, perp *exchange.OrderResult) {
	s.emit(ctx, events.SubjPositionClosed, events.ExecReport{
		IntentID: in.ID, Symbol: in.Symbol, Side: in.Side, Qty: s.qty(),
		SpotOrderID: spot.OrderID, PerpOrderID: perp.OrderID,
		SpotPrice: spot.Price, PerpPrice: perp.Price, Fee: spot.Fee + perp.Fee,
		Time: time.Now().UTC(),
	})
	s.positionOpen.Store(false)
	intentsTotal.WithLabelValues(in.Side, "closed").Inc()
	s.log.Info("position closed", "id", in.ID, "reason", in.Reason)
}

func (s *service) emitFailed(ctx context.Context, in events.Intent, reason string) {
	s.log.Warn("terminal failure, emitting exec.failed", "id", in.ID, "side", in.Side, "reason", reason)
	s.emit(ctx, events.SubjExecFailed, events.ExecReport{
		IntentID: in.ID, Symbol: in.Symbol, Side: in.Side, Qty: s.qty(),
		Error: reason, Time: time.Now().UTC(),
	})
	intentsTotal.WithLabelValues(in.Side, "failed").Inc()
}

// alert is a failure we cannot resolve automatically — the exchange position is
// no longer the clean pair (naked spot after a failed rollback, or a half-closed
// position). Trading on top of that would compound the damage, so it halts the
// service the same way an unbalanced startup reconcile does; the exec.failed
// fact carries the reason downstream (notification-service relays it).
func (s *service) alert(ctx context.Context, in events.Intent, reason string) {
	alertsTotal.Inc()
	s.halted = true
	// No snapshot here — an empty gate makes the first halted tick publish the
	// live shape, which is exactly the report a human wants after this alert.
	s.haltedShape = ""
	haltedGauge.Set(1)
	s.log.Error("ALERT: halting trading", "id", in.ID, "reason", reason)
	s.emitFailed(ctx, in, reason)
}

// emit publishes an exec fact to JetStream with the intent id as the dedup key,
// so a redelivered intent that reaches the same outcome does not write the fact
// twice within the dedup window.
func (s *service) emit(ctx context.Context, subj string, report events.ExecReport) {
	data, err := json.Marshal(report)
	if err != nil {
		s.log.Error("marshal exec report", "err", err)
		return
	}
	if _, err := s.js.Publish(ctx, subj, data, jetstream.WithMsgID(report.IntentID+":"+subj)); err != nil {
		s.log.Error("publish exec report", "subj", subj, "id", report.IntentID, "err", err)
	}
}

// qty parses the configured order size for reporting; an unparseable value just
// reports zero, since the orders themselves use the raw string Bybit expects.
func (s *service) qty() float64 {
	q, _ := strconv.ParseFloat(s.cfg.OrderQty, 64)
	return q
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getdur parses a Go duration (e.g. "30s", "1m"); a missing or malformed value
// falls back to def, so a typo degrades to the default rather than crashing.
func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// getfloat parses a float env (e.g. "0.5"); a missing or malformed value falls
// back to def, so a typo degrades to the default rather than crashing.
func getfloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// getbool parses a bool env (1/true/yes and their negatives, case-insensitive);
// a missing or malformed value falls back to def. HL_MAINNET must match HL_API,
// so the safe default is testnet (false).
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
