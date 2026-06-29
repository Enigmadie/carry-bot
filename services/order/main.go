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
	"os"
	"os/signal"
	"strconv"
	"strings"
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
)

type config struct {
	NATSURL  string
	Symbol   string
	Provider string // EXCHANGE: mock | bybit | hyperliquid
	OrderQty string // base-coin amount per leg; intents carry no size

	FundingPoll time.Duration // how often to poll the exchange for funding; 0 disables

	BybitREST   string
	APIKey      string
	APISecret   string
	MockFailLeg string // mock only: category whose leg fails, for rollback testing

	HLAPI        string  // hyperliquid REST host; "" → testnet
	HLMainnet    bool    // hyperliquid: phantom-agent source; must match HLAPI
	HLPrivateKey string  // hyperliquid agent-wallet signing key
	HLVault      string  // hyperliquid: optional vault/sub-account address
	HLAccount    string  // hyperliquid: master account for funding queries
	HLSlippage   float64 // hyperliquid: IOC price offset past mid; 0 → client default (5%)
	HLLeverage   int     // hyperliquid: perp leverage set (cross) at startup; 0 → skip

	MetricsAddr string
}

func loadConfig() config {
	return config{
		NATSURL:  getenv("NATS_URL", nats.DefaultURL),
		Symbol:   getenv("SYMBOL", "BTCUSDT"),
		Provider: getenv("EXCHANGE", "mock"),
		OrderQty: getenv("ORDER_QTY", "0.001"),

		FundingPoll: getdur("FUNDING_POLL", 30*time.Second),

		BybitREST:   getenv("BYBIT_REST", bybit.TestnetREST),
		APIKey:      os.Getenv("BYBIT_API_KEY"),
		APISecret:   os.Getenv("BYBIT_API_SECRET"),
		MockFailLeg: os.Getenv("MOCK_FAIL_LEG"),

		HLAPI:        os.Getenv("HL_API"),
		HLMainnet:    getbool("HL_MAINNET", false),
		HLPrivateKey: os.Getenv("HL_PRIVATE_KEY"),
		HLVault:      os.Getenv("HL_VAULT"),
		HLAccount:    os.Getenv("HL_ACCOUNT"),
		HLSlippage:   getfloat("HL_SLIPPAGE", 0),
		HLLeverage:   getint("HL_LEVERAGE", 3),

		MetricsAddr: getenv("METRICS_ADDR", ":2114"),
	}
}

type service struct {
	log *slog.Logger
	js  jetstream.JetStream
	ex  exchange.Exchange
	cfg config
}

// buildExchange selects the provider from config. mock-first: the default needs no
// keys or network, so `make order` runs locally; bybit and hyperliquid are opt-in
// via EXCHANGE. ctx is threaded in only for hyperliquid, which must reach the
// exchange at startup to resolve its numeric asset ids (see the case below).
func buildExchange(ctx context.Context, cfg config) (exchange.Exchange, error) {
	switch cfg.Provider {
	case "mock":
		return mock.New(cfg.MockFailLeg), nil
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
		// Hyperliquid opens a perp in isolated 10x by default, which strands the spot
		// USDC and can block the close (see leverage.go). Switch the perp to cross at a
		// low leverage so the spot collateral backs the short. Persists on the account,
		// so once at startup is enough; HL_LEVERAGE=0 opts out (leave the account as-is).
		if cfg.HLLeverage > 0 {
			if err := c.UpdateLeverage(ctx, cfg.Symbol, true, cfg.HLLeverage); err != nil {
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
		log: log,
		js:  js,
		ex:  ex,
		cfg: cfg,
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-msgs:
			s.handle(ctx, m)
		}
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
// stateless about positions — like the rest of order-service, which forgets
// state across restarts — so it emits whenever the exchange reports funding;
// portfolio attaches it to the open position and drops it when flat. `since`
// advances past the newest settlement seen so a payment is emitted once.
func (s *service) pollFunding(ctx context.Context) {
	if s.cfg.FundingPoll <= 0 {
		s.log.Info("funding polling disabled")
		return
	}
	t := time.NewTicker(s.cfg.FundingPoll)
	defer t.Stop()
	since := time.Now().UTC()
	s.log.Info("polling funding", "every", s.cfg.FundingPoll, "symbol", s.cfg.Symbol)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
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

// alert is a failure we cannot resolve automatically. Until notification-service
// exists the alert is a loud log plus a durable exec.failed fact, which is
// enough to wake someone via a Grafana/Prometheus alert later.
func (s *service) alert(ctx context.Context, in events.Intent, reason string) {
	alertsTotal.Inc()
	s.log.Error("ALERT", "id", in.ID, "reason", reason)
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
