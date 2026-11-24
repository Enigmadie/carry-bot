// Command portfolio is the bookkeeper. It consumes the EXEC stream of execution
// facts and maintains the position ledger in Postgres: an opened pair becomes an
// open row, a close settles it and books realized P&L. It derives state purely
// from the event stream and never calls the exchange — reconciling that ledger
// against the real exchange position is a later, separate concern (§9).
//
// v1 holds a single delta-neutral position at a time, which is what lets a close
// settle "the open one" without correlating intent ids across open and close.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/metrics"
)

const (
	execStream  = "EXEC"
	durableName = "portfolio-service"
	execMaxAge  = 72 * time.Hour
	ackWait     = 30 * time.Second

	// A failed ledger write is retried in place until it lands, backing off up
	// to retryMaxDelay. Each wait plus one bounded attempt stays under ackWait,
	// so the InProgress heartbeat between attempts keeps the message ours.
	retryMaxDelay  = 15 * time.Second
	attemptTimeout = 10 * time.Second

	// metricsRefresh is how often the ledger aggregates are re-queried into the
	// gauges. The values come from Postgres, not in-memory counters, so they stay
	// correct across a restart instead of resetting to zero.
	metricsRefresh = 15 * time.Second
)

var (
	openPositions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "portfolio",
		Name: "positions_open", Help: "Positions currently open in the ledger.",
	})
	realizedPnL = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "portfolio",
		Name: "realized_pnl_total", Help: "Sum of realized P&L over closed positions, quote currency.",
	})
	fundingTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "portfolio",
		Name: "funding_total", Help: "Sum of all booked funding settlements, quote currency.",
	})
	reconcileMismatch = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "portfolio",
		Name: "reconcile_mismatch", Help: "1 when the latest exchange reconcile snapshot disagrees with the ledger.",
	})
	ledgerStalled = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: "portfolio",
		Name: "ledger_stalled", Help: "1 while an exec fact is being retried against Postgres; nothing behind it is booked.",
	})
)

const schema = `
CREATE TABLE IF NOT EXISTS positions (
    id               BIGSERIAL PRIMARY KEY,
    open_intent_id   TEXT NOT NULL UNIQUE,
    close_intent_id  TEXT,
    symbol           TEXT NOT NULL,
    qty              DOUBLE PRECISION NOT NULL,
    status           TEXT NOT NULL,
    entry_spot_price DOUBLE PRECISION NOT NULL,
    entry_perp_price DOUBLE PRECISION NOT NULL,
    exit_spot_price  DOUBLE PRECISION,
    exit_perp_price  DOUBLE PRECISION,
    fees             DOUBLE PRECISION NOT NULL DEFAULT 0,
    funding_total    DOUBLE PRECISION NOT NULL DEFAULT 0,
    realized_pnl     DOUBLE PRECISION,
    opened_at        TIMESTAMPTZ NOT NULL,
    closed_at        TIMESTAMPTZ
);

-- Funding ledger: one row per settlement, keyed by the exchange's payment id.
-- The PK makes booking idempotent — a redelivered exec.funding.received that
-- conflicts on payment_id is dropped, so funding_total is never double-counted.
CREATE TABLE IF NOT EXISTS funding_payments (
    payment_id  TEXT PRIMARY KEY,
    position_id BIGINT REFERENCES positions(id),
    symbol      TEXT NOT NULL,
    amount      DOUBLE PRECISION NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

-- P&L already banked by shrinking an open position; folded into realized_pnl
-- when the position closes.
ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_partial DOUBLE PRECISION NOT NULL DEFAULT 0;

-- Resize ledger: one row per executed resize, keyed by the intent id. Like
-- funding_payments, the PK is what makes a redelivered fact a no-op.
CREATE TABLE IF NOT EXISTS position_resizes (
    intent_id   TEXT PRIMARY KEY,
    position_id BIGINT NOT NULL REFERENCES positions(id),
    delta       DOUBLE PRECISION NOT NULL,
    spot_price  DOUBLE PRECISION NOT NULL,
    perp_price  DOUBLE PRECISION NOT NULL,
    fee         DOUBLE PRECISION NOT NULL,
    resized_at  TIMESTAMPTZ NOT NULL
);`

type config struct {
	NATSURL     string
	DatabaseURL string
	MetricsAddr string
}

func loadConfig() config {
	return config{
		NATSURL:     getenv("NATS_URL", nats.DefaultURL),
		DatabaseURL: getenv("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5555/carrybot"),
		MetricsAddr: getenv("METRICS_ADDR", ":2115"),
	}
}

type service struct {
	log  *slog.Logger
	js   jetstream.JetStream
	pool *pgxpool.Pool
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Error("ensure schema", "err", err)
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

	s := &service{log: log, js: js, pool: pool}
	if err := s.run(ctx); err != nil && ctx.Err() == nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

func (s *service) run(ctx context.Context) error {
	// EXEC is owned by order-service; ensure it here too so portfolio can start
	// (and be smoke-tested) independently of start order. Config matches order's.
	if _, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      execStream,
		Subjects:  []string{"exec.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    execMaxAge,
	}); err != nil {
		return fmt.Errorf("ensure exec stream: %w", err)
	}

	cons, err := s.js.CreateOrUpdateConsumer(ctx, execStream, jetstream.ConsumerConfig{
		Durable:        durableName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{events.SubjPositionOpened, events.SubjPositionClosed, events.SubjPositionResized, events.SubjExecFailed, events.SubjFundingReceived, events.SubjReconciled},
		AckWait:        ackWait,
		// The ledger is a fold over facts in stream order: a dropped resize
		// leaves qty wrong for good, and a fact applied after a later one
		// (a resize after its close) books against the wrong position. So no
		// delivery cap, and one fact in flight at a time.
		MaxDeliver:    -1,
		MaxAckPending: 1,
	})
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}
	s.log.Info("consuming exec facts", "stream", execStream, "durable", durableName)

	// Ledger gauges are sourced from Postgres, not the event handlers, so they
	// survive a restart. Refresh on its own clock alongside the consume loop.
	go s.refreshMetrics(ctx)

	msgs := make(chan jetstream.Msg, 16)
	cc, err := cons.Consume(func(m jetstream.Msg) { msgs <- m })
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}
	defer cc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-msgs:
			s.handle(ctx, m)
		}
	}
}

// refreshMetrics re-queries the ledger aggregates into the gauges on a ticker,
// once at startup and then every metricsRefresh, until ctx is cancelled.
func (s *service) refreshMetrics(ctx context.Context) {
	t := time.NewTicker(metricsRefresh)
	defer t.Stop()
	for {
		s.scrapeMetrics(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// scrapeMetrics reads the three ledger aggregates and sets the gauges. A query
// error leaves the last good value in place rather than zeroing a gauge.
func (s *service) scrapeMetrics(ctx context.Context) {
	var open int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM positions WHERE status = 'open'`).Scan(&open); err != nil {
		s.log.Warn("metrics: open positions", "err", err)
		return
	}
	openPositions.Set(float64(open))

	var pnl float64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(realized_pnl), 0) FROM positions WHERE status = 'closed'`).Scan(&pnl); err != nil {
		s.log.Warn("metrics: realized pnl", "err", err)
		return
	}
	realizedPnL.Set(pnl)

	var funding float64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount), 0) FROM funding_payments`).Scan(&funding); err != nil {
		s.log.Warn("metrics: funding total", "err", err)
		return
	}
	fundingTotal.Set(funding)
}

// retryBaseDelay is the first backoff step; a var so tests can shrink it.
var retryBaseDelay = time.Second

// errPoison marks a message that can never be applied (bad payload, unknown
// subject); it is terminated instead of retried.
var errPoison = errors.New("poison message")

func (s *service) handle(ctx context.Context, m jetstream.Msg) {
	err := retryUntilApplied(ctx, m, s.log, func(ctx context.Context) error {
		return s.apply(ctx, m)
	})
	switch {
	case err == nil:
		m.Ack()
	case errors.Is(err, errPoison):
		s.log.Error("dropping unprocessable message", "subject", m.Subject(), "err", err)
		m.Term()
	default:
		// Shutting down mid-retry: hand the fact back for the next start.
		m.Nak()
	}
}

// retryUntilApplied runs fn until it succeeds, fails with errPoison, or ctx
// ends. Every handler error short of poison is a database failure (business
// no-ops return nil), so retrying is always safe, and giving up would lose the
// fact. A permanent DB error stalls the ledger here; ledgerStalled says so.
func retryUntilApplied(ctx context.Context, m jetstream.Msg, log *slog.Logger, fn func(context.Context) error) error {
	delay := retryBaseDelay
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := fn(actx)
		cancel()
		if err == nil || errors.Is(err, errPoison) {
			ledgerStalled.Set(0)
			return err
		}
		ledgerStalled.Set(1)
		log.Warn("ledger write failed, retrying", "subject", m.Subject(), "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		m.InProgress()
		delay = min(delay*2, retryMaxDelay)
	}
}

// apply books one fact. Funding and reconcile snapshots ride the same EXEC
// stream but carry different payloads, so they branch off before the
// ExecReport decode.
func (s *service) apply(ctx context.Context, m jetstream.Msg) error {
	switch m.Subject() {
	case events.SubjFundingReceived:
		var f events.FundingReceived
		if err := json.Unmarshal(m.Data(), &f); err != nil {
			return fmt.Errorf("%w: unmarshal funding: %v", errPoison, err)
		}
		return s.onFunding(ctx, f)
	case events.SubjReconciled:
		var r events.Reconciled
		if err := json.Unmarshal(m.Data(), &r); err != nil {
			return fmt.Errorf("%w: unmarshal reconciled: %v", errPoison, err)
		}
		return s.onReconciled(ctx, r)
	}

	var r events.ExecReport
	if err := json.Unmarshal(m.Data(), &r); err != nil {
		return fmt.Errorf("%w: unmarshal exec report: %v", errPoison, err)
	}
	switch m.Subject() {
	case events.SubjPositionOpened:
		return s.onOpened(ctx, r)
	case events.SubjPositionClosed:
		return s.onClosed(ctx, r)
	case events.SubjPositionResized:
		return s.onResized(ctx, r)
	case events.SubjExecFailed:
		s.log.Warn("exec failed", "intent", r.IntentID, "error", r.Error)
		return nil
	default:
		return fmt.Errorf("%w: unknown exec subject %q", errPoison, m.Subject())
	}
}

const insertOpen = `
INSERT INTO positions
    (open_intent_id, symbol, qty, status, entry_spot_price, entry_perp_price, fees, opened_at)
VALUES ($1, $2, $3, 'open', $4, $5, $6, $7)
ON CONFLICT (open_intent_id) DO NOTHING;`

func (s *service) onOpened(ctx context.Context, r events.ExecReport) error {
	tag, err := s.pool.Exec(ctx, insertOpen,
		r.IntentID, r.Symbol, r.Qty, r.SpotPrice, r.PerpPrice, r.Fee, r.Time)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.log.Info("open already recorded (idempotent replay)", "intent", r.IntentID)
		return nil
	}
	s.log.Info("position opened", "intent", r.IntentID, "symbol", r.Symbol,
		"qty", r.Qty, "spot", r.SpotPrice, "perp", r.PerpPrice, "fee", r.Fee)
	return nil
}

// Realized P&L of the delta-neutral pair: the spot long gains when spot rises,
// the perp short gains when perp falls, minus all fees, plus funding booked over
// the life of the position, plus whatever earlier shrinks already banked. fees in
// the expression is the pre-update fee (entry and resizes).
const settleClose = `
UPDATE positions
SET status          = 'closed',
    close_intent_id = $1,
    exit_spot_price = $2,
    exit_perp_price = $3,
    fees            = fees + $4,
    closed_at       = $5,
    realized_pnl    = ($2 - entry_spot_price) * qty
                    + (entry_perp_price - $3) * qty
                    - (fees + $4)
                    + funding_total
                    + realized_partial
WHERE id = (SELECT id FROM positions WHERE status = 'open' ORDER BY opened_at DESC LIMIT 1)
RETURNING realized_pnl;`

func (s *service) onClosed(ctx context.Context, r events.ExecReport) error {
	var pnl float64
	err := s.pool.QueryRow(ctx, settleClose,
		r.IntentID, r.SpotPrice, r.PerpPrice, r.Fee, r.Time).Scan(&pnl)
	if err == pgx.ErrNoRows {
		// No open position to settle: either a redelivered close (already settled)
		// or a close with nothing open. Both are safe no-ops.
		s.log.Warn("close with no open position", "intent", r.IntentID)
		return nil
	}
	if err != nil {
		return err
	}
	s.log.Info("position closed", "intent", r.IntentID, "realized_pnl", pnl)
	return nil
}

// A grow blends the new fill into the entry prices (size-weighted average), so
// the close formula keeps working on one qty and one entry per leg.
const applyGrow = `
UPDATE positions
SET entry_spot_price = CASE WHEN $3::float8 > 0 THEN (entry_spot_price * qty + $3 * $2::float8) / (qty + $2) ELSE entry_spot_price END,
    entry_perp_price = CASE WHEN $4::float8 > 0 THEN (entry_perp_price * qty + $4 * $2) / (qty + $2) ELSE entry_perp_price END,
    qty              = round((qty + $2)::numeric, 8)::float8,
    fees             = fees + $5
WHERE id = $1;`

// A shrink closes $2 coins of each leg at the fill prices: that slice's P&L is
// banked in realized_partial and the entries of what remains are untouched.
// qty is rounded in both statements: float8 sums like 0.64 − 0.22 otherwise
// store 0.42000000000000004.
const applyShrink = `
UPDATE positions
SET realized_partial = realized_partial
                     + CASE WHEN $3::float8 > 0 THEN ($3 - entry_spot_price) * $2::float8 ELSE 0 END
                     + CASE WHEN $4::float8 > 0 THEN (entry_perp_price - $4) * $2 ELSE 0 END,
    qty              = round((qty - $2)::numeric, 8)::float8,
    fees             = fees + $5
WHERE id = $1;`

// onResized applies a resize to the open position, in one transaction with its
// position_resizes row so a redelivered fact (PK conflict) changes nothing. A
// leg price of 0 means the fill price is unknown (a replayed leg): that leg is
// then left out of the average or the banked P&L instead of poisoning it.
func (s *service) onResized(ctx context.Context, r events.ExecReport) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var posID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM positions WHERE status = 'open' ORDER BY opened_at DESC LIMIT 1 FOR UPDATE`).Scan(&posID)
	if err == pgx.ErrNoRows {
		s.log.Warn("resize with no open position", "intent", r.IntentID)
		return nil
	}
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx,
		`INSERT INTO position_resizes (intent_id, position_id, delta, spot_price, perp_price, fee, resized_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (intent_id) DO NOTHING`,
		r.IntentID, posID, r.Qty, r.SpotPrice, r.PerpPrice, r.Fee, r.Time)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.log.Info("resize already recorded (idempotent replay)", "intent", r.IntentID)
		return tx.Commit(ctx)
	}

	query, size := applyGrow, r.Qty
	if r.Qty < 0 {
		query, size = applyShrink, -r.Qty
	}
	if _, err := tx.Exec(ctx, query, posID, size, r.SpotPrice, r.PerpPrice, r.Fee); err != nil {
		return err
	}
	s.log.Info("position resized", "intent", r.IntentID, "position", posID,
		"delta", r.Qty, "spot", r.SpotPrice, "perp", r.PerpPrice, "fee", r.Fee)
	return tx.Commit(ctx)
}

// onReconciled checks order-service's exchange snapshot against the ledger:
// both should agree on whether a position is held. A mismatch is only reported
// (warn log + gauge for alerting) — the ledger is history, not live state, and
// deciding what to do about a divergence is the human's call. The gauge also
// resets on agreement, so it tracks the *latest* snapshot.
func (s *service) onReconciled(ctx context.Context, r events.Reconciled) error {
	var openCount int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM positions WHERE status = 'open'`).Scan(&openCount); err != nil {
		return err
	}

	ledgerOpen := openCount > 0
	exchangeOpen := r.Verdict == events.ReconcileOpen
	switch {
	case r.Verdict == events.ReconcileUnbalanced:
		reconcileMismatch.Set(1)
		s.log.Error("reconcile: exchange position UNBALANCED, order-service halted",
			"perp", r.PerpSize, "spot", r.SpotSize, "open_orders", r.OpenOrders)
	case ledgerOpen != exchangeOpen:
		reconcileMismatch.Set(1)
		s.log.Error("reconcile: ledger disagrees with exchange",
			"ledger_open_positions", openCount, "exchange_verdict", r.Verdict,
			"perp", r.PerpSize, "spot", r.SpotSize)
	default:
		reconcileMismatch.Set(0)
		s.log.Info("reconcile: ledger agrees with exchange",
			"verdict", r.Verdict, "open_positions", openCount)
	}
	return nil
}

const insertFunding = `
INSERT INTO funding_payments (payment_id, position_id, symbol, amount, received_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (payment_id) DO NOTHING;`

// onFunding books one settlement: it attaches the payment to the currently open
// position (if any) and adds it to that position's funding_total, all in one
// transaction so the ledger row and the running total never diverge. The ledger
// PK makes a redelivery a no-op — the conflicting insert affects no row, so we
// skip the increment. Funding that arrives while flat is recorded unattached
// (position_id NULL) and does not move any total.
func (s *service) onFunding(ctx context.Context, f events.FundingReceived) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var posID int64
	hasOpen := true
	err = tx.QueryRow(ctx,
		`SELECT id FROM positions WHERE status = 'open' ORDER BY opened_at DESC LIMIT 1`).Scan(&posID)
	if err == pgx.ErrNoRows {
		hasOpen = false
	} else if err != nil {
		return err
	}

	var posParam any
	if hasOpen {
		posParam = posID
	}
	tag, err := tx.Exec(ctx, insertFunding, f.PaymentID, posParam, f.Symbol, f.Amount, f.Time)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.log.Info("funding already booked (idempotent replay)", "payment", f.PaymentID)
		return tx.Commit(ctx)
	}
	if !hasOpen {
		s.log.Warn("funding received while flat, recorded unattached", "payment", f.PaymentID, "amount", f.Amount)
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE positions SET funding_total = funding_total + $1 WHERE id = $2`, f.Amount, posID); err != nil {
		return err
	}
	s.log.Info("funding booked", "payment", f.PaymentID, "position", posID, "amount", f.Amount)
	return tx.Commit(ctx)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
