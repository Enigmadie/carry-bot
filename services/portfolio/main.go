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

	"github.com/Enigmadie/carry-bot/pkg/events"
)

const (
	execStream  = "EXEC"
	durableName = "portfolio-service"
	execMaxAge  = 72 * time.Hour
	ackWait     = 30 * time.Second
	maxDeliver  = 5
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
);`

type config struct {
	NATSURL     string
	DatabaseURL string
}

func loadConfig() config {
	return config{
		NATSURL:     getenv("NATS_URL", nats.DefaultURL),
		DatabaseURL: getenv("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5555/carrybot"),
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
		FilterSubjects: []string{events.SubjPositionOpened, events.SubjPositionClosed, events.SubjExecFailed},
		AckWait:        ackWait,
		MaxDeliver:     maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}
	s.log.Info("consuming exec facts", "stream", execStream, "durable", durableName)

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

func (s *service) handle(ctx context.Context, m jetstream.Msg) {
	var r events.ExecReport
	if err := json.Unmarshal(m.Data(), &r); err != nil {
		s.log.Error("unmarshal exec report", "err", err)
		m.Term()
		return
	}

	var err error
	switch m.Subject() {
	case events.SubjPositionOpened:
		err = s.onOpened(ctx, r)
	case events.SubjPositionClosed:
		err = s.onClosed(ctx, r)
	case events.SubjExecFailed:
		s.log.Warn("exec failed", "intent", r.IntentID, "error", r.Error)
	default:
		s.log.Error("unknown exec subject", "subject", m.Subject())
		m.Term()
		return
	}

	// A DB error is transient (connection blip) — nak to retry. Business no-ops
	// (idempotent replay, close with no open) return nil and are acked.
	if err != nil {
		s.log.Warn("db write failed, will retry", "intent", r.IntentID, "err", err)
		m.Nak()
		return
	}
	m.Ack()
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
// the life of the position. fees in the expression is the pre-update (entry) fee.
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
