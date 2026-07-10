// Command notification relays execution facts to a human over Telegram. It is a
// pure consumer of the EXEC stream — it never talks to the exchange and keeps no
// state — and exists because the halt logic in order-service is only useful if
// somebody finds out: an unbalanced position parks the bot until a person acts.
//
// It relays position opens/closes, terminal failures (which include every ALERT
// order-service raises before halting), and reconcile snapshots with an
// unbalanced verdict; clean snapshots and funding settlements stay in
// Grafana where they belong.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Enigmadie/carry-bot/pkg/events"
	"github.com/Enigmadie/carry-bot/pkg/metrics"
)

const (
	execStream  = "EXEC"
	durableName = "notification-service"
	execMaxAge  = 72 * time.Hour
	ackWait     = 30 * time.Second
	maxDeliver  = 5
	sendTimeout = 10 * time.Second
)

// errRetry marks a send failure worth a JetStream redelivery (network blip,
// Telegram 429/5xx). Anything else — a bad token or chat id — will fail the
// same way every time, so the message is logged and dropped instead.
var errRetry = errors.New("retry")

var (
	sentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "notification",
		Name: "sent_total", Help: "Telegram messages delivered, by event kind.",
	}, []string{"kind"})
	sendErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "notification",
		Name: "send_errors_total", Help: "Telegram sendMessage calls that failed.",
	})
)

type config struct {
	NATSURL     string
	BotToken    string
	ChatID      string
	TelegramAPI string // overridable for tests; "" → api.telegram.org
	MetricsAddr string
}

func loadConfig() config {
	return config{
		NATSURL:     getenv("NATS_URL", nats.DefaultURL),
		BotToken:    os.Getenv("TELEGRAM_BOT_TOKEN"),
		ChatID:      os.Getenv("TELEGRAM_CHAT_ID"),
		TelegramAPI: getenv("TELEGRAM_API", "https://api.telegram.org"),
		MetricsAddr: getenv("METRICS_ADDR", ":2116"),
	}
}

type service struct {
	log  *slog.Logger
	js   jetstream.JetStream
	http *http.Client
	cfg  config
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()
	if cfg.BotToken == "" || cfg.ChatID == "" {
		log.Error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
		os.Exit(1)
	}

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

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream init", "err", err)
		os.Exit(1)
	}

	s := &service{
		log:  log,
		js:   js,
		http: &http.Client{Timeout: sendTimeout},
		cfg:  cfg,
	}
	if err := s.run(ctx); err != nil && ctx.Err() == nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

func (s *service) run(ctx context.Context) error {
	// EXEC is owned by order-service; ensure it here too so notification can
	// start (and be smoke-tested) independently of start order.
	if _, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      execStream,
		Subjects:  []string{"exec.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    execMaxAge,
	}); err != nil {
		return fmt.Errorf("ensure exec stream: %w", err)
	}

	// DeliverNew: the very first deployment must not replay up to 72h of exec
	// history into the chat. Only creation is affected — the durable cursor
	// covers every restart after that, so downtime messages still arrive.
	cons, err := s.js.CreateOrUpdateConsumer(ctx, execStream, jetstream.ConsumerConfig{
		Durable:        durableName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		FilterSubjects: []string{events.SubjPositionOpened, events.SubjPositionClosed, events.SubjExecFailed, events.SubjReconciled},
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
	kind, text, err := format(m.Subject(), m.Data())
	if err != nil {
		s.log.Error("unmarshal exec fact", "subject", m.Subject(), "err", err)
		m.Term()
		return
	}
	if text == "" { // fact we deliberately stay quiet about (clean reconcile)
		m.Ack()
		return
	}

	if err := s.send(ctx, text); err != nil {
		sendErrorsTotal.Inc()
		if errors.Is(err, errRetry) {
			s.log.Warn("telegram send failed, will retry", "kind", kind, "err", err)
			m.Nak()
			return
		}
		// Permanent (bad token/chat): dropping beats blocking the stream, but
		// every send will fail like this — the error log is the signal to fix env.
		s.log.Error("telegram send failed permanently, dropping", "kind", kind, "err", err)
		m.Term()
		return
	}
	sentTotal.WithLabelValues(kind).Inc()
	s.log.Info("notified", "kind", kind)
	m.Ack()
}

// format renders one exec fact into the message text. An empty text means the
// fact is consumed silently. Kind labels the metric and logs.
func format(subject string, data []byte) (kind, text string, err error) {
	if subject == events.SubjReconciled {
		var r events.Reconciled
		if err := json.Unmarshal(data, &r); err != nil {
			return "reconciled", "", err
		}
		if r.Verdict != events.ReconcileUnbalanced {
			return "reconciled", "", nil
		}
		return "reconciled", fmt.Sprintf(
			"🚨 UNBALANCED %s at reconcile — trading halted, manual intervention required\nperp=%v spot=%v open_orders=%d collateral=%v",
			r.Symbol, r.PerpSize, r.SpotSize, r.OpenOrders, r.Collateral), nil
	}

	var r events.ExecReport
	if err := json.Unmarshal(data, &r); err != nil {
		return "exec", "", err
	}
	switch subject {
	case events.SubjPositionOpened:
		return "opened", fmt.Sprintf("🟢 OPENED %s qty=%v\nspot=%v perp=%v fee=%v",
			r.Symbol, r.Qty, r.SpotPrice, r.PerpPrice, r.Fee), nil
	case events.SubjPositionClosed:
		return "closed", fmt.Sprintf("⚪ CLOSED %s qty=%v\nspot=%v perp=%v fee=%v",
			r.Symbol, r.Qty, r.SpotPrice, r.PerpPrice, r.Fee), nil
	case events.SubjExecFailed:
		return "failed", fmt.Sprintf("⚠️ FAILED %s %s\n%s", r.Side, r.Symbol, r.Error), nil
	default:
		return "exec", "", fmt.Errorf("unknown exec subject %q", subject)
	}
}

// send posts one message to the Telegram Bot API. Plain text, no parse mode, so
// exchange error strings can't break the markup.
func (s *service) send(ctx context.Context, text string) error {
	body, err := json.Marshal(map[string]string{"chat_id": s.cfg.ChatID, "text": text})
	if err != nil {
		return err
	}
	url := s.cfg.TelegramAPI + "/bot" + s.cfg.BotToken + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", err, errRetry)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	err = fmt.Errorf("telegram status %d: %s", resp.StatusCode, msg)
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return fmt.Errorf("%w: %w", err, errRetry)
	}
	return err
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
