package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

func TestFormat(t *testing.T) {
	report, _ := json.Marshal(events.ExecReport{
		Symbol: "HYPEUSDC", Side: events.IntentOpen, Qty: 1,
		SpotPrice: 64.99, PerpPrice: 33.86, Fee: 0.05,
		Error: "spot leg failed: boom", Time: time.Now(),
	})
	unbalanced, _ := json.Marshal(events.Reconciled{
		Symbol: "HYPEUSDC", Verdict: events.ReconcileUnbalanced,
		PerpSize: -1, SpotSize: 0.01, OpenOrders: 0, Collateral: 930,
	})
	clean, _ := json.Marshal(events.Reconciled{
		Symbol: "HYPEUSDC", Verdict: events.ReconcileFlat,
	})

	cases := []struct {
		subject  string
		data     []byte
		kind     string
		contains string // "" → expect silence
	}{
		{events.SubjPositionOpened, report, "opened", "OPENED HYPEUSDC"},
		{events.SubjPositionClosed, report, "closed", "CLOSED HYPEUSDC"},
		{events.SubjExecFailed, report, "failed", "spot leg failed: boom"},
		{events.SubjReconciled, unbalanced, "reconciled", "UNBALANCED HYPEUSDC"},
		{events.SubjReconciled, clean, "reconciled", ""},
	}
	for _, c := range cases {
		kind, text, err := format(c.subject, c.data)
		if err != nil {
			t.Fatalf("format(%s): %v", c.subject, err)
		}
		if kind != c.kind {
			t.Errorf("format(%s) kind = %q, want %q", c.subject, kind, c.kind)
		}
		if c.contains == "" {
			if text != "" {
				t.Errorf("format(%s) = %q, want silence", c.subject, text)
			}
		} else if !strings.Contains(text, c.contains) {
			t.Errorf("format(%s) = %q, want substring %q", c.subject, text, c.contains)
		}
	}

	if _, _, err := format(events.SubjPositionOpened, []byte("{broken")); err == nil {
		t.Error("format on malformed JSON: want error")
	}
}

// TestSendClassification pins which HTTP outcomes are retryable: network errors,
// 429 and 5xx must carry errRetry; a 4xx (bad token/chat) must not, since it
// would fail identically on every redelivery.
func TestSendClassification(t *testing.T) {
	newService := func(status int) (*service, *httptest.Server) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/bottok/sendMessage") {
				t.Errorf("unexpected path %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["chat_id"] != "42" || body["text"] == "" {
				t.Errorf("bad request body: %v err=%v", body, err)
			}
			w.WriteHeader(status)
		}))
		return &service{
			log:  slog.Default(),
			http: srv.Client(),
			cfg:  config{BotToken: "tok", ChatID: "42", TelegramAPI: srv.URL},
		}, srv
	}

	cases := []struct {
		status    int
		ok        bool
		retryable bool
	}{
		{http.StatusOK, true, false},
		{http.StatusBadRequest, false, false},
		{http.StatusTooManyRequests, false, true},
		{http.StatusBadGateway, false, true},
	}
	for _, c := range cases {
		s, srv := newService(c.status)
		err := s.send(context.Background(), "hello")
		srv.Close()
		if c.ok != (err == nil) {
			t.Fatalf("status %d: err = %v, want ok=%v", c.status, err, c.ok)
		}
		if errors.Is(err, errRetry) != c.retryable {
			t.Errorf("status %d: retryable = %v, want %v", c.status, errors.Is(err, errRetry), c.retryable)
		}
	}

	// Connection refused (server closed) → retryable.
	s, srv := newService(http.StatusOK)
	srv.Close()
	if err := s.send(context.Background(), "hello"); !errors.Is(err, errRetry) {
		t.Errorf("network error: retryable = false, want true (err=%v)", err)
	}
}
