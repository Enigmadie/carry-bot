package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// TestStreamOnceDetectsDeadSocket pins the gap-detection behaviour: a server
// that accepts the connection and the subscriptions but then goes silent — the
// half-open-socket shape, where the read never errors on its own — must be torn
// down by the watchdog within staleTimeout instead of blocking forever.
func TestStreamOnceDetectsDeadSocket(t *testing.T) {
	defer func(d time.Duration) { staleTimeout = d }(staleTimeout)
	staleTimeout = 200 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		// Consume the two subscribe messages, answer one frame, then go silent.
		ctx := r.Context()
		for i := 0; i < 2; i++ {
			var sub map[string]any
			if err := wsjson.Read(ctx, c, &sub); err != nil {
				t.Errorf("read subscribe: %v", err)
				return
			}
		}
		if err := c.Write(ctx, websocket.MessageText, []byte(sampleAllMids)); err != nil {
			return
		}
		<-ctx.Done() // hold the socket open without sending anything
	}))
	defer srv.Close()

	s := &service{
		log:  slog.Default(),
		cfg:  config{WSURL: "ws" + strings.TrimPrefix(srv.URL, "http"), Symbol: "BTCUSDT"},
		ws:   srv.Client(),
		coin: "MISSING", // no mids match → nothing published → nc stays untouched
	}

	done := make(chan error, 1)
	go func() { done <- s.streamOnce(context.Background()) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "presumed dead") {
			t.Fatalf("streamOnce = %v, want stale-connection error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("streamOnce did not detect the dead socket")
	}
}
