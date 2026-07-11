// User-fills WebSocket watch: the fast lane of runtime reconciliation. The
// periodic REST reconcile (main.go) catches any drift within RECONCILE_POLL;
// this watch collapses that window to seconds by nudging a reconcile as soon
// as a fill the bot did not place lands on the account — ADL, a liquidation,
// or a manual order. Fills are only a trigger: position truth still comes from
// State(), so a dropped frame or a dead socket costs latency, never
// correctness — which is why the reconnect/keepalive here can stay simple.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	// Hyperliquid drops an idle socket after ~60s; ping well inside that window.
	// Every ping draws a pong frame, so a healthy socket never goes silent for
	// wsStaleTimeout — a read that does is a dead/half-open connection.
	wsPingInterval   = 15 * time.Second
	wsStaleTimeout   = 45 * time.Second
	wsReconnectDelay = 3 * time.Second

	mainnetWS = "wss://api.hyperliquid.xyz/ws"
	testnetWS = "wss://api.hyperliquid-testnet.xyz/ws"
)

// userWSURL resolves the WS host the same way the REST host resolves: an
// explicit HL_WS wins, otherwise HL_MAINNET picks the network.
func userWSURL(cfg config) string {
	if cfg.HLWS != "" {
		return cfg.HLWS
	}
	if cfg.HLMainnet {
		return mainnetWS
	}
	return testnetWS
}

// watchUserFills keeps the user-fills subscription alive for the lifetime of
// the service, reconnecting with a fixed delay.
func (s *service) watchUserFills(ctx context.Context, wsURL, user string) {
	s.log.Info("watching user fills", "url", wsURL, "user", user)
	for {
		err := s.userFillsOnce(ctx, wsURL, user)
		if ctx.Err() != nil {
			return
		}
		userWSReconnects.Inc()
		s.log.Warn("user fills stream dropped, reconnecting", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wsReconnectDelay):
		}
	}
}

func (s *service) userFillsOnce(ctx context.Context, wsURL, user string) error {
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	c.SetReadLimit(1 << 20)

	sub := map[string]any{
		"method":       "subscribe",
		"subscription": map[string]any{"type": "userFills", "user": user},
	}
	if err := wsjson.Write(ctx, c, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	go func() { // ping until this connection dies; each ping draws a pong frame
		t := time.NewTicker(wsPingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := wsjson.Write(ctx, c, map[string]any{"method": "ping"}); err != nil {
					return // connection is going away; the read loop will surface it
				}
			}
		}
	}()

	for {
		// A per-read deadline is the gap detection: pongs keep a healthy socket
		// talking, so a silent one is dead and the timeout forces the reconnect
		// a half-open TCP connection would otherwise never trigger.
		rctx, cancel := context.WithTimeout(ctx, wsStaleTimeout)
		_, data, err := c.Read(rctx)
		cancel()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if dir, ok := foreignFill(data); ok {
			userWSNudges.Inc()
			select {
			case s.nudge <- dir:
			default: // a reconcile is already queued; it covers this fill too
			}
		}
	}
}

// userFillsFrame is the userFills channel payload. Only the fields the nudge
// decision needs: cloid tells ours from foreign, dir names what happened.
type userFillsFrame struct {
	Channel string `json:"channel"`
	Data    struct {
		IsSnapshot bool `json:"isSnapshot"`
		Fills      []struct {
			Dir   string `json:"dir"`
			Cloid string `json:"cloid"`
		} `json:"fills"`
	} `json:"data"`
}

// foreignFill reports whether a raw WS frame carries a fill the bot did not
// place, returning its dir (e.g. "Auto-Deleveraging"). Every bot order carries
// a cloid, so a fill without one is foreign. The snapshot frame (history
// replayed on subscribe) is skipped: startup reconcile already covered it.
// Non-fill frames (subscriptionResponse, pong) fall through.
func foreignFill(raw []byte) (string, bool) {
	var f userFillsFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", false
	}
	if f.Channel != "userFills" || f.Data.IsSnapshot {
		return "", false
	}
	for _, fill := range f.Data.Fills {
		if fill.Cloid == "" {
			return fill.Dir, true
		}
	}
	return "", false
}
