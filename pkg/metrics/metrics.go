// Package metrics exposes a Prometheus /metrics endpoint for a service. Each
// carry-bot binary declares its counters and gauges against the default registry
// (via promauto) and calls Serve to publish them; Prometheus on the homelab
// scrapes every container by name on the shared infra network, the same way it
// scrapes iot-hub.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric name so they group as carry_* in Prometheus.
const Namespace = "carry"

// Serve publishes the default Prometheus registry at /metrics on addr and shuts
// it down when ctx is cancelled. It never blocks the caller and a bind failure
// is logged rather than fatal: a broken metrics endpoint must not take the
// service it observes down with it.
func Serve(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	go func() {
		log.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
}
