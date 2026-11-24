package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeMsg implements only what retryUntilApplied touches; the embedded nil
// interface panics if anything else is called.
type fakeMsg struct {
	jetstream.Msg
	inProgress int
}

func (m *fakeMsg) Subject() string   { return "exec.position.resized" }
func (m *fakeMsg) InProgress() error { m.inProgress++; return nil }

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func init() { retryBaseDelay = time.Millisecond }

func TestRetryKeepsGoingUntilTheWriteLands(t *testing.T) {
	m := &fakeMsg{}
	calls := 0
	err := retryUntilApplied(context.Background(), m, quietLog(), func(context.Context) error {
		calls++
		if calls < 8 { // past the old MaxDeliver of 5
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil || calls != 8 {
		t.Fatalf("err=%v calls=%d, want nil after 8 calls", err, calls)
	}
	if m.inProgress != 7 {
		t.Fatalf("InProgress called %d times, want once per retry (7)", m.inProgress)
	}
}

func TestRetryStopsOnPoison(t *testing.T) {
	calls := 0
	err := retryUntilApplied(context.Background(), &fakeMsg{}, quietLog(), func(context.Context) error {
		calls++
		return fmt.Errorf("%w: bad json", errPoison)
	})
	if !errors.Is(err, errPoison) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want poison after one call", err, calls)
	}
}

func TestRetryGivesBackOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := retryUntilApplied(ctx, &fakeMsg{}, quietLog(), func(context.Context) error {
		cancel()
		return errors.New("db down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
}

func TestApplyFlagsBadPayloadAsPoison(t *testing.T) {
	s := &service{log: quietLog()}
	err := s.apply(context.Background(), &payloadMsg{subj: "exec.position.resized", data: []byte("{")})
	if !errors.Is(err, errPoison) {
		t.Fatalf("err=%v, want poison", err)
	}
	err = s.apply(context.Background(), &payloadMsg{subj: "exec.nope", data: []byte("{}")})
	if !errors.Is(err, errPoison) {
		t.Fatalf("unknown subject: err=%v, want poison", err)
	}
}

type payloadMsg struct {
	jetstream.Msg
	subj string
	data []byte
}

func (m *payloadMsg) Subject() string { return m.subj }
func (m *payloadMsg) Data() []byte    { return m.data }
