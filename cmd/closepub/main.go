// Command closepub publishes a single strategy.intent.close to the STRATEGY
// stream, to drive a manual close during the testnet round-trip smoke (срез 5).
// Throwaway: strategy keeps open/close state only in memory, so a config change
// can't trigger a clean close — this injects the close intent directly.
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	in := events.Intent{
		ID:     "manual-close-srez5-c",
		Symbol: "HYPEUSDC",
		Side:   events.IntentClose,
		Reason: "manual srez5 close",
		Time:   time.Now(),
	}
	b, _ := json.Marshal(in)

	ack, err := js.Publish(context.Background(), events.SubjIntentClose, b, jetstream.WithMsgID(in.ID))
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	log.Printf("published close intent id=%s seq=%d", in.ID, ack.Sequence)
}
