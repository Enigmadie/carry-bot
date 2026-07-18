// Command pricepub is a smoke-test helper: it publishes one spot and one perp
// price tick to core NATS as if market-data had produced them, so strategy's
// basis gate can be driven without a live WebSocket. Untracked throwaway, like
// closepub/fundpub.
package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/Enigmadie/carry-bot/pkg/events"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: pricepub <spot> <perp>")
	}
	spot, err := strconv.ParseFloat(os.Args[1], 64)
	if err != nil {
		log.Fatalf("parse spot %q: %v", os.Args[1], err)
	}
	perp, err := strconv.ParseFloat(os.Args[2], 64)
	if err != nil {
		log.Fatalf("parse perp %q: %v", os.Args[2], err)
	}

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Drain()

	for subj, price := range map[string]float64{
		events.SubjPriceSpot: spot,
		events.SubjPricePerp: perp,
	} {
		data, _ := json.Marshal(events.Price{
			Symbol: "BTCUSDT", Price: price, Time: time.Now().UTC(),
		})
		if err := nc.Publish(subj, data); err != nil {
			log.Fatalf("publish %s: %v", subj, err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Printf("published spot=%v perp=%v", spot, perp)
}
