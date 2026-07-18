// Command fundpub is a smoke-test helper: it publishes one funding tick to core
// NATS as if market-data had produced it, so strategy can be driven without a
// live WebSocket. Untracked throwaway, like closepub/execread.
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
	rate := 0.0
	if len(os.Args) > 1 {
		var err error
		if rate, err = strconv.ParseFloat(os.Args[1], 64); err != nil {
			log.Fatalf("parse rate %q: %v", os.Args[1], err)
		}
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

	data, _ := json.Marshal(events.FundingRate{
		Symbol: "BTCUSDT", Rate: rate, Time: time.Now().UTC(),
	})
	if err := nc.Publish(events.SubjFundingPredicted, data); err != nil {
		log.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Printf("published funding rate=%v", rate)
}
