// Command fundbt backtests the funding-carry rule against Hyperliquid's public
// funding history: for each coin it replays the strategy's entry/exit thresholds
// over every hourly settlement and reports what the round trips would have
// earned net of fees.
//
// Everything is expressed as a fraction of position notional, so no price series
// is needed — funding accrues at rate × notional and a taker fee costs feeRate ×
// notional, which makes the result size-independent.
//
// The model is deliberately optimistic: fills land exactly at the settlement that
// triggered them, there is no slippage beyond the flat fee, and neither the basis
// gate nor the reopen cooldown blocks an entry. A rule that cannot clear its fees
// here will not clear them live.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Enigmadie/carry-bot/pkg/hyperliquid"
)

// trip is one completed round trip: opened on the tick that crossed the entry
// threshold, closed on the one that fell to the exit threshold.
type trip struct {
	open, close time.Time
	hours       int
	funding     float64 // summed rate over the ticks held, as a fraction of notional
}

type result struct {
	coin       string
	ticks      []hyperliquid.FundingTick
	trips      []trip
	grossFund  float64 // funding collected across all trips
	feeCost    float64 // taker fees across all trips
	holdGross  float64 // sum of every rate in the window: the never-exit baseline
	aboveEntry int     // ticks where the rate met the entry threshold
}

func main() {
	var (
		coins   = flag.String("coins", "HYPE", "comma-separated coins to test (e.g. HYPE,BTC,ETH,SOL)")
		days    = flag.Int("days", 180, "how far back to pull funding history")
		entry   = flag.Float64("entry", 0.0001, "open when the hourly rate is at or above this fraction")
		exit    = flag.Float64("exit", 0.00005, "close when the hourly rate falls to or below this fraction")
		fee     = flag.Float64("fee", 0.00045, "taker fee per leg-side; a round trip pays four (spot+perp, in and out)")
		testnet = flag.Bool("testnet", false, "read testnet history instead of mainnet")
	)
	flag.Parse()

	if *exit >= *entry {
		fmt.Fprintln(os.Stderr, "exit threshold must be below entry threshold (hysteresis)")
		os.Exit(1)
	}

	client, err := hyperliquid.New(hyperliquid.Config{Mainnet: !*testnet})
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	end := time.Now().UTC()
	start := end.AddDate(0, 0, -*days)

	// A round trip crosses the book four times: buy spot + short perp on the way
	// in, and unwind both on the way out.
	const legsPerTrip = 4
	tripFee := *fee * legsPerTrip

	fmt.Printf("window   %s → %s (%d days)\n", start.Format("2006-01-02"), end.Format("2006-01-02"), *days)
	fmt.Printf("rule     enter >= %.4f%%/h, exit <= %.4f%%/h, fee %.4f%% × %d legs = %.3f%% per trip\n\n",
		*entry*100, *exit*100, *fee*100, legsPerTrip, tripFee*100)

	for _, coin := range strings.Split(*coins, ",") {
		coin = strings.TrimSpace(coin)
		if coin == "" {
			continue
		}
		ticks, err := client.FundingHistory(ctx, coin, start, end)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", coin, err)
			continue
		}
		if len(ticks) == 0 {
			fmt.Printf("%s: no funding history in window\n\n", coin)
			continue
		}
		report(simulate(coin, ticks, *entry, *exit, tripFee), tripFee, *days)
	}
}

// simulate replays the strategy's state machine over the settlements. It mirrors
// evaluate() in services/strategy: enter at or above the entry threshold, exit at
// or below the (lower) exit threshold, and stay put in between — the hysteresis
// that stops a position flapping around a single number. Funding accrues from the
// settlement after the open, since the one that triggered the entry had already
// paid out to whoever held the position then.
func simulate(coin string, ticks []hyperliquid.FundingTick, entry, exit, tripFee float64) result {
	res := result{coin: coin, ticks: ticks}

	var (
		holding bool
		cur     trip
	)
	for _, t := range ticks {
		res.holdGross += t.Rate
		if t.Rate >= entry {
			res.aboveEntry++
		}

		if !holding {
			if t.Rate >= entry {
				holding = true
				cur = trip{open: t.Time}
			}
			continue
		}

		cur.funding += t.Rate
		cur.hours++
		if t.Rate <= exit {
			cur.close = t.Time
			res.trips = append(res.trips, cur)
			res.grossFund += cur.funding
			res.feeCost += tripFee
			holding = false
		}
	}
	// An open position at the end of the window is marked to the last settlement
	// and charged the full round trip, so the tail cannot flatter the result.
	if holding {
		cur.close = ticks[len(ticks)-1].Time
		res.trips = append(res.trips, cur)
		res.grossFund += cur.funding
		res.feeCost += tripFee
	}
	return res
}

func report(r result, tripFee float64, days int) {
	net := r.grossFund - r.feeCost
	rates := make([]float64, len(r.ticks))
	for i, t := range r.ticks {
		rates[i] = t.Rate
	}
	sort.Float64s(rates)

	var heldHours int
	winners := 0
	best, worst := trip{funding: -1e9}, trip{funding: 1e9}
	for _, t := range r.trips {
		heldHours += t.hours
		if t.funding-tripFee > 0 {
			winners++
		}
		if t.funding > best.funding {
			best = t
		}
		if t.funding < worst.funding {
			worst = t
		}
	}

	fmt.Printf("── %s ──\n", r.coin)
	fmt.Printf("  settlements   %d hourly ticks, median %.5f%%/h, p90 %.5f%%/h, max %.4f%%/h\n",
		len(r.ticks), pct(rates, 0.5)*100, pct(rates, 0.9)*100, rates[len(rates)-1]*100)
	fmt.Printf("  above entry   %d ticks (%.1f%% of the window)\n",
		r.aboveEntry, 100*float64(r.aboveEntry)/float64(len(r.ticks)))

	baseline := func() {
		fmt.Printf("  baseline      hold throughout: %+.3f%% gross, %+.3f%% net of one round trip\n\n",
			r.holdGross*100, (r.holdGross-tripFee)*100)
	}

	if len(r.trips) == 0 {
		fmt.Printf("  trips         none — the rule never fired\n")
		baseline()
		return
	}

	fmt.Printf("  trips         %d, %d of them net-positive, avg %.1f h held, %.1f%% of the window in position\n",
		len(r.trips), winners, float64(heldHours)/float64(len(r.trips)),
		100*float64(heldHours)/float64(len(r.ticks)))
	fmt.Printf("  gross funding %+.3f%% of notional\n", r.grossFund*100)
	fmt.Printf("  fees          -%.3f%% (%d trips × %.3f%%)\n", r.feeCost*100, len(r.trips), tripFee*100)
	fmt.Printf("  NET           %+.3f%% of notional over %d days  →  %+.1f%%/yr\n",
		net*100, days, net*100*365/float64(days))
	fmt.Printf("  best / worst  %+.3f%% (%dh) / %+.3f%% (%dh), before fees\n",
		best.funding*100, best.hours, worst.funding*100, worst.hours)
	baseline()
}

// pct returns the q-quantile of an already sorted slice (nearest-rank).
func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}
