package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type Stats struct {
	Ops       int
	Bytes     int64
	TotalCopy time.Duration
}

type ComputedStats struct {
	Ts   time.Time `json:"ts"`
	Ops  float64   `json:"ops"`
	MBps float64   `json:"MBps"`
	Avg  float64   `json:"latency_avg"`
	Pi50 float64   `json:"latency_50p"`
	Pi95 float64   `json:"latency_95p"`
	Max  float64   `json:"latency_max"`
	Min  float64   `json:"latency_min"`
}

type timings struct {
	copy  time.Duration
	bytes int64
}

func collectLoop(ctx context.Context, interval time.Duration, timingsChan chan timings) error {
	stats := Stats{
		Ops:       0,
		Bytes:     0,
		TotalCopy: 0,
	}
	latencySamples := make([]float64, 0, 5000)

	ticker := time.Tick(interval)
	for {
		select {
		// check for cancellation
		case <-ctx.Done():
			return ctx.Err()

		// print and reset stats every interval
		case <-ticker:
			totDuration := stats.TotalCopy
			computed := ComputedStats{
				Ts:   time.Now().Round(time.Second),
				Ops:  float64(stats.Ops) / interval.Seconds(),
				MBps: (float64(stats.Bytes) / (1024 * 1024)) / interval.Seconds(),
				Avg:  (totDuration.Seconds() * 1000) / float64(stats.Ops),
			}

			if nSamples := len(latencySamples); nSamples > 0 {
				sort.Float64s(latencySamples)
				computed.Max = latencySamples[nSamples-1]
				computed.Min = latencySamples[0]

				pi95idx := int(float64(len(latencySamples)) * 0.95)
				computed.Pi95 = latencySamples[pi95idx]

				pi50idx := int(float64(len(latencySamples)) * 0.50)
				computed.Pi50 = latencySamples[pi50idx]
			}

			printStats(computed)

			// clean collected stats
			stats = Stats{}
			latencySamples = latencySamples[:0]

		// handle new data
		case t, ok := <-timingsChan:
			if !ok {
				return nil
			}
			latencySamples = append(latencySamples, (t.copy).Seconds()*1000)
			stats.Ops += 1
			stats.Bytes += t.bytes
			stats.TotalCopy += t.copy
		}
	}
}

func printStats(stats ComputedStats) {
	stats = ComputedStats{
		Ts:   stats.Ts,
		Ops:  truncateToDecimals(stats.Ops),
		MBps: truncateToDecimals(stats.MBps),
		Avg:  truncateToDecimals(stats.Avg),
		Pi50: truncateToDecimals(stats.Pi50),
		Pi95: truncateToDecimals(stats.Pi95),
		Max:  truncateToDecimals(stats.Max),
		Min:  truncateToDecimals(stats.Min),
	}
	jsonBytes, _ := json.Marshal(stats)
	fmt.Println(string(jsonBytes))
}

func truncateToDecimals(f float64) float64 {
	return float64(int(f*100)) / 100
}
