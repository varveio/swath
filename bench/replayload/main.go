// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command replayload drives a listing server with concurrent S3 token
// walks for a fixed time and reports the object rows served per second,
// for replay server throughput and resource measurements. Each walk starts
// at its own place in the keyspace (a start-after spread over the fixture's
// row groups when -fixture names it, else the start of the listing), pages
// with max-keys, and starts over when its listing ends.
//
//	go run ./bench/replayload -endpoint http://127.0.0.1:19090 -bucket synth \
//	  -fixture /data/synthetic-1m/sorted-rolled -walks 64 -duration 30s -warmup 10s
//
// The one-line result (walks, seconds, requests, objects, rows/s, the
// request latency percentiles, retried 503s, errors) goes to stdout; with
// -metrics the server's swath.replay.inject.overrun and
// parquet.queries.peak are read at the end.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/replay/store"
)

func main() {
	endpoint := flag.String("endpoint", "", "server `URL` (http://host:port)")
	bucket := flag.String("bucket", "", "bucket name")
	fixture := flag.String("fixture", "", "the served fixture, to spread the walks' start positions over its row groups")
	walks := flag.Int("walks", 16, "concurrent token walks")
	maxKeys := flag.Int("max-keys", 1000, "max-keys of every page")
	duration := flag.Duration("duration", 30*time.Second, "measured time")
	warmup := flag.Duration("warmup", 0, "time walked before the measured time, not counted")
	metricsURL := flag.String("metrics", "", "the server's metrics `URL`, read at the end")
	label := flag.String("label", "", "a label printed with the result")
	flag.Parse()
	if *endpoint == "" || *bucket == "" || *walks < 1 {
		flag.Usage()
		os.Exit(2)
	}
	starts := make([]string, *walks)
	if *fixture != "" {
		var err error
		if starts, err = spread(*fixture, *walks); err != nil {
			log.Fatal(err)
		}
	}
	base := *endpoint + "/" + *bucket + "?list-type=2&max-keys=" + fmt.Sprint(*maxKeys)
	client := &http.Client{Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: *walks * 2, DisableCompression: true}}

	var counting atomic.Bool
	var requests, objects, retried, errs atomic.Int64
	lat := make([][]time.Duration, *walks)
	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*duration)
	defer cancel()
	time.AfterFunc(*warmup, func() { counting.Store(true) })
	measured := time.Now().Add(*warmup)
	var wg sync.WaitGroup
	for i := range *walks {
		wg.Go(func() {
			token, startAfter := "", starts[i]
			for ctx.Err() == nil {
				target := base
				if token != "" {
					target += "&continuation-token=" + url.QueryEscape(token)
				} else if startAfter != "" {
					target += "&start-after=" + url.QueryEscape(startAfter)
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
						log.Print(err)
						time.Sleep(100 * time.Millisecond)
					}
					continue
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				took := time.Since(t0)
				if resp.StatusCode == 503 {
					retried.Add(1)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if resp.StatusCode != 200 {
					errs.Add(1)
					log.Printf("status %d: %.200s", resp.StatusCode, body)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if counting.Load() {
					requests.Add(1)
					objects.Add(int64(bytes.Count(body, []byte("<Contents>"))))
					lat[i] = append(lat[i], took)
				}
				token = between(body, "<NextContinuationToken>", "</NextContinuationToken>")
				if token == "" {
					startAfter = "" // the listing ended: start over from the beginning
				}
			}
		})
	}
	wg.Wait()
	secs := time.Since(measured).Seconds()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	slices.Sort(all)
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[min(len(all)-1, int(p*float64(len(all))))].Microseconds()) / 1000
	}
	fmt.Printf("label=%s walks=%d max_keys=%d seconds=%.1f requests=%d objects=%d rows_per_s=%.0f p50_ms=%.1f p99_ms=%.1f retried_503=%d errors=%d",
		*label, *walks, *maxKeys, secs, requests.Load(), objects.Load(), float64(objects.Load())/secs, pct(0.5), pct(0.99),
		retried.Load(), errs.Load())
	if *metricsURL != "" {
		fmt.Print(serverMeters(*metricsURL))
	}
	fmt.Println()
}

// between returns the text of b between the first open and the close after
// it, "" when absent.
func between(b []byte, open, close string) string {
	i := bytes.Index(b, []byte(open))
	if i < 0 {
		return ""
	}
	rest := b[i+len(open):]
	j := bytes.Index(rest, []byte(close))
	if j < 0 {
		return ""
	}
	return string(rest[:j])
}

// spread returns n start-after keys over the fixture's routing keys (the
// first key of every row group), in walk order: walk i starts in group
// i*groups/n, so walks share a group only when there are fewer groups.
func spread(fixture string, n int) ([]string, error) {
	st, _, err := store.Open(context.Background(), fixture, store.Options{Readers: 1})
	if err != nil {
		return nil, err
	}
	defer st.Close()
	keys := st.(*store.Sorted).FirstKeys()
	out := make([]string, n)
	for i := range out {
		out[i] = string(keys[i*len(keys)/n])
	}
	out[0] = "" // the first walk starts at the beginning
	return out, nil
}

// serverMeters reads the overrun counts and the reader peak from a scrape.
func serverMeters(u string) string {
	resp, err := http.Get(u)
	if err != nil {
		return " metrics_error=" + err.Error()
	}
	defer resp.Body.Close()
	var p struct {
		Meters []struct {
			Name  string            `json:"name"`
			Tags  map[string]string `json:"tags"`
			Count *float64          `json:"count"`
			Value *float64          `json:"value"`
		} `json:"meters"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return " metrics_error=" + err.Error()
	}
	var overruns, requests, peak float64
	for _, m := range p.Meters {
		switch m.Name {
		case "swath.replay.inject.overrun":
			overruns += *m.Count
		case "swath.replay.http.requests":
			requests += *m.Count
		case "swath.replay.parquet.queries.peak":
			peak = *m.Value
		}
	}
	return fmt.Sprintf(" server_requests=%.0f inject_overruns=%.0f parquet_queries_peak=%.0f", requests, overruns, peak)
}
