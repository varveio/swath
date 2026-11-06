// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command faultproxy serves the test-only fault-injecting proxy
// (internal/source/objhttp/faulttest) in front of a listing server, for
// benchmarks: throttle storms, latency inflation, connection resets and a
// rate cap, deterministic from a seed. On SIGINT or SIGTERM it writes the
// timeline (TSV, 100 ms buckets since the first request) and prints the
// totals.
//
//	go run ./bench/faultproxy -target http://127.0.0.1:19090 -listen 127.0.0.1:19091 \
//	  -window throttle:10s-25s:0.5 -timeline storm.tsv
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

type windows []faulttest.Window

func (w *windows) String() string { return fmt.Sprint(*w) }
func (w *windows) Set(s string) error {
	x, err := faulttest.ParseWindow(s)
	if err == nil {
		*w = append(*w, x)
	}
	return err
}

func main() {
	var plan faulttest.Plan
	var ws windows
	target := flag.String("target", "", "listing server `URL` (http://host:port)")
	listen := flag.String("listen", "127.0.0.1:19091", "listen address")
	timeline := flag.String("timeline", "", "write the timeline TSV to this `file` on exit")
	flag.Uint64Var(&plan.Seed, "seed", 1, "decision seed")
	flag.StringVar(&plan.Dialect, "dialect", "s3", "throttle answer: s3 (503 SlowDown) or gcs (429 rateLimitExceeded)")
	flag.Float64Var(&plan.RateCap, "rate-cap", 0, "throttle requests beyond `N` per trailing second (0: off)")
	flag.Var(&ws, "window", "FAULT:FROM-TO:FRACTION[:DELAY], repeatable; FAULT is throttle, latency or reset")
	flag.Parse()
	plan.Windows = ws
	p, err := faulttest.New(*target, plan)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	go func() { _ = http.Serve(ln, p) }()
	fmt.Fprintf(os.Stderr, "faultproxy %s -> %s plan %+v\n", ln.Addr(), *target, plan)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	if *timeline != "" {
		if err := os.WriteFile(*timeline, []byte(faulttest.TSV(p.Timeline())), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	s := p.Stats()
	fmt.Println(strings.TrimSpace(fmt.Sprintf("requests=%d throttled=%d reset=%d delayed=%d forwarded=%d max_active=%d",
		s.Requests, s.Throttled, s.Reset, s.Delayed, s.Forwarded, s.MaxActive)))
}
