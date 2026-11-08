// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command faultproxy serves the test-only fault-injecting proxy
// (internal/source/objhttp/faulttest) in front of a listing server, for
// benchmarks: throttle storms, latency inflation, connection resets and a
// rate cap, deterministic from a seed. On SIGINT or SIGTERM it stops
// accepting connections, waits up to -drain for the requests in flight to
// finish (and be recorded), writes the timeline (TSV, 100 ms buckets
// since the first request) and prints the totals.
//
// With -record FILE it also writes every exchange it forwards (and every
// answer it injects) to FILE as JSON lines (record.go), each before its
// answer is released to the client, and bounds the answer bodies it
// buffers by -record-max-body (a longer one is answered 502 and recorded
// as exceeded): run a client through it against a live provider endpoint
// (-target https://s3.REGION.amazonaws.com for path-style S3, or
// https://storage.googleapis.com) and replay the capture with
// replaydiff -capture. A request's X-Record-* headers annotate its
// exchange (replaydiff -live-record sends the case and how a token was
// formed): they are written with it and never forwarded.
//
//	go run ./bench/faultproxy -target http://127.0.0.1:19090 -listen 127.0.0.1:19091 \
//	  -window throttle:10s-25s:0.5 -timeline storm.tsv
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	record := flag.String("record", "", "write every exchange to this `file` as JSON lines")
	maxBody := flag.Int64("record-max-body", 16<<20, "with -record, the largest answer body in `bytes` buffered and recorded; a longer one is answered 502 and recorded without it")
	drain := flag.Duration("drain", 30*time.Second, "on SIGINT or SIGTERM, wait this long for requests in flight to finish")
	flag.Uint64Var(&plan.Seed, "seed", 1, "decision seed")
	flag.StringVar(&plan.Dialect, "dialect", "s3", "throttle answer: s3 (503 SlowDown) or gcs (429 rateLimitExceeded)")
	flag.Float64Var(&plan.RateCap, "rate-cap", 0, "throttle requests beyond `N` per trailing second (0: off)")
	flag.Var(&ws, "window", "FAULT:FROM-TO:FRACTION[:DELAY], repeatable; FAULT is throttle, latency or reset")
	flag.Parse()
	plan.Windows = ws
	if *record != "" {
		plan.MaxBody = *maxBody
	}
	p, err := faulttest.New(*target, plan)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	var h http.Handler = p
	var rec *recorder
	var f *os.File
	if *record != "" {
		if f, err = os.Create(*record); err != nil {
			log.Fatal(err)
		}
		rec = newRecorder(p, *target, f, *maxBody)
		h = rec
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	fmt.Fprintf(os.Stderr, "faultproxy %s -> %s plan %+v\n", ln.Addr(), *target, plan)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	stopErr := stop(srv, rec, f, *drain)
	if *timeline != "" {
		if err := os.WriteFile(*timeline, []byte(faulttest.TSV(p.Timeline())), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	s := p.Stats()
	fmt.Println(strings.TrimSpace(fmt.Sprintf("requests=%d throttled=%d reset=%d delayed=%d forwarded=%d max_active=%d",
		s.Requests, s.Throttled, s.Reset, s.Delayed, s.Forwarded, s.MaxActive)))
	if stopErr != nil {
		log.Fatalf("recording: %v", stopErr)
	}
}

// stop shuts srv down: no new connections, the requests in flight
// answered (or drain elapsed); then rec, if set, records what is still in
// flight (a reset outlives its connection) and f is synced and closed.
func stop(srv *http.Server, rec *recorder, f *os.File, drain time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	shut := srv.Shutdown(ctx)
	if rec == nil {
		return nil
	}
	err := rec.Close(ctx)
	if err == nil && shut != nil {
		err = fmt.Errorf("shutdown: %w", shut)
	}
	if e := f.Sync(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	return err
}
