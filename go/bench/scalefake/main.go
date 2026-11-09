// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command scalefake serves a keyspace (package internal/source/scalefake)
// as an S3 and GCS endpoint for runs of the binary: a lazy synthetic one,
// nothing materialized, so 1B keys need no disk, or a sorted key file
// (-key-file, one key per line). The service behaviours are off unless
// asked for: every listing request then waits -latency.
//
//	go run ./bench/scalefake -addr 127.0.0.1:19500 -keys 1000000000 -fanout 32,32,32,8 -skew 1
//	go run ./bench/scalefake -key-file keys.txt -latency 50ms -fail503 0.01 -slow d/000=500ms
//	swath list s3://bkt --endpoint http://127.0.0.1:19500 --path-style --anonymous --region us-east-1 -o OUT
//	swath list gs://bkt --gcs-endpoint http://127.0.0.1:19500/storage/v1/ --anonymous -o OUT
//
// GET /__scalefake/stats is the accounting (requests, pages, keys and
// prefixes served, throttled, in flight now and at peak, the busy time
// whose rate is the mean in flight; ?reset=1 starts a new window for
// window_peak_inflight); GET /__scalefake/count?prefix=P counts keys.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/source/scalefake"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19500", "listen address")
	bucket := flag.String("bucket", "bkt", "bucket name")
	var s scalefake.Shape
	flag.Int64Var(&s.Keys, "keys", 1_000_000, "keys in the keyspace")
	fanout := flag.String("fanout", "16,16,16", "children per directory level, root first (comma-separated); empty: flat")
	flag.Float64Var(&s.Skew, "skew", 0, "sigma of the hierarchical log-normal density (0: uniform)")
	flag.IntVar(&s.SharedPrefix, "shared-prefix", 0, "length of a root prefix every key shares")
	flag.IntVar(&s.SegmentPad, "segment-pad", 0, "length of a shared run at the start of every directory segment")
	flag.IntVar(&s.KeyLen, "key-len", 0, "stretch leaf names so every key is at least this long (1024: 1 KiB keys)")
	flag.Uint64Var(&s.Seed, "seed", 1, "keyspace seed")
	keyFile := flag.String("key-file", "", "serve this sorted key `file` (one key per line, ascending as bytes) instead of a generated keyspace")
	var o scalefake.Options
	flag.DurationVar(&o.Latency, "latency", 0, "delay before every listing answer")
	flag.DurationVar(&o.Jitter, "jitter", 0, "plus a uniform delay in [0, jitter)")
	flag.Func("slow", "per-prefix latency, `prefix=duration[,prefix=duration...]`: the longest prefix matching the key a page starts at replaces -latency", func(v string) error {
		for _, e := range strings.Split(v, ",") {
			i := strings.LastIndexByte(e, '=')
			if i < 0 {
				return fmt.Errorf("%q: want prefix=duration", e)
			}
			d, err := time.ParseDuration(e[i+1:])
			if err != nil {
				return err
			}
			o.Slow = append(o.Slow, scalefake.Slow{Prefix: e[:i], Latency: d})
		}
		return nil
	})
	flag.DurationVar(&o.FirstLatency, "first-latency", 0, "the first listing request waits this instead of -latency")
	flag.DurationVar(&o.TokenLatency, "token-latency", 0, "extra delay for listing requests with a continuation token")
	flag.DurationVar(&o.StartLatency, "start-latency", 0, "extra delay for listing requests without a continuation token")
	flag.IntVar(&o.PageCap, "page-cap", 0, "answer at most `N` items per page, whatever the request asks (0: as asked, at most 1000)")
	flag.Float64Var(&o.Rate, "rate", 0, "cap listing requests at `N` per second (burst N/10, at least 1); past it a request is throttled at once")
	flag.Float64Var(&o.Fail503, "fail503", 0, "fraction of listing requests answered 503")
	flag.Uint64Var(&o.FaultSeed, "fault-seed", 1, "seed of the jitter and 503 draws")
	logFile := flag.String("log", "", "write one line per listing request to this file")
	flag.Parse()
	for _, f := range strings.Split(*fanout, ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			log.Fatalf("-fanout: %v", err)
		}
		s.Fanout = append(s.Fanout, n)
	}
	start := time.Now()
	k, err := keyspace(s, *keyFile)
	if err != nil {
		log.Fatal(err)
	}
	if *logFile != "" {
		f, err := os.Create(*logFile)
		if err != nil {
			log.Fatal(err)
		}
		w := bufio.NewWriterSize(f, 1<<20)
		o.Log = &flushing{w: w}
	}
	o.Bucket = *bucket
	srv := scalefake.NewServer(k, o)
	fmt.Printf("scalefake: %s, %d leaf directories, built in %s; serving s3://%s and gs://%s on http://%s\n",
		k, k.Dirs(), time.Since(start).Round(time.Millisecond), *bucket, *bucket, *addr)
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(hs.ListenAndServe())
}

func keyspace(s scalefake.Shape, file string) (*scalefake.Keyspace, error) {
	if file == "" {
		return scalefake.New(s)
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return scalefake.ReadKeys(f)
}

// flushing writes through a buffer and flushes it at most once a second.
type flushing struct {
	w    *bufio.Writer
	last time.Time
}

func (f *flushing) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if now := time.Now(); now.Sub(f.last) > time.Second {
		f.last = now
		err = f.w.Flush()
	}
	return n, err
}
