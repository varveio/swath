// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"time"

	"github.com/varveio/swath/internal/shapes"
	"github.com/varveio/swath/internal/source/scalefake"
)

const ms = time.Millisecond

// arm is one row of the suite: a listing of a shape behind a service.
type arm struct {
	name, shape string
	workers     int
	fake        scalefake.Options
	flags       []string
	parquet     bool          // write a dataset and compare its keys with the shape's
	rtt         time.Duration // the ideal's page latency where the fake's is not it
}

// suite is every arm, grouped by shape so each shape is generated once.
func suite() []arm {
	at := func(d time.Duration) scalefake.Options { return scalefake.Options{Latency: d} }
	var a []arm
	// Every shape at 50 ms and 64 workers, counted.
	for _, s := range shapes.All {
		a = append(a, arm{name: s.Name, shape: s.Name, workers: 64, fake: at(50 * ms)})
	}
	// The latency ladder.
	for _, s := range []string{"baseline", "counter"} {
		a = append(a, arm{name: s + ".d10", shape: s, workers: 64, fake: at(10 * ms)})
	}
	for _, s := range []string{"baseline", "counter", "date_hot", "zipf_folders", "chain50", "maxlen", "hex_flat", "wide_top",
		"onekey_folders", "percent_keys", "trailing_variants", "tiny100", "small10k"} {
		a = append(a, arm{name: s + ".d200", shape: s, workers: 64, fake: at(200 * ms)})
	}
	// Wide runs and short pages.
	for _, s := range []string{"baseline", "counter", "hex_flat", "onekey_folders", "wide_top"} {
		a = append(a, arm{name: s + ".w256", shape: s, workers: 256, fake: at(50 * ms)})
	}
	for _, s := range []string{"baseline", "counter"} {
		f := at(50 * ms)
		f.PageCap = 100
		a = append(a, arm{name: "pagecap100." + s, shape: s, workers: 64, fake: f})
	}
	// Service behaviours on the reference shape at 10, 50 and 200 ms: a
	// jittered latency (uniform over [d/2, 10d]), 1 % and 5 % of requests
	// throttled (with and without the limiter), a request-rate cap, a slow
	// prefix at 10d (half the keys, or one folder of 200), slow
	// continuations and seeks (4d more), and a slow first page (100d).
	for _, d := range []time.Duration{10 * ms, 50 * ms, 200 * ms} {
		sfx := fmt.Sprintf(".d%d", d/ms)
		b := func(name string, f func(*scalefake.Options), rtt time.Duration, flags ...string) {
			o := at(d)
			f(&o)
			a = append(a, arm{name: name + sfx, shape: "baseline", workers: 64, fake: o, rtt: rtt, flags: flags})
		}
		b("jitter", func(o *scalefake.Options) { o.Latency, o.Jitter = d/2, 10*d-d/2 }, 0)
		b("throttle1", func(o *scalefake.Options) { o.Fail503, o.FaultSeed = 0.01, 1 }, 0)
		b("throttle5", func(o *scalefake.Options) { o.Fail503, o.FaultSeed = 0.05, 1 }, 0)
		b("throttle5.nolimiter", func(o *scalefake.Options) { o.Fail503, o.FaultSeed = 0.05, 1 }, 0, "--no-adaptive-limit")
		b("rate300", func(o *scalefake.Options) { o.Rate = 300 }, 0)
		b("slowprefix.half", func(o *scalefake.Options) { o.Slow = []scalefake.Slow{{Prefix: "d/0", Latency: 10 * d}} }, d*55/10)
		b("slowprefix.narrow", func(o *scalefake.Options) { o.Slow = []scalefake.Slow{{Prefix: "d/000", Latency: 10 * d}} }, 0)
		b("tokenslow", func(o *scalefake.Options) { o.TokenLatency = 4 * d }, 0)
		b("startslow", func(o *scalefake.Options) { o.StartLatency = 4 * d }, 0)
		b("firstslow", func(o *scalefake.Options) { o.FirstLatency = 100 * d }, 0)
	}
	a = append(a,
		arm{name: "tokenslow.counter.d50", shape: "counter", workers: 64, fake: scalefake.Options{Latency: 50 * ms, TokenLatency: 200 * ms}},
		arm{name: "throttle5.w16.d50", shape: "baseline", workers: 16, fake: scalefake.Options{Latency: 50 * ms, Fail503: 0.05, FaultSeed: 1}},
		arm{name: "tiny100.noseed", shape: "tiny100", workers: 64, fake: at(50 * ms), flags: []string{"--no-seed"}},
		arm{name: "tiny1k.noseed", shape: "tiny1k", workers: 64, fake: at(50 * ms), flags: []string{"--no-seed"}},
		arm{name: "slowsplit.slowprefix.narrow.d200", shape: "baseline", workers: 64, flags: []string{"--slow-split"},
			fake: scalefake.Options{Latency: 200 * ms, Slow: []scalefake.Slow{{Prefix: "d/000", Latency: 2 * time.Second}}}},
		arm{name: "slowsplit.zipf_folders.d200", shape: "zipf_folders", workers: 64, fake: at(200 * ms), flags: []string{"--slow-split"}},
	)
	// Parquet: the dataset is the shape's keys, byte for byte.
	for _, s := range []string{"baseline", "highbytes", "maxlen", "percent_keys", "punct_siblings", "trailing_variants",
		"folder_objects", "counter_unpadded", "zipf_folders"} {
		a = append(a, arm{name: "pq." + s, shape: s, workers: 64, fake: at(50 * ms), parquet: true})
	}
	return a
}
