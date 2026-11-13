// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// The load tests drive a server with many concurrent token walks for a
// few seconds each; SWATH_REPLAY_LOAD=1 runs them. With
// SWATH_REPLAY_FIXTURES=ROOT they serve ROOT/fixtures/synthetic-1m/
// sorted-rolled (bench/replay/fixtures), otherwise one written in the test
// of the same key shape.
const loadEnv = "SWATH_REPLAY_LOAD"

func loadGate(t *testing.T) {
	t.Helper()
	if os.Getenv(loadEnv) != "1" {
		t.Skipf("set %s=1 to run the load tests", loadEnv)
	}
}

// loadFixture opens the synthetic-1m rolled fixture when it is there, or
// writes 200,000 keys of its shape in 6 parts of 500-row groups.
func loadFixture(t *testing.T, readers int) *store.Sorted {
	t.Helper()
	if root := os.Getenv("SWATH_REPLAY_FIXTURES"); root != "" {
		dir := filepath.Join(root, "fixtures", "synthetic-1m", "sorted-rolled")
		if _, err := os.Stat(dir); err == nil {
			st, _, err := store.Open(context.Background(), dir, store.Options{Readers: readers})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			t.Logf("fixture %s", dir)
			return st.(*store.Sorted)
		}
	}
	st, _ := sortedFixture(t, logKeys(200_000), 34_000, 500, readers)
	t.Cleanup(func() { st.Close() })
	return st
}

// walkStart is the start-after of walker i of n: a day partition of the
// synthetic key shape, spread over the keyspace.
func walkStart(i, n int) string {
	names := []string{"alpha", "beta", "delta", "gamma"}
	slot := i * (4 * 12 * 28) / n
	return fmt.Sprintf("logs/%s/dt=2026-%02d-%02d/", names[slot/(12*28)], 1+slot/28%12, 1+slot%28)
}

var (
	keyTag   = regexp.MustCompile(`<Key>`)
	tokenTag = regexp.MustCompile(`<NextContinuationToken>([^<]*)</NextContinuationToken>`)
)

// walker is one client token walk.
type walker struct {
	client  *http.Client
	base    string // http://host:port/bucket
	maxKeys int
	// retried counts 503 answers retried after a pause.
	retried atomic.Int64
}

func newWalker(base string) *walker {
	tr := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 1024, DisableCompression: true}
	return &walker{client: &http.Client{Transport: tr, Timeout: 60 * time.Second}, base: base, maxKeys: 1000}
}

// walk lists from startAfter, w.maxKeys a page, until the listing ends, pages
// pages have been read or ctx ends, and returns the objects and pages.
// A 503 (a throttle, or a refusal at the response ceiling) is retried.
func (w *walker) walk(ctx context.Context, startAfter string, pages int) (objects, read int, err error) {
	token := ""
	for read < pages || pages <= 0 {
		q := fmt.Sprintf("list-type=2&max-keys=%d", w.maxKeys)
		if token != "" {
			q += "&continuation-token=" + url.QueryEscape(token)
		} else if startAfter != "" {
			q += "&start-after=" + url.QueryEscape(startAfter)
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", w.base+"?"+q, nil)
		resp, err := w.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return objects, read, nil
			}
			return objects, read, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if ctx.Err() != nil {
			return objects, read, nil
		}
		if err != nil {
			return objects, read, err
		}
		if resp.StatusCode == 503 {
			w.retried.Add(1)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if resp.StatusCode != 200 {
			return objects, read, fmt.Errorf("status %d: %s", resp.StatusCode, body)
		}
		read++
		objects += len(keyTag.FindAllIndex(body, -1))
		m := tokenTag.FindSubmatch(body)
		if m == nil {
			return objects, read, nil
		}
		token = string(m[1])
	}
	return objects, read, nil
}

// peak samples f every millisecond until stop is closed and returns the
// largest value seen.
func peak(stop <-chan struct{}, f func() int64) <-chan int64 {
	out := make(chan int64, 1)
	go func() {
		var top int64
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			top = max(top, f())
			select {
			case <-stop:
				out <- top
				return
			case <-tick.C:
			}
		}
	}()
	return out
}

func serveProxy(t *testing.T, s *Server, plan faulttest.Plan) (*faulttest.Proxy, string) {
	t.Helper()
	p, err := faulttest.New(fmt.Sprintf("http://127.0.0.1:%d", s.Port()), plan)
	if err != nil {
		t.Fatal(err)
	}
	ps := httptest.NewServer(p)
	t.Cleanup(ps.Close)
	return p, ps.URL
}

// TestLoadRequestCeilingUnderFaultProxy runs 32 walks through the fault
// proxy (a throttle window and a latency window) against a server whose
// request ceiling is 8 and whose pages take 50 ms: the proxy holds more
// than 8 requests at once, but the server never has more than 8 in its
// request path (the execution slots), the requests beyond them waiting
// before any server work, as in a full worker pool's connector queue, and
// every walk completes with every key. Then 16 clients that stop reading
// hold the 16 response permits (twice the ceiling), and a request through
// the proxy is refused 503 response_count_exhausted, counted, until they
// go away.
func TestLoadRequestCeilingUnderFaultProxy(t *testing.T) {
	loadGate(t)
	const n, walks, keys = MinMaxConcurrentRequests, 32, 20_000
	st, _ := sortedFixture(t, logKeys(keys), 5_000, 1_000, 4)
	cfg := config(st)
	cfg.MaxConcurrentRequests = n
	profile, err := latency.Parse("worker_page=50ms,pivot_probe=50ms,structure_probe=50ms", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Latency = profile
	s := start(t, cfg)
	proxy, base := serveProxy(t, s, faulttest.Plan{Seed: 7, Windows: []faulttest.Window{
		{From: 500 * time.Millisecond, To: 2 * time.Second, Fraction: 0.3, Fault: faulttest.Throttle},
		{From: 0, To: time.Hour, Fraction: 0.5, Fault: faulttest.Latency, Delay: 20 * time.Millisecond},
	}})
	stop := make(chan struct{})
	active := peak(stop, func() int64 { return s.s3Active.Load() })
	slots := peak(stop, func() int64 { return int64(len(s.slots)) })
	w := newWalker(base + "/bucket")
	var wg sync.WaitGroup
	errs := make(chan error, walks)
	started := time.Now()
	for i := range walks {
		wg.Go(func() {
			objects, _, err := w.walk(context.Background(), "", 0)
			if err == nil && objects != keys {
				err = fmt.Errorf("walk %d listed %d keys, want %d", i, objects, keys)
			}
			errs <- err
		})
	}
	wg.Wait()
	close(stop)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	maxActive, maxSlots := <-active, <-slots
	ps := proxy.Stats()
	t.Logf("%d walks of %d keys in %v: proxy %+v, server peak slots %d, peak active %d, retried %d",
		walks, keys, time.Since(started).Round(time.Millisecond), ps, maxSlots, maxActive, w.retried.Load())
	// A request holds its slot until its page is rendered and its delay
	// served, and stays active (protocol.requests.active) through its
	// write, so active requests are bounded by the response permits.
	if maxSlots > n || maxActive > int64(cfg.MaxResponses()) {
		t.Errorf("server peak slots %d over the ceiling of %d, or %d active over %d responses", maxSlots, n, maxActive, cfg.MaxResponses())
	}
	if maxSlots < n || ps.MaxActive <= n {
		t.Errorf("the ceiling was not reached (server %d slots, proxy %d in flight): the test did not load the server", maxSlots, ps.MaxActive)
	}
	if ps.Throttled == 0 || ps.Delayed == 0 {
		t.Errorf("the proxy injected no faults: %+v", ps)
	}
	if got := count(s, metrics.HTTPRequests); got != ps.Forwarded {
		t.Errorf("server answered %d requests, the proxy forwarded %d", got, ps.Forwarded)
	}
	if got := count(s, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted"); got > 0 {
		t.Errorf("%d refusals with readers that keep up", got)
	}

	// The response ceiling, through the proxy.
	if runtime.GOOS == "windows" {
		return
	}
	big := memstore.Keys(memstore.Options{}, spaceKeys(1000)...)
	bcfg := config(big)
	bcfg.MaxConcurrentRequests, bcfg.WriteTimeout = n, time.Minute
	bs := start(t, bcfg)
	_, bbase := serveProxy(t, bs, faulttest.Plan{Seed: 7, Windows: []faulttest.Window{
		{From: 0, To: time.Hour, Fraction: 0.5, Fault: faulttest.Latency, Delay: 20 * time.Millisecond},
	}})
	var conns []net.Conn
	for range bcfg.MaxResponses() {
		conns = append(conns, slowReader(t, bs, bigPage))
	}
	waitFor(t, "every response permit held by a stalled reader", func() bool {
		return bs.responses.Load() == int64(bcfg.MaxResponses()) && len(bs.slots) == 0
	})
	resp, err := http.Get(bbase + "/bucket?list-type=2&max-keys=1")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("x-swath-replay-error") != "response_count_exhausted" {
		t.Errorf("through the proxy at the response ceiling: %d %v", resp.StatusCode, resp.Header)
	}
	if count(bs, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted") != 1 {
		t.Errorf("refusal not counted: %s", bs.reg.AppendMeters(nil))
	}
	for _, c := range conns {
		c.Close()
	}
	waitFor(t, "the permits to come back", func() bool { return bs.responses.Load() == 0 })
	if objects, _, err := newWalker(bbase+"/bucket").walk(context.Background(), "", 0); err != nil || objects != 1000 {
		t.Errorf("walk after the readers left: %d keys, %v", objects, err)
	}
}

// TestLoadReadersBoundPageDecodes runs 256 concurrent token walks from
// spread start positions against a store with 8 reader slots: the reads
// holding a slot (parquet.queries.in_flight, sampled, and its peak) never
// exceed 8 and reach it.
func TestLoadReadersBoundPageDecodes(t *testing.T) {
	loadGate(t)
	const readers, walks = 8, 256
	pages := 8
	if raceEnabled {
		pages = 2
	}
	st := loadFixture(t, readers)
	s := start(t, config(st))
	stop := make(chan struct{})
	inFlight := peak(stop, func() int64 { return st.Counters().InFlightReaders.Load() })
	w := newWalker(fmt.Sprintf("http://127.0.0.1:%d/bucket", s.Port()))
	var wg sync.WaitGroup
	var objects atomic.Int64
	started := time.Now()
	for i := range walks {
		wg.Go(func() {
			n, _, err := w.walk(context.Background(), walkStart(i, walks), pages)
			if err != nil {
				t.Error(err)
			}
			objects.Add(int64(n))
		})
	}
	wg.Wait()
	close(stop)
	sampled := <-inFlight
	elapsed := time.Since(started)
	t.Logf("%d walks x %d pages: %d objects in %v (%.0f rows/s), sampled in-flight peak %d, peak %d",
		walks, pages, objects.Load(), elapsed.Round(time.Millisecond), float64(objects.Load())/elapsed.Seconds(),
		sampled, st.Counters().PeakReaders.Load())
	if p := st.Counters().PeakReaders.Load(); p != readers || sampled > readers {
		t.Errorf("reader peak %d, sampled %d, want exactly the bound %d", p, sampled, readers)
	}
	if !strings.Contains(string(s.Snapshot(time.Now())), fmt.Sprintf(`"swath.replay.parquet.queries.peak","type":"gauge","tags":{},"value":%d.000000`, readers)) {
		t.Error("parquet.queries.peak does not report the bound")
	}
}

// TestLoadNoOverrunAtSixtyFourWalks: 64 walks of 100-key pages against a
// 50 ms worker_page profile, for five seconds, never overrun it: the
// server's own cost stays inside the injected latency, so the clients
// observe the profile. The pages are 100 keys so that the load (128,000
// rows a second) is within a small shared host's capacity with the client
// in the same process; docs/internals/swath-replay.md has the 1,000-key measurement. Not
// run under the race detector, which multiplies the server's cost.
func TestLoadNoOverrunAtSixtyFourWalks(t *testing.T) {
	loadGate(t)
	if raceEnabled {
		t.Skip("the race detector's cost is not the server's")
	}
	st := loadFixture(t, 0)
	cfg := config(st)
	profile, err := latency.Parse("worker_page=50ms", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Latency = profile
	s := start(t, cfg)
	w := newWalker(fmt.Sprintf("http://127.0.0.1:%d/bucket", s.Port()))
	w.maxKeys = 100
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			if _, _, err := w.walk(ctx, walkStart(i, 64), 0); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	pages := count(s, metrics.RequestLatency, "worker_page")
	overruns := count(s, metrics.InjectOverrun, "worker_page")
	t.Logf("%d worker pages, %d overruns", pages, overruns)
	if pages < 64*50 {
		t.Errorf("%d pages in five seconds: the walks did not keep 64 requests in flight", pages)
	}
	if overruns > 0 {
		t.Errorf("%d of %d pages overran 50 ms: %s", overruns, pages, s.reg.AppendMeters(nil))
	}
}
