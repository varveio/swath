// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// hookStore runs hook before every Rows call of the store it wraps.
type hookStore struct {
	store.Store
	hook   func(ctx context.Context) error
	closed atomic.Bool
}

func (h *hookStore) Rows(ctx context.Context, from []byte, inc bool, to []byte, limit int, p store.Projection) ([]store.Object, error) {
	if h.hook != nil {
		if err := h.hook(ctx); err != nil {
			return nil, err
		}
	}
	return h.Store.Rows(ctx, from, inc, to, limit, p)
}

func (h *hookStore) Close() error { h.closed.Store(true); return h.Store.Close() }

func config(st store.Store) Config {
	return Config{
		Bucket: "bucket", Host: "127.0.0.1", MetricsPort: 0, MaxConcurrentRequests: DefaultMaxConcurrentRequests,
		ResponseBufferBudget: DefaultResponseBufferBudget, MaxResponseBytes: DefaultMaxResponseBytes,
		StopTimeout: DefaultStopTimeout, IdleTimeout: DefaultIdleTimeout, WriteTimeout: DefaultWriteTimeout,
		SlowRequestLog: -1, Store: st, ServingMode: "SORTED", FixtureIdentity: "test",
	}
}

func start(t *testing.T, cfg Config) *Server {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	t.Cleanup(func() { s.Shutdown(time.Second) })
	return s
}

type response struct {
	status int
	header http.Header
	body   string
}

func get(t *testing.T, s *Server, target string) response {
	t.Helper()
	return do(t, "GET", fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), target))
}

func do(t *testing.T, method, url string) response {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{resp.StatusCode, resp.Header, string(b)}
}

func count(s *Server, d metrics.Def, tags ...string) int64 {
	if m := s.reg.Find(d, tags...); m != nil {
		return m.Count()
	}
	return -1
}

func TestServesPathStyleListObjectsV2(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a/1", "b/1")))
	r := get(t, s, "/bucket?list-type=2&prefix=a%2F&encoding-type=url")
	if r.status != 200 || r.header.Get("Content-Type") != "application/xml" || r.header.Get("x-amz-request-id") != "S3LISTINGREPLAY" ||
		r.header.Get("x-amz-id-2") != "S3LISTINGREPLAY" || r.header.Get("x-swath-replay-error") != "" {
		t.Fatalf("%d %v", r.status, r.header)
	}
	for _, want := range []string{"<Name>bucket</Name>", "<Prefix>a/</Prefix>", "<Key>a/1</Key>", "<KeyCount>1</KeyCount>"} {
		if !strings.Contains(r.body, want) {
			t.Errorf("missing %s: %s", want, r.body)
		}
	}
	if count(s, metrics.HTTPRequests) != 1 || count(s, metrics.ProtocolHTTPRequests, "s3", "2xx") != 1 ||
		count(s, metrics.ProtocolObjects, "s3", "page") != 1 || count(s, metrics.ProtocolPrefixes, "s3", "page") != 0 ||
		count(s, metrics.ProtocolEncodedBytes, "s3", "page") != int64(len(r.body)) ||
		count(s, metrics.RequestLatency, "worker_page") != 1 || count(s, metrics.ServingPath, "sorted") != 1 {
		t.Errorf("meters: %s", s.reg.AppendMeters(nil))
	}
}

func TestErrorsAreS3Answers(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	for _, c := range []struct {
		method, target string
		status         int
		code, details  string
	}{
		{"GET", "/other?list-type=2", 404, "NoSuchBucket", "<BucketName>other</BucketName>"},
		{"GET", "/b%75cket?list-type=2", 404, "NoSuchBucket", "<BucketName>bucket</BucketName>"},
		{"GET", "/?list-type=2", 404, "NoSuchBucket", ""},
		{"GET", "/bucket?max-keys=1", 400, "InvalidArgument", "<ArgumentName>list-type</ArgumentName>"},
		{"GET", "/bucket?list-type=2&continuation-token=zz", 400, "InvalidArgument", "<ArgumentName>continuation-token</ArgumentName>"},
		{"POST", "/bucket?list-type=2", 405, "MethodNotAllowed", "<Method>POST</Method><ResourceType>BUCKET</ResourceType>"},
	} {
		r := do(t, c.method, fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), c.target))
		if r.status != c.status || !strings.Contains(r.body, "<Code>"+c.code+"</Code>") ||
			!strings.Contains(r.body, "</Message>"+c.details+"<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>") ||
			r.header.Get("x-amz-request-id") != "S3LISTINGREPLAY" || r.header.Get("Content-Type") != "application/xml" {
			t.Errorf("%s %s: %d %v %s", c.method, c.target, r.status, r.header, r.body)
		}
	}
	// HEAD answers the 405 with its length and no body.
	r := do(t, "HEAD", fmt.Sprintf("http://127.0.0.1:%d/bucket?list-type=2", s.Port()))
	if r.status != 405 || r.body != "" || r.header.Get("Content-Length") == "" {
		t.Errorf("HEAD: %d %v %q", r.status, r.header, r.body)
	}
	if count(s, metrics.ProtocolHTTPRequests, "s3", "4xx") != 7 {
		t.Errorf("4xx count %d", count(s, metrics.ProtocolHTTPRequests, "s3", "4xx"))
	}
}

func TestStoreFailureCarriesNoDetail(t *testing.T) {
	secret := "/srv/captures/private/part-00001.parquet at key secret/customer"
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error { return errors.New(secret) }}
	cfg := config(st)
	var log strings.Builder
	cfg.Log = &syncWriter{w: &log}
	s := start(t, cfg)
	r := get(t, s, "/bucket?list-type=2")
	if r.status != 500 || !strings.Contains(r.body, "<Code>InternalError</Code>") || !strings.Contains(r.body, "<Message>internal replay error</Message>") ||
		strings.Contains(r.body, "secret") || r.header.Get("x-swath-replay-error") != "internal" {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
	if count(s, metrics.HTTPErrors) != 1 || count(s, metrics.ProtocolHTTPRequests, "s3", "5xx") != 1 || count(s, metrics.ProtocolObjects, "s3", "page") != -1 {
		t.Errorf("meters: %s", s.reg.AppendMeters(nil))
	}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// String reads what was written, under the same lock.
func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.(fmt.Stringer).String()
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestEchoesMaxKeysButClampsThePage(t *testing.T) {
	keys := make([]string, 1500)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%05d", i)
	}
	s := start(t, config(memstore.Keys(memstore.Options{}, keys...)))
	r := get(t, s, "/bucket?list-type=2&max-keys=5000")
	if !strings.Contains(r.body, "<MaxKeys>5000</MaxKeys>") || !strings.Contains(r.body, "<KeyCount>1000</KeyCount>") ||
		!strings.Contains(r.body, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("%.400s", r.body)
	}
}

func TestConcurrentRequests(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a/1")))
	var wg sync.WaitGroup
	var bad atomic.Int32
	for range 64 {
		wg.Go(func() {
			if r := get(t, s, "/bucket?list-type=2"); r.status != 200 {
				bad.Add(1)
			}
		})
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Errorf("%d requests failed", bad.Load())
	}
}

// The request ceiling queues: with every slot held, one more request waits
// rather than being refused, and is served when a slot frees.
func TestRequestCeilingQueues(t *testing.T) {
	release := make(chan struct{})
	var entered atomic.Int32
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error {
		entered.Add(1)
		<-release
		return nil
	}}
	cfg := config(st)
	cfg.MaxConcurrentRequests = MinMaxConcurrentRequests
	s := start(t, cfg)
	results := make(chan int, 9)
	for range 9 {
		go func() { results <- get(t, s, "/bucket?list-type=2").status }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if n := entered.Load(); n != 8 {
		t.Fatalf("%d requests reached the store, want the ceiling of 8", n)
	}
	close(release)
	for range 9 {
		if st := <-results; st != 200 {
			t.Errorf("status %d", st)
		}
	}
}

// spaceKeys are n keys of about 1 KiB that encoding-type=url triples
// ('~' is %7E), so a max-keys=1000 page is about 3 MB.
func spaceKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%04d~%s", i, strings.Repeat("~", 1000))
	}
	return keys
}

const bigPage = "/bucket?list-type=2&max-keys=1000&encoding-type=url"

// slowReader opens a connection with a small receive buffer, sends one
// request and never reads the answer, so the server's write blocks.
func slowReader(t *testing.T, s *Server, target string) net.Conn {
	t.Helper()
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		c.Control(func(fd uintptr) { serr = setReceiveBuffer(fd, 4096) })
		return serr
	}}
	conn, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: replay\r\n\r\n", target); err != nil {
		t.Fatal(err)
	}
	return conn
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSlowReadersReachTheResponseCeiling: clients that stop reading hold
// their responses (count and bytes) but not their execution slots, so with
// a request ceiling of 8 there can be 16 outstanding responses, and the
// next request is answered 503 response_count_exhausted at once rather
// than queued behind the stalled writes. When the readers go away every
// permit and byte comes back.
func TestSlowReadersReachTheResponseCeiling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("receive buffer sizing")
	}
	cfg := config(memstore.Keys(memstore.Options{}, spaceKeys(1000)...))
	cfg.MaxConcurrentRequests = MinMaxConcurrentRequests
	cfg.WriteTimeout = time.Minute
	s := start(t, cfg)
	var conns []net.Conn
	for range cfg.MaxResponses() {
		conns = append(conns, slowReader(t, s, bigPage))
	}
	// Every response is admitted and writing, and no execution slot is
	// held by a response that is only writing.
	waitFor(t, "16 outstanding responses holding no execution slot", func() bool {
		return s.responses.Load() == int64(cfg.MaxResponses()) && len(s.slots) == 0
	})
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/bucket?list-type=2&max-keys=1", s.Port()))
	if err != nil {
		t.Fatalf("the request past the ceiling was not answered: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("x-swath-replay-error") != "response_count_exhausted" ||
		!strings.Contains(string(body), "<Message>response buffer budget exhausted</Message>") {
		t.Errorf("%d %v %s", resp.StatusCode, resp.Header, body)
	}
	if count(s, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted") != 1 {
		t.Errorf("refusal meters: %s", s.reg.AppendMeters(nil))
	}
	for _, c := range conns {
		c.Close()
	}
	waitFor(t, "the permits and bytes to come back", func() bool { return s.responses.Load() == 0 && s.budget.Charged() == 0 })
	if r := get(t, s, "/bucket?list-type=2&max-keys=1"); r.status != 200 {
		t.Errorf("after the readers left: %d", r.status)
	}
}

// TestSparseResponsesReturnTheirReservation is the review's scenario: with
// a 400,000-byte budget, a max-keys=1000 request reserves 320,512 bytes
// before paging, but a one-object page renders into one 4 KiB chunk and
// gives the rest back before its injected delay, so an overlapping request
// fits.
func TestSparseResponsesReturnTheirReservation(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, "a"))
	cfg.ResponseBufferBudget, cfg.MaxResponseBytes = 400_000, 400_000
	cfg.Latency, _ = latency.Parse("worker_page=600ms", 0, 1)
	s := start(t, cfg)
	results := make(chan response, 2)
	go func() { results <- get(t, s, "/bucket?list-type=2&max-keys=1000") }()
	waitFor(t, "the first request's delay", func() bool { return s.responses.Load() == 1 && s.budget.Charged() == 4096 })
	go func() { results <- get(t, s, "/bucket?list-type=2&max-keys=1000") }()
	for range 2 {
		if r := <-results; r.status != 200 {
			t.Errorf("%d %v", r.status, r.header)
		}
	}
	if s.budget.Peak() != 4096+320_512 {
		t.Errorf("peak %d", s.budget.Peak())
	}
}

// TestBudgetRefusesAnOverlappingPage: a response holding its rendered
// chunks through its delay leaves too little budget for the next
// request's reservation, which is refused 503 response_budget_exhausted;
// once it is written the budget is whole again.
func TestBudgetRefusesAnOverlappingPage(t *testing.T) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("%04d/%s", i, strings.Repeat("k", 300))
	}
	cfg := config(memstore.Keys(memstore.Options{}, keys...))
	cfg.ResponseBufferBudget, cfg.MaxResponseBytes = 600_000, 600_000
	cfg.Latency, _ = latency.Parse("worker_page=600ms", 0, 1)
	s := start(t, cfg)
	first := make(chan response, 1)
	go func() { first <- get(t, s, "/bucket?list-type=2&max-keys=1000") }()
	waitFor(t, "the first request's delay", func() bool { return s.responses.Load() == 1 && s.budget.Charged() > 300_000 })
	r := get(t, s, "/bucket?list-type=2&max-keys=1000")
	if r.status != 503 || r.header.Get("x-swath-replay-error") != "response_budget_exhausted" {
		t.Errorf("overlapping: %d %v", r.status, r.header)
	}
	if r := <-first; r.status != 200 {
		t.Errorf("first: %d", r.status)
	}
	if count(s, metrics.ResponseAdmissionRefused, "s3", "response_budget_exhausted") != 1 {
		t.Errorf("refusal meters: %s", s.reg.AppendMeters(nil))
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
	if r := get(t, s, "/bucket?list-type=2&max-keys=10"); r.status != 200 {
		t.Errorf("after: %d", r.status)
	}
}

// TestOversizedResponseStopsAtTheCap asks for a ~3 MB page against an 8 KiB
// cap: the answer is 500 response_too_large, rendering stopped at the cap
// (the budget never held more than the cap, and the request allocated far
// less than the page would encode to), and the budget recovers.
func TestOversizedResponseStopsAtTheCap(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, spaceKeys(1000)...))
	cfg.MaxResponseBytes = 8 << 10
	s := start(t, cfg)
	get(t, s, "/bucket?list-type=2&max-keys=1") // warm the connection pool and the meters
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	r := get(t, s, bigPage)
	runtime.ReadMemStats(&after)
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "response_too_large" || !strings.Contains(r.body, "<Code>InternalError</Code>") {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
	// The page itself (a thousand 1 KiB keys) is about 1 MB; its encoding
	// would be about 3 MB more.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2<<20 {
		t.Errorf("the refused request allocated %d bytes", allocated)
	}
	if peak := s.budget.Peak(); peak > int64(cfg.MaxResponseBytes) {
		t.Errorf("peak charged %d above the %d cap", peak, cfg.MaxResponseBytes)
	}
	if r := get(t, s, "/bucket?list-type=2&max-keys=5"); r.status != 200 {
		t.Errorf("a small page after the refusal: %d", r.status)
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
}

func TestValidate(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}))
	cfg.MaxConcurrentRequests = 7
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "at least 8") {
		t.Errorf("7: %v", err)
	}
	cfg.MaxConcurrentRequests = 8
	if err := cfg.Validate(); err != nil {
		t.Errorf("8: %v", err)
	}
	cfg.MaxResponseBytes = int(cfg.ResponseBufferBudget) + 1
	if cfg.Validate() == nil {
		t.Error("a per-response cap above the budget was accepted")
	}
}

func TestInjectedDelayIsADeadline(t *testing.T) {
	const cost = 60 * time.Millisecond
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error { time.Sleep(cost); return nil }}
	cfg := config(st)
	cfg.Latency, _ = latency.Parse("worker_page=250ms", 0, 1)
	s := start(t, cfg)
	began := time.Now()
	get(t, s, "/bucket?list-type=2")
	took := time.Since(began)
	if took < 250*time.Millisecond || took >= 250*time.Millisecond+cost {
		t.Errorf("took %v: want the 250ms target, not target plus the %v read", took, cost)
	}
	if count(s, metrics.InjectOverrun, "worker_page") != -1 {
		t.Error("an overrun was counted for a request inside its target")
	}
	if count(s, metrics.RequestStageLatency, "s3", "delay") != 1 {
		t.Error("delay stage not recorded")
	}

	// A server slower than the profile overruns it, and says so.
	cfg = config(&hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error { time.Sleep(cost); return nil }})
	cfg.Latency, _ = latency.Parse("worker_page=5ms", 0, 1)
	s = start(t, cfg)
	get(t, s, "/bucket?list-type=2")
	if count(s, metrics.InjectOverrun, "worker_page") != 1 || count(s, metrics.InjectOverrunMs, "worker_page") != 1 {
		t.Errorf("overrun meters: %s", s.reg.AppendMeters(nil))
	}
}

func TestShutdownInterruptsALongDelay(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, "a"))
	cfg.Latency, _ = latency.Parse("worker_page=60s", 0, 1)
	s, _ := New(cfg)
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	got := make(chan response, 1)
	go func() { got <- get(t, s, "/bucket?list-type=2") }()
	time.Sleep(100 * time.Millisecond)
	began := time.Now()
	if complete, err := s.Shutdown(2 * time.Second); !complete || err != nil {
		t.Errorf("shutdown: complete %v, %v", complete, err)
	}
	if d := time.Since(began); d > time.Second {
		t.Errorf("shutdown took %v", d)
	}
	r := <-got
	if r.status != 503 || r.header.Get("x-swath-replay-error") != "shutdown" {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
}

// A store read that ignores cancellation keeps the store open past the
// deadline; it is closed when the read returns.
func TestUninterruptibleReadDefersStoreClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error {
		close(entered)
		<-release
		return nil
	}}
	cfg := config(st)
	var log strings.Builder
	cfg.Log = &syncWriter{w: &log}
	s, _ := New(cfg)
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	go func() {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/bucket?list-type=2", s.Port()), nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	if complete, _ := s.Shutdown(200 * time.Millisecond); complete {
		t.Error("shutdown reported complete with a read outstanding")
	}
	if st.closed.Load() {
		t.Fatal("store closed under an active read")
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for !st.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !st.closed.Load() {
		t.Error("store never closed")
	}
	if !strings.Contains(log.String(), "shutdown_incomplete") {
		t.Errorf("log: %s", log.String())
	}
}

// closeStore wraps a store with a Close that blocks until release and then
// fails, counting its calls.
type closeStore struct {
	store.Store
	release chan struct{}
	calls   atomic.Int32
	err     error
}

func (c *closeStore) Close() error {
	c.calls.Add(1)
	if c.release != nil {
		<-c.release
	}
	c.Store.Close()
	return c.err
}

// TestShutdownBoundsABlockingStoreClose: with every request finished, a
// store Close that blocks cannot hold Shutdown past its deadline; it
// reports shutdown_incomplete and the Close still completes, once, in the
// background, its failure logged.
func TestShutdownBoundsABlockingStoreClose(t *testing.T) {
	st := &closeStore{Store: memstore.Keys(memstore.Options{}, "a"), release: make(chan struct{}), err: errors.New("boom")}
	cfg := config(st)
	log := &syncWriter{w: &strings.Builder{}}
	cfg.Log = log
	s, _ := New(cfg)
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	if r := get(t, s, "/bucket?list-type=2"); r.status != 200 {
		t.Fatalf("%d", r.status)
	}
	began := time.Now()
	complete, err := s.Shutdown(50 * time.Millisecond)
	if took := time.Since(began); took > time.Second || complete || err != nil {
		t.Errorf("took %v, complete %v, err %v", took, complete, err)
	}
	if !strings.Contains(log.String(), "shutdown_incomplete active replay work retained its resources for deferred cleanup") {
		t.Errorf("log: %s", log.String())
	}
	close(st.release)
	waitFor(t, "the deferred close", func() bool { return strings.Contains(log.String(), "replay store cleanup failed: boom") })
	if st.calls.Load() != 1 {
		t.Errorf("Close called %d times", st.calls.Load())
	}
}

// TestShutdownIsIdempotentAndSurfacesCloseFailureOnce is the contract's
// closeIsIdempotentAndSurfacesFixtureCloseFailureOnlyOnce: the first
// Shutdown returns the store's Close failure, the second neither closes
// the store again nor repeats the failure.
func TestShutdownIsIdempotentAndSurfacesCloseFailureOnce(t *testing.T) {
	st := &closeStore{Store: memstore.Keys(memstore.Options{}, "a"), err: errors.New("boom")}
	s, _ := New(config(st))
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	complete, err := s.Shutdown(time.Second)
	if !complete || err == nil || !strings.Contains(err.Error(), "failed to close S3 listing replay fixture") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("first: complete %v, %v", complete, err)
	}
	if complete, err := s.Shutdown(time.Second); !complete || err != nil || st.calls.Load() != 1 {
		t.Errorf("second: complete %v, %v, %d closes", complete, err, st.calls.Load())
	}
}

func TestMetricsPort(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	base := fmt.Sprintf("http://127.0.0.1:%d", s.MetricsPort())
	if r := do(t, "GET", base+"/healthz"); r.status != 200 || r.body != "ok\n" {
		t.Errorf("healthz: %d %q", r.status, r.body)
	}
	if r := do(t, "GET", base+"/other"); r.status != 404 || !strings.Contains(r.body, "/healthz only") {
		t.Errorf("other: %d %q", r.status, r.body)
	}
	get(t, s, "/bucket?list-type=2")
	r := do(t, "GET", base+"/metrics")
	var payload struct {
		SchemaVersion int    `json:"schema_version"`
		ServingMode   string `json:"serving_mode"`
		Serving       struct {
			Protocols             []string `json:"protocols"`
			MaxConcurrentRequests int      `json:"max_concurrent_requests"`
			MaxResponses          int      `json:"max_responses"`
		} `json:"serving"`
		UptimeMs int64 `json:"uptime_ms"`
		Meters   []struct {
			Name  string  `json:"name"`
			Type  string  `json:"type"`
			Count float64 `json:"count"`
		} `json:"meters"`
	}
	if err := json.Unmarshal([]byte(r.body), &payload); err != nil {
		t.Fatalf("%v: %s", err, r.body)
	}
	var requests float64
	for _, m := range payload.Meters {
		if m.Name == "swath.replay.http.requests" {
			requests += m.Count
		}
	}
	if payload.SchemaVersion != 2 || payload.ServingMode != "SORTED" || payload.Serving.MaxResponses != 1024 || requests != 1 {
		t.Errorf("%+v requests=%v", payload, requests)
	}
	// A scrape is not a request.
	if count(s, metrics.HTTPRequests) != 1 {
		t.Error("the metrics port moved a listing meter")
	}
	if r := do(t, "GET", base+"/runtime-attestation"); r.status != 200 || !json.Valid([]byte(r.body)) ||
		!strings.Contains(r.body, `"schema_version":"runtime-attestation-v1"`) {
		t.Errorf("attestation: %d %s", r.status, r.body)
	}
}

func TestAttestation(t *testing.T) {
	dir := t.TempDir()
	mount := filepath.Join(dir, "cgroup")
	os.MkdirAll(filepath.Join(mount, "svc"), 0o755)
	write := func(p, s string) { os.WriteFile(p, []byte(s), 0o644) }
	write(filepath.Join(dir, "cgroup.txt"), "1:cpu:/x\n0::/svc\n")
	write(filepath.Join(dir, "status"), "Name: x\nCpus_allowed_list:\t0-3\n")
	write(filepath.Join(mount, "svc", "cpuset.cpus.effective"), "0-1\n")
	write(filepath.Join(mount, "svc", "memory.max"), "4294967296\n")
	write(filepath.Join(mount, "svc", "memory.swap.max"), "max\n")
	a := attestation{filepath.Join(dir, "cgroup.txt"), filepath.Join(dir, "status"), mount}
	want := `{"schema_version":"runtime-attestation-v1","cgroup_v2":{"directory":` + jsonString(filepath.Join(mount, "svc")) +
		`,"cpuset_cpus_effective":{"value":"0-1","error":null},"memory_max":{"value":4294967296,"error":null},` +
		`"memory_swap_max":{"value":"max","error":null}},"proc_self_status":{"cpus_allowed_list":{"value":"0-3","error":null}}}`
	if got := string(a.render()); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
	write(filepath.Join(mount, "svc", "memory.max"), "-5\n")
	os.Remove(filepath.Join(mount, "svc", "cpuset.cpus.effective"))
	got := string(a.render())
	if !strings.Contains(got, `"cpuset_cpus_effective":{"value":null,"error":"missing: `) || !strings.Contains(got, `"error":"invalid negative cgroup limit in `) {
		t.Errorf("%s", got)
	}
	write(filepath.Join(dir, "cgroup.txt"), "0::/../../etc\n")
	if got := string(a.render()); !strings.Contains(got, `"directory":null`) || !strings.Contains(got, "escapes its mount") {
		t.Errorf("%s", got)
	}
}

func jsonString(s string) string { b, _ := json.Marshal(s); return string(b) }

// TestCountsNoOverrunWhenInjectionIsOff is the contract's
// countsNoOverrunWhenInjectionIsOff, with its
// recordsFixtureListLatencyMetricOnList: without a profile no overrun is
// counted, however slow the read, and every list is timed.
func TestCountsNoOverrunWhenInjectionIsOff(t *testing.T) {
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error { time.Sleep(20 * time.Millisecond); return nil }}
	s := start(t, config(st))
	for range 2 {
		if r := get(t, s, "/bucket?list-type=2"); r.status != 200 {
			t.Fatalf("%d", r.status)
		}
	}
	if count(s, metrics.InjectOverrun, "worker_page") != -1 || count(s, metrics.RequestStageLatency, "s3", "delay") != -1 {
		t.Errorf("an overrun or a delay was recorded with injection off: %s", s.reg.AppendMeters(nil))
	}
	if count(s, metrics.FixtureListLatency) != 2 {
		t.Errorf("fixture list latency counted %d", count(s, metrics.FixtureListLatency))
	}
}

// TestContinuationPagesDrawDistinctJitter is the contract's
// continuationPagesDrawDistinctJitter: successive pages of one walk differ
// only in their token, and must not all draw the same jitter.
func TestContinuationPagesDrawDistinctJitter(t *testing.T) {
	keys := make([]string, 40)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	p := &s3.Pager{Store: memstore.Keys(memstore.Options{}, keys...)}
	profile, _ := latency.Parse("worker_page=1000ms", 0.5, 1)
	seen := map[time.Duration]bool{}
	query := "list-type=2&max-keys=2"
	for page := 0; page < 8; page++ {
		req, err := s3.ParseRequest("bucket", query)
		if err != nil {
			t.Fatal(err)
		}
		res, err := p.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		seen[profile.Delay(latency.WorkerPage, 0, req.LatencyKey())] = true
		query = "list-type=2&max-keys=2&continuation-token=" + s3.PercentEncode([]byte(res.NextToken))
	}
	if len(seen) < 6 {
		t.Errorf("8 continuation pages drew %d distinct delays", len(seen))
	}
}

// TestRoutesShareTheResponseCeiling: the GCS and Azure routes go through
// the same admission as S3's. With every response permit held by slow S3
// readers (and no execution slot), a request on either other route is
// refused response_count_exhausted at once; when the readers leave, both
// answer again and every byte of the budget is back.
func TestRoutesShareTheResponseCeiling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("receive buffer sizing")
	}
	cfg := config(memstore.Keys(memstore.Options{}, spaceKeys(1000)...))
	cfg.Bucket = "bucket"
	cfg.Protocols = []string{ProtocolS3, ProtocolGCS, ProtocolAzure}
	cfg.AzureAccount = DefaultAzureAccount
	cfg.MaxConcurrentRequests = MinMaxConcurrentRequests
	cfg.WriteTimeout = time.Minute
	s := start(t, cfg)
	var conns []net.Conn
	for range cfg.MaxResponses() {
		conns = append(conns, slowReader(t, s, bigPage))
	}
	waitFor(t, "every response permit held and no slot", func() bool {
		return s.responses.Load() == int64(cfg.MaxResponses()) && len(s.slots) == 0
	})
	client := http.Client{Timeout: 5 * time.Second}
	for _, target := range []string{"/storage/v1/b/bucket/o?maxResults=1", "/replay/bucket?restype=container&comp=list&maxresults=1"} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), target), nil)
		req.Header.Set("x-ms-version", "2026-06-06")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: not answered: %v", target, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 || resp.Header.Get("x-swath-replay-error") != "response_count_exhausted" {
			t.Errorf("%s: %d %v", target, resp.StatusCode, resp.Header)
		}
	}
	for _, c := range conns {
		c.Close()
	}
	waitFor(t, "the permits and bytes to come back", func() bool { return s.responses.Load() == 0 && s.budget.Charged() == 0 })
	for _, target := range []string{"/storage/v1/b/bucket/o?maxResults=1", "/replay/bucket?restype=container&comp=list&maxresults=1"} {
		req, _ := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), target), nil)
		req.Header.Set("x-ms-version", "2026-06-06")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s after: %d", target, resp.StatusCode)
		}
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
}

// pipelinedReader opens a connection with a small receive buffer and
// sends n requests for target back to back without reading an answer.
func pipelinedReader(t *testing.T, s *Server, target string, n int) net.Conn {
	t.Helper()
	conn := slowReader(t, s, target)
	go func() {
		for range n - 1 {
			if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: replay\r\n\r\n", target); err != nil {
				return
			}
		}
	}()
	return conn
}

// TestPipelinedSmallResponsesStayWithinTheCeiling: small answers fit the
// server's own buffers, so a client that pipelines requests and stops
// reading stalls the answers in the final flush, not in Write. The flush
// happens while the response permit is held, so with a ceiling of 8 the
// sixteen permits end up held by stalled flushes, every further request
// (on the stalled connections or a fresh one) is refused
// response_count_exhausted, and everything comes back on disconnect.
func TestPipelinedSmallResponsesStayWithinTheCeiling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("receive buffer sizing")
	}
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	cfg := config(memstore.Keys(memstore.Options{}, keys...))
	cfg.MaxConcurrentRequests = MinMaxConcurrentRequests
	cfg.WriteTimeout = time.Minute
	s := start(t, cfg)
	var conns []net.Conn
	for range cfg.MaxResponses() + 8 {
		// About 4 KB an answer: well inside the server's write buffer, so
		// each Write returns at once, and 2,000 of them are far more than
		// the socket buffers hold.
		conns = append(conns, pipelinedReader(t, s, "/bucket?list-type=2&max-keys=20", 2000))
	}
	defer func() {
		if t.Failed() {
			t.Logf("responses %d slots %d refused %d requests %d", s.responses.Load(), len(s.slots),
				count(s, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted"), count(s, metrics.HTTPRequests))
		}
	}()
	waitFor(t, "every response permit held by a stalled flush", func() bool {
		return s.responses.Load() == int64(cfg.MaxResponses()) && len(s.slots) == 0 &&
			count(s, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted") > 0
	})
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/bucket?list-type=2&max-keys=1", s.Port()))
	if err != nil {
		t.Fatalf("a request past the ceiling was not answered: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("x-swath-replay-error") != "response_count_exhausted" {
		t.Errorf("%d %v", resp.StatusCode, resp.Header)
	}
	if n := s.responses.Load(); n > int64(cfg.MaxResponses()) {
		t.Errorf("%d responses outstanding", n)
	}
	for _, c := range conns {
		c.Close()
	}
	waitFor(t, "the permits and bytes to come back", func() bool { return s.responses.Load() == 0 && s.budget.Charged() == 0 })
	if r := get(t, s, "/bucket?list-type=2&max-keys=1"); r.status != 200 {
		t.Errorf("after the readers left: %d", r.status)
	}
}

// TestMetricsConnectionsTimeOut: the metrics listener holds no
// connection past its timeouts. An idle keep-alive connection is closed
// after --idle-timeout, and a client that pipelines /metrics requests
// and stops reading is cut off after --write-timeout.
func TestMetricsConnectionsTimeOut(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("receive buffer sizing")
	}
	cfg := config(memstore.Keys(memstore.Options{}, "a"))
	cfg.IdleTimeout, cfg.WriteTimeout = 300*time.Millisecond, 300*time.Millisecond
	s := start(t, cfg)
	addr := fmt.Sprintf("127.0.0.1:%d", s.MetricsPort())

	idle, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	fmt.Fprintf(idle, "GET /healthz HTTP/1.1\r\nHost: m\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(idle), nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	resp.Body.Close()
	waitFor(t, "the idle metrics connection to be closed", func() bool { return s.metricsConns.Load() == 0 })

	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		c.Control(func(fd uintptr) { serr = setReceiveBuffer(fd, 4096) })
		return serr
	}}
	stalled, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	go func() {
		for range 5000 {
			if _, err := fmt.Fprintf(stalled, "GET /metrics HTTP/1.1\r\nHost: m\r\n\r\n"); err != nil {
				return
			}
		}
	}()
	waitFor(t, "the stalled metrics connection to open", func() bool { return s.metricsConns.Load() == 1 })
	waitFor(t, "the stalled metrics connection to be cut off", func() bool { return s.metricsConns.Load() == 0 })
}
