// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
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
		code           string
	}{
		{"GET", "/other?list-type=2", 404, "NoSuchBucket"},
		{"GET", "/b%75cket?list-type=2", 404, "NoSuchBucket"},
		{"GET", "/?list-type=2", 404, "NoSuchBucket"},
		{"GET", "/bucket?max-keys=1", 400, "InvalidArgument"},
		{"GET", "/bucket?list-type=2&continuation-token=zz", 400, "InvalidArgument"},
		{"POST", "/bucket?list-type=2", 405, "MethodNotAllowed"},
	} {
		r := do(t, c.method, fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), c.target))
		resource, _, _ := strings.Cut(c.target, "?")
		if r.status != c.status || !strings.Contains(r.body, "<Code>"+c.code+"</Code>") ||
			!strings.Contains(r.body, "<Resource>"+resource+"</Resource>") || r.header.Get("x-amz-request-id") != "S3LISTINGREPLAY" ||
			r.header.Get("Content-Type") != "application/xml" {
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

func TestResponseCountAndBudgetRefusals(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	s.responses.Store(int64(s.cfg.MaxResponses()))
	r := get(t, s, "/bucket?list-type=2")
	if r.status != 503 || r.header.Get("x-swath-replay-error") != "response_count_exhausted" || !strings.Contains(r.body, "<Code>ServiceUnavailable</Code>") {
		t.Errorf("count: %d %v %s", r.status, r.header, r.body)
	}
	s.responses.Store(0)
	if r := get(t, s, "/bucket?list-type=2"); r.status != 200 {
		t.Errorf("after the permits came back: %d", r.status)
	}
	s.budget.charged.Store(s.budget.limit)
	r = get(t, s, "/bucket?list-type=2")
	if r.status != 503 || r.header.Get("x-swath-replay-error") != "response_budget_exhausted" {
		t.Errorf("budget: %d %v", r.status, r.header)
	}
	s.budget.charged.Store(0)
	if count(s, metrics.ResponseAdmissionRefused, "s3", "response_count_exhausted") != 1 ||
		count(s, metrics.ResponseAdmissionRefused, "s3", "response_budget_exhausted") != 1 {
		t.Errorf("refusal meters: %s", s.reg.AppendMeters(nil))
	}
	if r := get(t, s, "/bucket?list-type=2"); r.status != 200 || s.budget.charged.Load() != 0 {
		t.Errorf("after the budget came back: %d, charged %d", r.status, s.budget.charged.Load())
	}
}

func TestOversizedResponseIsRefusedAndTheBudgetRecovers(t *testing.T) {
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%03d", strings.Repeat("x", 100), i)
	}
	cfg := config(memstore.Keys(memstore.Options{}, keys...))
	cfg.MaxResponseBytes = 8 << 10
	s := start(t, cfg)
	r := get(t, s, "/bucket?list-type=2")
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "response_too_large" || !strings.Contains(r.body, "<Code>InternalError</Code>") {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
	if r := get(t, s, "/bucket?list-type=2&max-keys=5"); r.status != 200 {
		t.Errorf("a small page after the refusal: %d", r.status)
	}
	if s.budget.charged.Load() != 0 || s.budget.peak.Load() == 0 {
		t.Errorf("charged %d peak %d", s.budget.charged.Load(), s.budget.peak.Load())
	}
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
	if !s.Shutdown(2 * time.Second) {
		t.Error("shutdown incomplete")
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
	if s.Shutdown(200 * time.Millisecond) {
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
