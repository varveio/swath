// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func TestMain(m *testing.M) {
	if os.Getenv("SWATH_REPLAY_TEST_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestFlagSpellings(t *testing.T) {
	for _, args := range [][]string{
		{"--fixture", "f.tsv", "--bucket", "b", "--max-concurrent-requests", "1024", "--inject-latency", "prod-commoncrawl", "--latency-scale", "50"},
		{"--fixture=f.tsv", "--bucket=b", "--max-concurrent-requests=1024", "-inject-latency=prod-commoncrawl", "-latency-scale", "50"},
	} {
		f, err := parseServe(args, io.Discard)
		if err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		if f.fixture != "f.tsv" || f.bucket != "b" || f.maxConcurrentRequests != 1024 || f.latencyScale != 50 || f.injectLatency != "prod-commoncrawl" {
			t.Errorf("%q: %+v", args, f)
		}
	}
	f, _ := parseServe([]string{"--fixture", "f", "--bucket", "b"}, io.Discard)
	if f.latencyScale != 1 || f.metricsPort != -1 || f.maxConcurrentRequests != 512 || f.host != "127.0.0.1" || f.stopTimeout != "10s" {
		t.Errorf("defaults: %+v", f)
	}
	if _, err := parseServe([]string{"--bucket", "b"}, io.Discard); err == nil {
		t.Error("a missing --fixture was accepted")
	}
}

func TestConfigRefusals(t *testing.T) {
	base := []string{"--fixture", "f", "--bucket", "b"}
	for _, extra := range [][]string{
		{"--serving-mode", "duckdb"}, {"--serving-mode", "other"}, {"--protocols", "s3,gcs"}, {"--protocols", "azure"}, {"--protocols", "azure", "--azure-account", "Bad"},
		{"--protocols", "azure", "--host", "0.0.0.0"},
		{"--protocols", "s3,"}, {"--protocols", "replay"}, {"--azure-account", "replay"}, {"--advertised-host", "http://x"},
		{"--max-concurrent-requests", "7"}, {"--stop-timeout", "0s"}, {"--write-timeout", "soon"},
		{"--max-response-bytes", "300000000"},
	} {
		f, err := parseServe(append(append([]string{}, base...), extra...), io.Discard)
		if err != nil {
			t.Fatalf("%q: %v", extra, err)
		}
		if _, err := f.config(); err == nil {
			t.Errorf("%q accepted", extra)
		}
	}
	f, _ := parseServe(append(base, "--serving-mode", "SORTED", "--protocols", " S3 ", "--stop-timeout", "PT0.5S", "--idle-timeout", "1500ms"), io.Discard)
	cfg, err := f.config()
	if err != nil || cfg.StopTimeout != 500*time.Millisecond || cfg.IdleTimeout != 1500*time.Millisecond {
		t.Errorf("%+v %v", cfg, err)
	}
}

func TestParseTimeout(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"10s": 10 * time.Second, "250ms": 250 * time.Millisecond, "PT10S": 10 * time.Second, "pt1m30s": 90 * time.Second,
		"P1D": 24 * time.Hour, "PT0.25S": 250 * time.Millisecond, " 3S ": 3 * time.Second,
	} {
		if got, err := parseTimeout(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	for _, in := range []string{"", "10", "s", "ms", "1.5s", "PT", "P", "soon"} {
		if _, err := parseTimeout(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestAdvertisedHost(t *testing.T) {
	for _, bad := range []string{"http://example.test", "example.test:8080", "example/path", "::", "[::]", "0.0.0.0",
		"00.0.0.0", "999.999.999.999", "::ffff:127.0.0.1", "fe80::1%eth0", "-x.test"} {
		if validAdvertisedHost(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
	for _, good := range []string{"example.test", "2001:db8::1", "[2001:db8::1]", "10.0.0.1", "localhost"} {
		if !validAdvertisedHost(good) {
			t.Errorf("%q refused", good)
		}
	}
}

func TestBracketHost(t *testing.T) {
	if got := bracketHost("", "::1"); got != "[::1]" {
		t.Errorf("%s", got)
	}
}

// TestServeEndToEnd runs the command as a process: the startup line, a
// listing, the health and metrics endpoints, then SIGTERM.
func TestServeEndToEnd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "keys.tsv")
	if err := os.WriteFile(fixture, []byte("a/1\t5\t1767225600000000\tetag\tSTANDARD\nb%20c\t7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "serve", "--fixture", fixture, "--bucket", "bkt", "--port", "0", "--metrics-port=0",
		"--parquet-connections", "2", "--inject-latency", "worker_page=5ms", "--latency-scale", "2")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	lines := bufio.NewScanner(stderr)
	var startup string
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	var names []string
	for _, field := range strings.Fields(startup)[1:] {
		name, _, _ := strings.Cut(field, "=")
		names = append(names, name)
	}
	const want = "endpoint bucket fixture serving_mode parquet_connections inject_latency latency_scale metrics_endpoint " +
		"max_concurrent_requests protocols response_buffer_budget max_response_bytes max_responses output_chunk_bytes " +
		"fixture_identity ordering_profile metadata_policy profiles native_endpoints"
	if strings.Join(names, " ") != want {
		t.Errorf("startup keys:\n%s\nwant\n%s\n(%s)", strings.Join(names, " "), want, startup)
	}
	endpoint := regexp.MustCompile(`endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]
	metrics := regexp.MustCompile(`metrics_endpoint=(http://[^ ]+)/metrics`).FindStringSubmatch(startup)[1]
	if !strings.Contains(startup, "parquet_connections=2 inject_latency=worker_page=5ms latency_scale=2.0") ||
		!strings.Contains(startup, "serving_mode=MEMORY") {
		t.Errorf("startup: %s", startup)
	}
	body := fetch(t, metrics+"/healthz")
	if body != "ok\n" {
		t.Errorf("healthz %q", body)
	}
	body = fetch(t, endpoint+"/bkt?list-type=2&encoding-type=url")
	if !strings.Contains(body, "<Key>a/1</Key>") || !strings.Contains(body, "<Key>b%20c</Key>") {
		t.Errorf("%s", body)
	}
	if !strings.Contains(fetch(t, metrics+"/metrics"), `"name":"swath.replay.http.requests","type":"counter","tags":{},"count":1.000000`) {
		t.Error("metrics did not count the request")
	}
	cmd.Process.Signal(syscall.SIGTERM)
	rest, _ := io.ReadAll(stderr)
	err := cmd.Wait()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 143 {
		t.Errorf("exit: %v", err)
	}
	if !strings.Contains(string(rest), "swath_replay_shutdown_hook complete") {
		t.Errorf("stderr after the signal: %s", rest)
	}
}

func fetch(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// TestSigtermWithActiveWorkIsBoundedAndIdempotent is the contract's
// sigtermRunsBoundedIdempotentServeShutdownHook with work in flight: a
// request sleeping in a 60 s injected delay is answered 503 shutdown, the
// process exits 143 within its 1 s stop timeout plus slack, and two
// SIGTERMs run the shutdown once.
func TestSigtermWithActiveWorkIsBoundedAndIdempotent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	fixture := filepath.Join(t.TempDir(), "keys.tsv")
	if err := os.WriteFile(fixture, []byte("a\t1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "serve", "--fixture", fixture, "--bucket", "bkt", "--port", "0", "--metrics-port", "0",
		"--inject-latency", "worker_page=60s", "--stop-timeout", "1s")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	lines := bufio.NewScanner(stderr)
	var startup string
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	endpoint := regexp.MustCompile(`endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]
	metrics := regexp.MustCompile(`metrics_endpoint=(http://[^ ]+)/metrics`).FindStringSubmatch(startup)[1]
	answered := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Get(endpoint + "/bkt?list-type=2")
		if err != nil {
			answered <- nil
			return
		}
		answered <- resp
	}()
	active := `"name":"swath.replay.protocol.requests.active","type":"gauge","tags":{"protocol":"s3"},"value":1.000000`
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(fetch(t, metrics+"/metrics"), active) {
		if time.Now().After(deadline) {
			t.Fatal("the request never became active")
		}
		time.Sleep(10 * time.Millisecond)
	}
	began := time.Now()
	cmd.Process.Signal(syscall.SIGTERM)
	cmd.Process.Signal(syscall.SIGTERM)
	rest, _ := io.ReadAll(stderr)
	err := cmd.Wait()
	if took := time.Since(began); took > 3*time.Second {
		t.Errorf("shutdown took %v with a 1s stop timeout", took)
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 143 {
		t.Errorf("exit: %v", err)
	}
	if n := strings.Count(string(rest), "swath_replay_shutdown_hook complete"); n != 1 {
		t.Errorf("hook ran %d times: %s", n, rest)
	}
	resp := <-answered
	if resp != nil {
		resp.Body.Close()
	}
	if resp == nil || resp.StatusCode != 503 || resp.Header.Get("x-swath-replay-error") != "shutdown" {
		t.Errorf("the in-flight request: %v", resp)
	}
}

// failingCloseStore is a store whose Close fails, counting its calls.
type failingCloseStore struct {
	store.Store
	closes int
}

func (f *failingCloseStore) Close() error {
	f.closes++
	f.Store.Close()
	return errors.New("boom")
}

// TestFailedStartClosesTheStoreAndKeepsItsError is the contract's
// startFailureClosesOwnedFixtureAndPreservesCleanupFailureAsSuppressed:
// when the server cannot start, the store it owns is closed once, the
// start's own error comes back unchanged, and the close failure is
// reported once beside it.
func TestFailedStartClosesTheStoreAndKeepsItsError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	base := server.Config{Bucket: "bkt", Host: "127.0.0.1", Port: busy.Addr().(*net.TCPAddr).Port, MetricsPort: -1,
		MaxConcurrentRequests: server.DefaultMaxConcurrentRequests, ResponseBufferBudget: server.DefaultResponseBufferBudget,
		MaxResponseBytes: server.DefaultMaxResponseBytes, StopTimeout: time.Second, IdleTimeout: time.Second,
		WriteTimeout: time.Second, SlowRequestLog: -1, ServingMode: "MEMORY"}

	st := &failingCloseStore{Store: memstore.Keys(memstore.Options{}, "a")}
	cfg := base
	cfg.Store = st
	var log strings.Builder
	srv, err := startServer(cfg, &log)
	var opErr *net.OpError
	if srv != nil || !errors.As(err, &opErr) || opErr.Op != "listen" {
		t.Errorf("listen failure: %v, %v", srv, err)
	}
	if st.closes != 1 || strings.Count(log.String(), "replay store cleanup failed after a failed start: boom") != 1 {
		t.Errorf("%d closes, log %q", st.closes, log.String())
	}

	st = &failingCloseStore{Store: memstore.Keys(memstore.Options{}, "a")}
	cfg = base
	cfg.Store, cfg.MaxConcurrentRequests = st, 1
	log.Reset()
	_, err = startServer(cfg, &log)
	var usage usageError
	if !errors.As(err, &usage) || !strings.Contains(err.Error(), "at least 8") || st.closes != 1 ||
		strings.Count(log.String(), "boom") != 1 {
		t.Errorf("configuration failure: %v, %d closes, log %q", err, st.closes, log.String())
	}
}
