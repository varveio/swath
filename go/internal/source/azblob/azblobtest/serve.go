// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// Container is the container (and S3 bucket) Serve exposes; the account is
// server.DefaultAzureAccount.
const Container = "corpus"

// Replay is a replay server Serve started.
type Replay struct {
	// Endpoint is the Azure endpoint with the account path,
	// http://127.0.0.1:PORT/ACCOUNT (no trailing slash), as Azurite's is:
	// the value --endpoint-url takes. The container's listing is
	// Endpoint/Container?restype=container&comp=list.
	Endpoint string
	// Account and Container name the listing.
	Account, Container string
	// S3Endpoint is the S3 route on the same server, http://127.0.0.1:PORT;
	// the bucket is Container.
	S3Endpoint string
	// Stop shuts the server down and closes the fixture; it is registered
	// with t.Cleanup and may be called earlier, more than once.
	Stop func()
}

// Option configures Serve.
type Option func(*options)

type options struct{ hns bool }

// HNS has Get Account Information report a hierarchical-namespace account
// (server.Config.AzureHNS), for the provider's refusal.
func HNS() Option { return func(o *options) { o.hns = true } }

// Serve sorts keys into a fixture (sort-fixture's key cap at MaxNameBytes;
// the key at byte-order index i has size i and Last-Modified i seconds after
// 2026-01-01T00:00:00Z, as legacytest.Object derives them), opens it with
// the replay's store and serves it in-process with --protocols=s3,azure on
// 127.0.0.1. Duplicate keys or a key over MaxNameBytes fail the test.
//
// The Go replay has no injected short or empty pages (its pagination
// profile is fixed at short=0;empty-first=false), so a test forces short
// pages with maxresults.
func Serve(t testing.TB, keys []string, opts ...Option) *Replay {
	t.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	keys = slices.Clone(keys)
	slices.Sort(keys)
	rows := make([]legacytest.Row, len(keys))
	for i, k := range keys {
		rows[i] = legacytest.Object(k, i)
	}
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	if err := os.Mkdir(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(capture, "part-00001.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(dir, "fixture")
	opt := sortfixture.Options{Capture: capture, Output: fixture, KeyLimit: MaxNameBytes}
	if _, err := sortfixture.Run(context.Background(), opt, io.Discard); err != nil {
		t.Fatal(err)
	}
	st, meta, err := store.Open(context.Background(), fixture, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.New(server.Config{
		Bucket: Container, Host: "127.0.0.1", MetricsPort: -1,
		MaxConcurrentRequests: server.DefaultMaxConcurrentRequests,
		ResponseBufferBudget:  server.DefaultResponseBufferBudget, MaxResponseBytes: server.DefaultMaxResponseBytes,
		StopTimeout: server.DefaultStopTimeout, IdleTimeout: server.DefaultIdleTimeout, WriteTimeout: server.DefaultWriteTimeout,
		SlowRequestLog: -1, Store: st, ServingMode: "SORTED", FixtureIdentity: meta.Identity,
		Protocols:    []string{"s3", server.ProtocolAzure},
		AzureAccount: server.DefaultAzureAccount, AzureHNS: o.hns,
	})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		st.Close()
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = s.Serve()
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			s.Shutdown(time.Second)
			<-served
			st.Close()
		})
	}
	t.Cleanup(stop)
	base := fmt.Sprintf("http://127.0.0.1:%d", s.Port())
	return &Replay{
		Endpoint: base + "/" + server.DefaultAzureAccount, Account: server.DefaultAzureAccount, Container: Container,
		S3Endpoint: base, Stop: stop,
	}
}
