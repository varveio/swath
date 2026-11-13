// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

const replayAccount, replayContainer = "replay", "bkt"

// replayServer serves keys on the replay's Azure route alone (account
// replay, container bkt) and returns the server's base URL.
func replayServer(t *testing.T, keys []string) string {
	t.Helper()
	objs := make([]store.Object, len(keys))
	for i, k := range keys {
		size, lm := int64(10*i+1), time.Date(2026, 1, 2, 3, 4, i, 0, time.UTC)
		objs[i] = store.Object{Key: []byte(k), Size: &size, LastModified: &lm}
	}
	st, err := memstore.New(objs, memstore.Options{Rollup: true})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: replayContainer, Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: 256 << 20, MaxResponseBytes: 64 << 20, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "MEMORY", FixtureIdentity: "azblob-client",
		Protocols: []string{"azure"}, AzureAccount: replayAccount})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	return fmt.Sprintf("http://127.0.0.1:%d", srv.Port())
}
