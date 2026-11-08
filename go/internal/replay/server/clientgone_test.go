// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// TestClientGoneFinishesThePage: a client that disconnects while its page
// is being read does not cancel the read. On every route the page is
// finished and counted as served (2xx, nothing in http.errors, no failure
// in the log), as the replay wire contract's server counts it.
func TestClientGoneFinishesThePage(t *testing.T) {
	for _, tc := range []struct{ protocol, target, header string }{
		{"s3", "/bucket?list-type=2", ""},
		{"gcs", gcsList, ""},
		{"azure", "/replay/bucket?restype=container&comp=list", "2026-06-06"},
	} {
		t.Run(tc.protocol, func(t *testing.T) {
			entered, release := make(chan struct{}, 1), make(chan struct{})
			st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a", "b"), hook: func(ctx context.Context) error {
				entered <- struct{}{}
				<-release
				return ctx.Err()
			}}
			cfg := config(st)
			cfg.Protocols = []string{ProtocolS3}
			if tc.protocol != ProtocolS3 {
				cfg.Protocols = append(cfg.Protocols, tc.protocol)
			}
			if tc.protocol == ProtocolAzure {
				cfg.AzureAccount = DefaultAzureAccount
			}
			var log strings.Builder
			cfg.Log = &syncWriter{w: &log}
			s := start(t, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), tc.target), nil)
			if tc.header != "" {
				req.Header.Set("x-ms-version", tc.header)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if resp, err := http.DefaultClient.Do(req); err == nil {
					resp.Body.Close()
				}
			}()
			<-entered
			cancel()
			<-done
			time.Sleep(20 * time.Millisecond) // the server sees the connection close
			close(release)
			waitFor(t, "the page to be answered", func() bool { return count(s, metrics.HTTPRequests) == 1 })
			if count(s, metrics.ProtocolHTTPRequests, tc.protocol, "2xx") != 1 || count(s, metrics.HTTPErrors) != 0 ||
				strings.Contains(log.String(), "failed to serve") {
				t.Errorf("meters %s\nlog %s", s.reg.AppendMeters(nil), log.String())
			}
		})
	}
}
