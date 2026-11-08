// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// TestConnectionsAreBounded: with a request ceiling of 8 the serving
// listener holds at most 32 connections; the 33rd is closed as soon as it
// is accepted, and once one of the 32 closes a new connection is served.
func TestConnectionsAreBounded(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, "a"))
	cfg.MaxConcurrentRequests = MinMaxConcurrentRequests
	s := start(t, cfg)
	if cfg.MaxConnections() != 32 {
		t.Fatalf("bound %d", cfg.MaxConnections())
	}
	addr := fmt.Sprintf("127.0.0.1:%d", s.Port())
	var held []net.Conn
	for range cfg.MaxConnections() {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	waitFor(t, "32 connections held", func() bool { return s.conns.open.Load() == int64(cfg.MaxConnections()) })
	extra, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	extra.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(extra, "GET /bucket?list-type=2 HTTP/1.1\r\nHost: x\r\n\r\n")
	if n, err := extra.Read(make([]byte, 1)); n != 0 || err == nil || err.Error() == "i/o timeout" {
		t.Errorf("the connection past the bound was not closed: %d %v", n, err)
	}
	if s.conns.open.Load() != int64(cfg.MaxConnections()) {
		t.Errorf("%d open", s.conns.open.Load())
	}
	held[0].Close()
	held = held[1:]
	waitFor(t, "a connection to free", func() bool { return s.conns.open.Load() < int64(cfg.MaxConnections()) })
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(c, "GET /bucket?list-type=2 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	b, _ := io.ReadAll(c)
	if len(b) < 12 || string(b[9:12]) != "200" {
		t.Errorf("after one closed: %q", b)
	}
}
