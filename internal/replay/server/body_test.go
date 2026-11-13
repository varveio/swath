// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// TestUnsentBodyHoldsNothing: a listing request that declares a body (one
// byte by Content-Length, or chunked) and never sends it is answered as it
// would be without one, and the connection is closed after the answer,
// not held open waiting for the body: on all three routes, the answer
// arrives at once, the connection ends within the idle timeout, and no
// response permit or budget byte stays held. Before, the server read the
// missing body while committing the answer, holding the permit forever.
func TestUnsentBodyHoldsNothing(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{}, "a", "b"))
	cfg.Protocols = []string{ProtocolS3, ProtocolGCS, ProtocolAzure}
	cfg.AzureAccount = DefaultAzureAccount
	cfg.IdleTimeout = 2 * time.Second
	s := start(t, cfg)
	targets := []string{
		"/bucket?list-type=2&max-keys=1",
		"/storage/v1/b/bucket/o?maxResults=1",
		"/replay/bucket?restype=container&comp=list&maxresults=1",
	}
	var conns []net.Conn
	for _, target := range targets {
		for _, framing := range []string{"Content-Length: 1\r\n", "Transfer-Encoding: chunked\r\n"} {
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, conn)
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\nx-ms-version: 2026-06-06\r\n%s\r\n", target, framing)
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
			if err != nil {
				t.Fatalf("%s %q: no answer: %v", target, framing, err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("%s %q: %d", target, framing, resp.StatusCode)
			}
			// The server ends the connection rather than waiting for the
			// body (an error other than EOF would be the read deadline).
			if _, err := br.ReadByte(); err != io.EOF && !strings.Contains(fmt.Sprint(err), "reset") {
				t.Errorf("%s %q: the connection stayed open: %v", target, framing, err)
			}
		}
	}
	waitFor(t, "the permits and bytes to come back", func() bool { return s.responses.Load() == 0 && s.budget.Charged() == 0 })
	for _, c := range conns {
		c.Close()
	}
}
