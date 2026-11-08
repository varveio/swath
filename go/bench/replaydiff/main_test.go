// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func TestBytesComparator(t *testing.T) {
	h := func(kv ...string) http.Header {
		x := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			x[kv[i]] = []string{kv[i+1]}
		}
		return x
	}
	ref := &response{200, h("Content-Type", "application/xml", "x-amz-request-id", "A", "Date", "x", "Server", "Jetty"), []byte("<a>hello</a>")}
	same := &response{200, h("Content-Type", "application/xml", "X-Amz-Request-Id", "A", "Date", "y"), []byte("<a>hello</a>")}
	if d := (bytesComparator{}).compare(ref, same); !d.OK {
		t.Errorf("identical answers differ: %+v", d)
	}
	other := &response{200, h("Content-Type", "text/xml", "x-amz-request-id", "A", "x-swath-replay-error", "r"), []byte("<a>help</a>")}
	d := (bytesComparator{}).compare(ref, other)
	if d.OK || d.Offset != 6 || len(d.HeaderDiffs) != 2 {
		t.Errorf("%+v", d)
	}
}

func TestMatrixNamesAreUnique(t *testing.T) {
	keys := [][]byte{[]byte("a/b/c"), []byte("a/b/d e"), []byte("a/x"), []byte("b/é"), []byte("c")}
	seen := map[string]bool{}
	for _, r := range s3Matrix(keys, rand.New(rand.NewPCG(1, 2)), 100) {
		if seen[r.Name] {
			t.Errorf("duplicate %q", r.Name)
		}
		seen[r.Name] = true
	}
	if !seen["prefix=a/b/ delimiter=/"] || !seen["start-after=successor a/"] || !seen["encoded key 0"] {
		t.Errorf("missing expected requests: %v", len(seen))
	}
}

// TestDriverFindsNoDifferenceBetweenRollupAndWalk runs the whole request
// set against two Go servers over the same keys, one whose store answers
// the "/" rollup and one that walks ranges: their answers must be
// byte-identical, and the driver must say so.
func TestDriverFindsNoDifferenceBetweenRollupAndWalk(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("d%d/s%d/k%03d", i%3, i%7, i), fmt.Sprintf("top%02d", i%40), fmt.Sprintf("x/%c y", 'a'+i%26))
	}
	client := &http.Client{}
	ref := &target{name: "walk", base: startGo(t, false, dedupe(keys)...), client: client}
	got := &target{name: "rollup", base: startGo(t, true, dedupe(keys)...), client: client}
	o := options{bucket: "bkt", parallel: 4, maxPrefixes: 50, maxTop: 10}
	rep := &report{}
	d := &driver{o: o, ref: ref, got: got, comp: bytesComparator{}, rep: rep}
	all, _, err := listAll(ref, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(9, 9))
	cases := s3Matrix(all, rng, o.maxPrefixes)
	if runtime.GOOS == "windows" {
		// The residual cases end with the server closing a connection that
		// has unread input: Windows then resets it, which can discard the 400
		// before the client reads it.
		cases = slices.DeleteFunc(cases, func(r request) bool { return r.Compare == "residual" })
	}
	d.runCases("case", cases)
	d.runWalks(s3Walks(all, []int{1, 7, 1000}, o.maxTop))
	var random []request
	for range 200 {
		random = append(random, randomRequest(all, rng, directoryPrefixes(all)))
	}
	d.runCases("random", random)
	if len(rep.Skipped) != 0 {
		t.Errorf("requests skipped on a fixture that truncates every seed page: %v", rep.Skipped)
	}
	if rep.Failed != 0 || rep.Passed < 500 {
		for _, r := range rep.Results {
			if !r.Pass {
				t.Errorf("%s %q: %+v %s", r.Kind, r.Name, r.diff, r.Error)
				break
			}
		}
		t.Fatalf("%d passed, %d failed", rep.Passed, rep.Failed)
	}
}

func dedupe(keys []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// startGo serves keys from a Go server on a free port.
func startGo(t *testing.T, rollup bool, keys ...string) string {
	t.Helper()
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
		ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: rollup}, keys...), ServingMode: "MEMORY"})
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

// TestTokenCasesSkipWithoutAToken: on a one-key fixture the seed pages
// issue no token, so every request that needs one is reported skipped,
// not passed; on two keys none is skipped.
func TestTokenCasesSkipWithoutAToken(t *testing.T) {
	needing := 0
	for _, r := range s3Matrix([][]byte{[]byte("a")}, rand.New(rand.NewPCG(1, 1)), 10) {
		if r.TokenFrom != "" {
			needing++
		}
	}
	for _, c := range []struct {
		keys    []string
		skipped int
	}{{[]string{"a"}, needing}, {[]string{"a", "b"}, 0}} {
		base := startGo(t, false, c.keys...)
		client := &http.Client{}
		rep := &report{}
		d := &driver{o: options{bucket: "bkt", parallel: 2}, ref: &target{name: "a", base: base, client: client},
			got: &target{name: "b", base: base, client: client}, comp: bytesComparator{}, rep: rep}
		var keys [][]byte
		for _, k := range c.keys {
			keys = append(keys, []byte(k))
		}
		var tokenCases []request
		for _, r := range s3Matrix(keys, rand.New(rand.NewPCG(1, 1)), 10) {
			if r.TokenFrom != "" || strings.Contains(r.Name, "token page 1") {
				tokenCases = append(tokenCases, r)
			}
		}
		d.runCases("case", tokenCases)
		if len(rep.Skipped) != c.skipped || rep.Failed != 0 || rep.Passed+len(rep.Skipped) != len(tokenCases) {
			t.Errorf("%v: %d passed, %d failed, %d skipped; want %d skipped", c.keys, rep.Passed, rep.Failed, len(rep.Skipped), c.skipped)
		}
	}
}

// TestResidualsStandApart: a residual request that is answered is listed
// with both statuses and counted in neither Compared, Passed nor Failed;
// one whose request fails (here the second server is gone) is a failure
// of the run, not a residual.
func TestResidualsStandApart(t *testing.T) {
	base := startGo(t, false, "a", "b")
	client := &http.Client{}
	residual := []request{{Name: "residual", Method: "GET", Compare: "residual", Raw: "//{bucket}?list-type=2"}}

	rep := &report{}
	d := &driver{o: options{bucket: "bkt", parallel: 2}, ref: &target{name: "a", base: base, client: client},
		got: &target{name: "b", base: base, client: client}, comp: bytesComparator{}, rep: rep}
	d.runCases("case", residual)
	if len(rep.Residuals) != 1 || rep.Residuals[0] != "residual: reference 400, go 400" ||
		rep.Compared != 0 || rep.Passed != 0 || rep.Failed != 0 || len(rep.Results) != 0 {
		t.Errorf("answered: residuals %v, compared %d passed %d failed %d", rep.Residuals, rep.Compared, rep.Passed, rep.Failed)
	}

	gone, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	goneBase := "http://" + gone.Addr().String()
	gone.Close()
	rep = &report{}
	d = &driver{o: options{bucket: "bkt", parallel: 2}, ref: &target{name: "a", base: base, client: client},
		got: &target{name: "b", base: goneBase, client: client}, comp: bytesComparator{}, rep: rep}
	d.runCases("case", residual)
	if len(rep.Residuals) != 0 || rep.Compared != 1 || rep.Failed != 1 || rep.Results[0].Error == "" {
		t.Errorf("errored: residuals %v, compared %d failed %d, %+v", rep.Residuals, rep.Compared, rep.Failed, rep.Results)
	}
}

// wireServer answers every connection with the bytes of reply once the
// request head has arrived, then closes it, or holds it open when hold
// is set.
func wireServer(t *testing.T, reply string, hold bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil || line == "\r\n" {
						break
					}
				}
				io.WriteString(c, reply)
				if hold {
					time.Sleep(2 * time.Second)
				}
			}()
		}
	}()
	return "http://" + ln.Addr().String()
}

// TestWireAnswersMustBeWhole: a byte-stream request passes only on
// answers read whole up to a clean end of the connection: a body shorter
// than its Content-Length, a successor answer that does not parse and a
// connection held open past the deadline are errors, not the last answer
// read so far.
func TestWireAnswersMustBeWhole(t *testing.T) {
	defer func(d time.Duration) { wireDeadline = d }(wireDeadline)
	wireDeadline = 500 * time.Millisecond
	const ok = "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: 5\r\n\r\nhello"
	req := request{Method: "GET", Wire: func(b, h string) string {
		return "GET /" + b + "?list-type=2 HTTP/1.1\r\nHost: " + h + "\r\nConnection: close\r\n\r\n"
	}}
	for _, c := range []struct {
		name, reply string
		hold, pass  bool
	}{
		{"whole, then closed", ok, false, true},
		{"two whole answers", ok + ok, false, true},
		{"a body one byte short", strings.Replace(ok, "Content-Length: 5", "Content-Length: 6", 1), false, false},
		{"a whole answer, then a short one", ok + strings.Replace(ok, "Content-Length: 5", "Content-Length: 6", 1), false, false},
		{"a malformed successor", ok + "garbage\r\n\r\n", false, false},
		{"held open past the deadline", ok, true, false},
		{"nothing", "", false, false},
		{"a body framed by the close", "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\nhello", false, true},
		{"a chunked body cut short", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n", false, false},
	} {
		tg := &target{name: "x", base: wireServer(t, c.reply, c.hold), client: http.DefaultClient}
		resp, err := tg.do("bkt", req)
		if c.pass && (err != nil || resp == nil || string(resp.Body) != "hello") {
			t.Errorf("%s: %v %+v", c.name, err, resp)
		}
		if !c.pass && err == nil {
			t.Errorf("%s: answered %d %q, want an error", c.name, resp.Status, resp.Body)
		}
	}
}
