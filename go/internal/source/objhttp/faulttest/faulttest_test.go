// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package faulttest

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecideIsDeterministicAndProportional(t *testing.T) {
	p := Plan{Seed: 7, Windows: []Window{{From: 0, To: time.Second, Fraction: 0.3, Fault: Throttle}}}
	q := p
	hits := 0
	for n := uint64(0); n < 20000; n++ {
		a, _, _ := p.Decide(n, 0)
		b, _, _ := q.Decide(n, 0)
		if a != b {
			t.Fatalf("request %d: same plan, different decision", n)
		}
		if a {
			hits++
		}
		if c, _, _ := p.Decide(n, time.Second); c {
			t.Fatalf("request %d throttled outside its window", n)
		}
	}
	if f := float64(hits) / 20000; math.Abs(f-0.3) > 0.02 {
		t.Fatalf("throttled share %.3f, want 0.3", f)
	}
	other := Plan{Seed: 8, Windows: p.Windows}
	same := 0
	for n := uint64(0); n < 1000; n++ {
		a, _, _ := p.Decide(n, 0)
		b, _, _ := other.Decide(n, 0)
		if a == b {
			same++
		}
	}
	if same == 1000 {
		t.Fatal("a different seed made the same decisions")
	}
	byN := Plan{Windows: []Window{{N0: 5, N1: 8, Fraction: 1, Fault: Throttle}}}
	for n := uint64(0); n < 12; n++ {
		if a, _, _ := byN.Decide(n, time.Hour); a != (n >= 5 && n < 8) {
			t.Fatalf("request %d: throttled %v by a [5, 8) window", n, a)
		}
	}
}

func TestProxyFaults(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer up.Close()
	get := func(p *Proxy) (int, string, error) {
		srv := httptest.NewServer(p)
		defer srv.Close()
		resp, err := http.Get(srv.URL + "/b?list-type=2")
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), err
	}
	mk := func(plan Plan) *Proxy {
		p, err := New(up.URL, plan)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if code, body, err := get(mk(Plan{})); err != nil || code != 200 || body != "ok" {
		t.Fatalf("forward: %d %q %v", code, body, err)
	}
	all := func(f Fault, d time.Duration) []Window { return []Window{{Fraction: 1, Fault: f, Delay: d}} }
	if code, body, _ := get(mk(Plan{Windows: all(Throttle, 0)})); code != 503 || !strings.Contains(body, "<Code>SlowDown</Code>") {
		t.Fatalf("s3 throttle: %d %q", code, body)
	}
	if code, body, _ := get(mk(Plan{Dialect: "gcs", Windows: all(Throttle, 0)})); code != 429 || !strings.Contains(body, "rateLimitExceeded") {
		t.Fatalf("gcs throttle: %d %q", code, body)
	}
	if _, _, err := get(mk(Plan{Windows: all(Reset, 0)})); err == nil {
		t.Fatal("reset: the request succeeded")
	}
	p := mk(Plan{Windows: all(Latency, 150*time.Millisecond)})
	t0 := time.Now()
	if code, _, err := get(p); err != nil || code != 200 || time.Since(t0) < 150*time.Millisecond {
		t.Fatalf("latency: %d %v after %s", code, err, time.Since(t0))
	}
	if s := p.Stats(); s.Delayed != 1 || s.Forwarded != 1 || len(p.Timeline()) == 0 {
		t.Fatalf("stats %+v", s)
	}
	capped := mk(Plan{RateCap: 2})
	codes := map[int]int{}
	for range 5 {
		c, _, _ := get(capped)
		codes[c]++
	}
	if codes[200] != 2 || codes[503] != 3 {
		t.Fatalf("rate cap 2/s: %v", codes)
	}
}

func TestParseWindow(t *testing.T) {
	w, err := ParseWindow("latency:5s-:0.5:200ms")
	if err != nil || w != (Window{From: 5 * time.Second, Fraction: 0.5, Fault: Latency, Delay: 200 * time.Millisecond}) {
		t.Fatalf("%+v %v", w, err)
	}
	for _, bad := range []string{"throttle:1s:1", "storm:0s-1s:1", "throttle:0s-1s:2", "latency:0s-1s:1"} {
		if _, err := ParseWindow(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestAnsweredRequestLeavesTheCount: a client that sends one request at a
// time, the next as soon as it has an answer, is never seen with two in
// flight, even when the handler stalls after the answer is released (a
// descheduled handler on a loaded host). Each fault ends a request
// differently: a reset is visible to the client at once, a throttle and a
// forwarded page when the handler's buffered answer is flushed.
func TestAnsweredRequestLeavesTheCount(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat("page ", 4000)) // more than one write buffer
	}))
	defer up.Close()
	for _, f := range []Fault{Reset, Throttle, Latency} {
		t.Run(f.String(), func(t *testing.T) {
			p, err := New(up.URL, Plan{Seed: 3, Windows: []Window{{Fraction: 0.5, Fault: f, Delay: time.Millisecond}}})
			if err != nil {
				t.Fatal(err)
			}
			p.answered = func() { time.Sleep(20 * time.Millisecond) }
			srv := httptest.NewServer(p)
			defer srv.Close()
			c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}} // a reset is not retried by the transport
			for range 20 {
				resp, err := c.Get(srv.URL + "/b?list-type=2")
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			if s := p.Stats(); s.MaxActive != 1 || s.Requests != 20 {
				t.Fatalf("one at a time: %+v", s)
			}
		})
	}
}

// TestHTTPSTargetIsSentItsOwnHost: a request forwarded to an https
// provider endpoint carries the endpoint's host, not the proxy's, so a
// virtual-hosted service routes it; a plain http target keeps the
// client's Host as before.
func TestHTTPSTargetIsSentItsOwnHost(t *testing.T) {
	for _, c := range []struct{ target, host string }{
		{"https://s3.us-east-1.amazonaws.com", "s3.us-east-1.amazonaws.com"},
		{"http://127.0.0.1:9", "127.0.0.1:19091"},
	} {
		p, err := New(c.target, Plan{})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("GET", "http://127.0.0.1:19091/b?list-type=2", nil)
		p.next.Director(r)
		if r.Host != c.host || r.URL.Scheme+"://"+r.URL.Host != c.target {
			t.Errorf("%s: forwarded to %s://%s with Host %q", c.target, r.URL.Scheme, r.URL.Host, r.Host)
		}
	}
	if _, err := New("ftp://host", Plan{}); err == nil {
		t.Error("an ftp target was accepted")
	}
}

// TestMaxBodyBoundsTheBuffer: with MaxBody set, an upstream body of
// exactly MaxBody bytes is forwarded whole; a longer one, with or
// without a Content-Length, is read no further than MaxBody+1 bytes and
// answered 502 with BodyLimitHeader, never as a cut 200.
func TestMaxBodyBoundsTheBuffer(t *testing.T) {
	var read atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		if r.URL.Query().Get("cl") != "" {
			w.Header().Set("Content-Length", strconv.Itoa(n))
		}
		w.(http.Flusher).Flush()
		for i := 0; i < n; i += 1000 {
			k, err := io.WriteString(w, strings.Repeat("x", min(1000, n-i)))
			read.Add(int64(k))
			if err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	defer up.Close()
	p, err := New(up.URL, Plan{MaxBody: 5000})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	defer srv.Close()
	get := func(q string) (int, string, string) {
		resp, err := http.Get(srv.URL + "/b?" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get(BodyLimitHeader), string(b)
	}
	if code, mark, body := get("n=5000"); code != 200 || mark != "" || len(body) != 5000 {
		t.Errorf("at the limit: %d %q %d bytes", code, mark, len(body))
	}
	for _, q := range []string{"n=5001", "n=20000000", "n=20000000&cl=1"} {
		read.Store(0)
		code, mark, body := get(q)
		if code != http.StatusBadGateway || mark != "5000" || !strings.Contains(body, "over 5000 bytes") {
			t.Errorf("%s: %d %q %q", q, code, mark, body)
		}
		// The upstream handler may have written ahead into its socket
		// buffers, never the whole 20 MB.
		if n := read.Load(); n > 8<<20 {
			t.Errorf("%s: the upstream wrote %d bytes", q, n)
		}
	}
}
