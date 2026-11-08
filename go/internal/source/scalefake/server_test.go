// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package scalefake

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestBehaviourDelays: each request's wait is its page's slow prefix
// (the longest matching the key it starts at; its bound past the last
// key) or the latency, the first request's FirstLatency instead, plus the
// token or start latency, plus a jitter that is the same for the same
// request number and seed.
func TestBehaviourDelays(t *testing.T) {
	k, err := FromKeys([]string{"a/1", "d/000/1", "d/000/2", "d/1/1", "e"})
	if err != nil {
		t.Fatal(err)
	}
	ms := time.Millisecond
	s := NewServer(k, Options{Latency: 50 * ms, FirstLatency: 5 * time.Second, TokenLatency: 200 * ms, StartLatency: 7 * ms,
		Slow: []Slow{{"d/", 300 * ms}, {"d/000", 2 * time.Second}}})
	for i, c := range []struct {
		q    Query
		want time.Duration
	}{
		{Query{}, 5*time.Second + 7*ms}, // the first request
		{Query{}, 57 * ms},
		{Query{After: "c"}, 2*time.Second + 7*ms}, // the page starts at d/000/1
		{Query{After: "d/000/2"}, 307 * ms},
		{Query{Prefix: "d/1/"}, 307 * ms},
		{Query{After: "d/000/0", Token: "x"}, 2*time.Second + 200*ms},
		{Query{After: "e"}, 57 * ms},   // past the last key: its bound
		{Query{After: "d/2"}, 57 * ms}, // starts at e
		{Query{After: "zz"}, 57 * ms},
	} {
		if got := s.delay(uint64(i+1), c.q); got != c.want {
			t.Errorf("request %d %+v: delay %v, want %v", i+1, c.q, got, c.want)
		}
	}
	past := NewServer(k, Options{Latency: 50 * ms, Slow: []Slow{{"zz", time.Second}}})
	if got := past.delay(1, Query{After: "zzz"}); got != time.Second {
		t.Errorf("past the last key: %v, want the bound's slow prefix", got)
	}
	j := Options{Latency: 20 * ms, Jitter: 480 * ms, FaultSeed: 3}
	a, b := NewServer(k, j), NewServer(k, j)
	spread := map[time.Duration]bool{}
	for seq := uint64(1); seq <= 50; seq++ {
		d := a.delay(seq, Query{})
		if d != b.delay(seq, Query{}) || d < 20*ms || d >= 500*ms {
			t.Fatalf("jitter: request %d waits %v", seq, d)
		}
		spread[d] = true
	}
	if len(spread) < 40 {
		t.Fatalf("jitter: %d distinct delays in 50", len(spread))
	}
}

// TestBehaviourAnswers: a page cap shortens pages whatever max-keys asks;
// a rate cap, on a driven clock, answers the request past its burst with
// SlowDown at once, counts it, and admits again once the bucket refills; the 503 fraction is drawn per request number, so two servers
// with one seed fail the same requests.
func TestBehaviourAnswers(t *testing.T) {
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = string(rune('A'+i/26)) + string(rune('a'+i%26))
	}
	k, err := FromKeys(keys)
	if err != nil {
		t.Fatal(err)
	}
	get := func(url string) (int, int) {
		t.Helper()
		resp, err := http.Get(url + "/bkt?list-type=2&max-keys=1000")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var page struct{ KeyCount int }
		if resp.StatusCode == http.StatusOK {
			if err := xml.NewDecoder(resp.Body).Decode(&page); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, page.KeyCount
	}
	capped := httptest.NewServer(NewServer(k, Options{PageCap: 7}))
	defer capped.Close()
	if code, n := get(capped.URL); code != 200 || n != 7 {
		t.Fatalf("page cap: %d, %d keys", code, n)
	}
	// One request a second, a burst of one, on a clock the test drives:
	// the first request takes the token; the second, at the same instant,
	// is throttled at once (before any delay); a second later the bucket
	// has refilled and the third is answered.
	clock := time.Unix(1_700_000_000, 0)
	rated := NewServer(k, Options{Rate: 1, Now: func() time.Time { return clock }})
	rs := httptest.NewServer(rated)
	defer rs.Close()
	if code, _ := get(rs.URL); code != 200 {
		t.Fatalf("rate cap: first request %d", code)
	}
	if code, _ := get(rs.URL); code != http.StatusServiceUnavailable {
		t.Fatalf("rate cap: second request %d, want 503", code)
	}
	clock = clock.Add(time.Second)
	if code, _ := get(rs.URL); code != 200 {
		t.Fatalf("rate cap: third request, a second later, %d", code)
	}
	if st := rated.Stats(false); st.Throttled != 1 || st.Requests != 3 || st.Pages != 2 {
		t.Fatalf("rate cap: stats %+v", st)
	}
	f := Options{Fail503: 0.3, FaultSeed: 9}
	a, b := NewServer(k, f), NewServer(k, f)
	n := 0
	for seq := uint64(1); seq <= 1000; seq++ {
		if a.fail(seq) != b.fail(seq) {
			t.Fatal("503 draws differ between servers with one seed")
		}
		if a.fail(seq) {
			n++
		}
	}
	if n < 250 || n > 350 {
		t.Fatalf("%d of 1000 requests fail at 0.3", n)
	}
}
