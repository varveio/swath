// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/objhttp/faulttest"
)

// counting counts the listing responses a Lister returned.
type counting struct {
	l         Lister
	responses atomic.Int64
}

func (c *counting) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	out, err := c.l.List(ctx, in)
	if err == nil {
		c.responses.Add(1)
	}
	return out, err
}

// replayClient is a client of the replay's Azure route at endpoint (the
// server's base URL, without the account).
func replayClient(t *testing.T, endpoint, account string) *client.Client {
	t.Helper()
	c, err := client.New(context.Background(), client.Config{Endpoint: endpoint, Account: account, Anonymous: true, Workers: 1,
		Retry: objhttp.Retry{BaseBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// list is q's names through a, asking any failed page again up to
// retries times (transport faults surface after the client's retries).
func list(t *testing.T, a *Adapter, q keyrange.Query, retries int) []string {
	t.Helper()
	items, err := flat(walk(a, q, retries, func(error) bool { return true }))
	if err != nil {
		t.Fatalf("%s: %v", short(q), err)
	}
	return names(items)
}

// The full §7.3 (S3 route, shared fields, workers, seed and structure
// modes, faults, crash and resume) and §7.4 belong to the wiring PR.
func TestDualRoute(t *testing.T) {
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	endpoint := strings.TrimSuffix(r.Endpoint, "/"+r.Account)
	cl := &counting{l: replayClient(t, endpoint, r.Account)}
	a := New(cl, r.Container, nil, 0)
	check := func(q keyrange.Query, got []string) {
		t.Helper()
		if want := oracle(keys, q); !slices.Equal(got, want) {
			t.Fatalf("%s: %d names, want %d (first difference at %d)", short(q), len(got), len(want), firstDifference(got, want))
		}
	}
	for _, max := range []int{1, 2, 7, 5000} {
		if max == 1 && testing.Short() {
			continue
		}
		q := keyrange.Query{Max: max}
		check(q, list(t, a, q, 0))
	}
	for _, max := range []int{7, 5000} {
		q := keyrange.Query{Delimiter: "/", Max: max}
		check(q, list(t, a, q, 0))
	}
	for _, prefix := range []string{"d/", "n1/n2/n3/", "r", "r\U00010000", "only-s/", "only-e/", "p"} {
		for _, d := range []string{"", "/"} {
			q := keyrange.Query{Prefix: prefix, Delimiter: d, Max: 7}
			check(q, list(t, a, q, 0))
		}
	}
	// Bounds at the corpus's names with a byte in EE..F4: from each to the
	// next in byte order, up to each from the one before, and each alone,
	// at Max 7 and 5,000. Pairs that straddle the 5,000 filler names
	// "f/..." are left out: the filler is listed whole above.
	var high []string
	for _, k := range keys {
		if strings.ContainsFunc(k, func(r rune) bool { return r >= 0xE000 }) {
			high = append(high, k)
		}
	}
	if len(high) < 30 {
		t.Fatalf("%d names at U+E000 and above", len(high))
	}
	for i := 1; i < len(high); i++ {
		lo, hi := high[i-1], high[i]
		if lo < "f/" && hi > "f/" {
			continue
		}
		for _, max := range []int{7, 5000} {
			for _, q := range []keyrange.Query{
				{After: lo, Through: hi, Bounded: true, Max: max},
				{After: lo + "x", Through: hi + "x", Bounded: true, Max: max},
				{After: lo, Through: lo, Bounded: true, Max: max},
				{Prefix: string([]rune(lo)[:1]), After: lo, Through: hi, Bounded: true, Delimiter: "/", Max: max},
			} {
				check(q, list(t, a, q, 0))
			}
		}
	}
	if a.Len() != 0 {
		t.Errorf("%d chains registered", a.Len())
	}
	if n, attempts := cl.responses.Load(), cl.l.(*client.Client).Live().Attempts; n > attempts {
		t.Errorf("%d responses, %d attempts", n, attempts)
	}
	t.Logf("%d responses: %d spans beyond the first, %d splits, %d seeks, %d probes, %d overlap rows, stack max %d, buffer max %d",
		cl.responses.Load(), a.Counter(CSpans), a.Counter(CSplits), a.Counter(CSeeks), a.Counter(CProbes), a.Counter(COverlapRows),
		a.Counter(CStackMax), a.Counter(CBufferRowsMax))

	px, err := faulttest.New(endpoint, faulttest.Plan{Seed: 7, Dialect: "azure", Windows: []faulttest.Window{
		{Fraction: 0.2, Fault: faulttest.Throttle}, {Fraction: 0.05, Fault: faulttest.Reset}}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(px)
	t.Cleanup(srv.Close)
	faulty := replayClient(t, srv.URL, r.Account)
	fa := New(faulty, r.Container, nil, 0)
	q := keyrange.Query{Max: 7}
	check(q, list(t, fa, q, 100))
	if st := px.Stats(); st.Throttled == 0 || st.Reset == 0 {
		t.Errorf("the faults never engaged: %+v", st)
	}
}

// short renders q with its strings cut to 40 bytes (the corpus's names
// run to 3,072).
func short(q keyrange.Query) string {
	cut := func(s string) string {
		if len(s) > 40 {
			return fmt.Sprintf("%q...(%d bytes)", s[:40], len(s))
		}
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("prefix=%s delimiter=%q after=%s through=%s bounded=%v max=%d", cut(q.Prefix), q.Delimiter, cut(q.After), cut(q.Through), q.Bounded, q.Max)
}

func firstDifference(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}
