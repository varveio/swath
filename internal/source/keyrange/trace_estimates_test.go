// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/runtrace"
	"github.com/varveio/swath/internal/source"
)

// TestTraceEstimatesInRange: a gauge over a page of long keys can estimate
// more keys than an int64 holds; such an estimate is left out of the split
// event, never written as a wrapped negative count (reproduced: 200 keys
// of 60 zeros and a counter, a prior cut at "1", pages of 10, 4 workers).
func TestTraceEstimatesInRange(t *testing.T) {
	var keys []string
	for i := range 200 {
		keys = append(keys, strings.Repeat("0", 60)+fmt.Sprintf("%04d", i))
	}
	splits := 0
	for _, counted := range []bool{false, true} {
		p, err := newTest(newSorted(keys, true, 1), Options{Workers: 4, PageSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.SeedFrom(&source.Prior{Source: "cuts", Unscoped: true, Cuts: []source.Cut{{Key: "1", Rows: 200}}, Counted: counted}); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		w := runtrace.New(&buf)
		p.TraceTo(w)
		c := &collect{}
		if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 4, Budget: 500 * 256}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(time.Second, "ok", ""); err != nil {
			t.Fatal(err)
		}
		evs := decode(t, buf.Bytes())
		splits += checkTrace(t, fmt.Sprintf("counted=%v", counted), evs, keysOf(c.rows))
		for _, e := range evs {
			for _, f := range []string{"remainder", "est_lower", "est_upper", "expected"} {
				if v, ok := e[f].(float64); ok && v < 0 {
					t.Fatalf("counted=%v: %s %s = %v: an estimate wrapped past int64", counted, e.name(), f, v)
				}
			}
		}
	}
	if splits == 0 {
		t.Fatal("no split: the estimates were not exercised")
	}
}
