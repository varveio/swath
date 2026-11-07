// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/output"
)

// contractThresholds are the Java replay server's answers, from its own
// GCS renderer over the same pages: for each layout (n entries, name
// lengths cycled, a mode) and each position k of one invalid entry, the
// least response cap at which the entry's fixture problem is reported
// rather than response_too_large, which is how many bytes the renderer
// had written to its sink before it validated entry k. The entry is an
// object named "." (reserved_name), or with mode prefix an empty prefix
// (name_length) after n valid objects; mode quote puts '"' and a TAB in
// every third name (the escaping path), mode full renders the empty acl.
// Runs are "k:threshold" where the threshold changes.
var contractThresholds = []struct {
	n          int
	lens, mode string
	runs       string
}{
	{60, "10", "-", "0:1 32:7994"},
	{40, "1000", "-", "0:1 7:7538 13:14990 19:22442 25:29894 31:37346 37:44798"},
	{30, "600", "-", "0:1 10:7664 19:15242 28:22820"},
	{30, "1024,20", "-", "0:1 11:7726 21:15366"},
	{40, "300", "quote", "0:1 13:6598 25:13110 37:19622"},
	{40, "40", "prefix", "0:7982"},
	{60, "50", "full", "0:1 27:7986 53:15986"},
	{50, "1001,999,7", "quote,full", "0:1 4:2848 7:5610 10:8372 13:11134 16:13896 19:16658 22:19420 25:22182 28:24944 31:27706 34:30468 37:33230 40:35992 43:38754 46:41516 49:44278"},
}

// layoutPage builds the probe page: entry i is "%05d" plus filler to its
// length, entry bad invalid.
func layoutPage(n int, lens []int, mode string, bad int) *Page {
	page := &Page{}
	for i := range n {
		l := lens[i%len(lens)]
		name := fmt.Sprintf("%05d", i)
		if strings.Contains(mode, "quote") && i%3 == 0 {
			name += "\"\t" + strings.Repeat("x", max(0, l-7))
		} else {
			name += strings.Repeat("x", max(0, l-5))
		}
		if !strings.Contains(mode, "prefix") && i == bad {
			name = "."
		}
		page.Items = append(page.Items, Item{Key: []byte(name), Size: 1})
		if strings.Contains(mode, "prefix") {
			p := []byte(name + "/")
			if i == bad {
				p = nil
			}
			page.Prefixes = append(page.Prefixes, p)
		}
	}
	return page
}

// renderUnderCap renders page under cap: "fixture", "too_large" or "ok".
func renderUnderCap(r *Request, page *Page, cap int) string {
	o, err := output.New(output.NewBudget(1<<40, nil), min(cap, 64), cap)
	if err != nil {
		panic(err)
	}
	defer o.Close()
	err = RenderPage(o, r, page)
	var p *FixtureProblem
	switch {
	case errors.As(err, &p):
		return "fixture"
	case o.Err() != nil:
		return "too_large"
	}
	return "ok"
}

// TestRenderOrderMatchesTheContract: with an invalid fixture row and a
// small response cap, the renderer reports what the Java server's does,
// at every position of the row: its problem, unless the bytes flushed to
// the sink before it was validated already passed the cap.
func TestRenderOrderMatchesTheContract(t *testing.T) {
	for _, c := range contractThresholds {
		var lens []int
		for _, f := range strings.Split(c.lens, ",") {
			n, _ := strconv.Atoi(f)
			lens = append(lens, n)
		}
		r := &Request{Bucket: "bucket", PageSize: 1000, FullProjection: strings.Contains(c.mode, "full")}
		want := map[int]int{}
		for _, run := range strings.Fields(c.runs) {
			k, v, _ := strings.Cut(run, ":")
			kk, _ := strconv.Atoi(k)
			want[kk], _ = strconv.Atoi(v)
		}
		threshold := 0
		for k := range c.n {
			if v, ok := want[k]; ok {
				threshold = v
			}
			page := layoutPage(c.n, lens, c.mode, k)
			if got := renderUnderCap(r, page, threshold); got != "fixture" {
				t.Errorf("%d %s %s, bad %d: cap %d gives %s", c.n, c.lens, c.mode, k, threshold, got)
			}
			if threshold > 1 {
				if got := renderUnderCap(r, page, threshold-1); got != "too_large" {
					t.Errorf("%d %s %s, bad %d: cap %d gives %s", c.n, c.lens, c.mode, k, threshold-1, got)
				}
			}
		}
	}
	// The review's case: a first object named "." under a 1-byte cap is
	// reserved_name, not the cap: the header is still staged.
	if got := renderUnderCap(&Request{Bucket: "b", PageSize: 1}, &Page{Items: []Item{{Key: []byte(".")}}}, 1); got != "fixture" {
		t.Errorf("first object '.' under a 1-byte cap: %s", got)
	}
}

// TestOversizedNativeMetadataStaysBounded renders an already-read page (so
// the fixture read is outside the measure) whose one native object has a
// 1 MiB content type, under an 8 KiB cap and an 8 KiB budget: the render
// stops at the cap, never charges past it, and allocates the staging
// buffer and the capped output, not the value's rendering. Staging the
// whole entry before writing it allocated over 1 MiB uncharged.
func TestOversizedNativeMetadataStaysBounded(t *testing.T) {
	big := strings.Repeat("t", 1<<20)
	gen := int64(7)
	page := &Page{Items: []Item{{Key: []byte("a"), Size: 1, Native: &Native{ContentType: &big, Generation: &gen}}}}
	r := &Request{Bucket: "bucket", PageSize: 1}
	budget := output.NewBudget(8192, nil)
	var o *output.Output
	measure := func(f func()) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	var err error
	allocated := measure(func() {
		o, err = output.New(budget, 4096, 8192)
		if err == nil {
			err = RenderPage(o, r, page)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Err() != output.ErrTooLarge {
		t.Errorf("render: %v", o.Err())
	}
	if budget.Peak() > 8192 {
		t.Errorf("peak charge %d over the cap", budget.Peak())
	}
	if allocated > 64<<10 {
		t.Errorf("rendering allocated %d bytes for a %d-byte value under an 8 KiB cap", allocated, len(big))
	}
	o.Close()
	if budget.Charged() != 0 {
		t.Errorf("%d still charged", budget.Charged())
	}
	t.Logf("allocated %d bytes", allocated)
}
