// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
)

// TestOversizedContentTypeStaysBounded renders a GCS recording's row whose
// content type is 1 MiB of '&' (each escaped to five bytes) under an 8 KiB
// cap and an 8 KiB budget: the render stops at the cap, never charges past
// it, and allocates the staging buffer and the capped output, not the
// value's rendering. Escaping the value into the staged entry before
// writing it allocated over 5 MiB uncharged.
func TestOversizedContentTypeStaysBounded(t *testing.T) {
	big := strings.Repeat("&", 1<<20)
	res := &Result{Request: parse(t, ""), Entries: []Entry{{Object: &store.Object{Key: []byte("a"), ContentType: &big}}}}
	budget := output.NewBudget(8192, nil)
	var o *output.Output
	var err error
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	o, err = output.New(budget, 4096, 8192)
	if err == nil {
		err = RenderList(o, res, "http://h:1/replay/")
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
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
		t.Errorf("rendering allocated %d bytes for a %d-byte content type under an 8 KiB cap", allocated, len(big))
	}
	o.Close()
	if budget.Charged() != 0 {
		t.Errorf("%d still charged", budget.Charged())
	}
	t.Logf("allocated %d bytes", allocated)
}
