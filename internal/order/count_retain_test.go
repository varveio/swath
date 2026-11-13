// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"runtime"
	"testing"
	"weak"

	"github.com/varveio/swath/internal/source"
)

// heavy is a range holding memory, as a local directory's chunk holds the
// directory's names.
type heavy struct {
	name string
	data []byte
}

// retains emits a heavy range and then a checking one: Count pops the
// heavy range first (LIFO, children pushed in reverse), and while the
// checking range expands the heavy one must already be collectable.
type retains struct {
	t     *testing.T
	heavy weak.Pointer[heavy]
	freed bool
}

func (p *retains) Root() *heavy { return &heavy{name: "root"} }

func (p *retains) Expand(_ context.Context, r *heavy, e source.Emitter[*heavy, tp]) error {
	switch r.name {
	case "root":
		h := &heavy{name: "heavy", data: make([]byte, 1<<20)}
		p.heavy = weak.Make(h)
		if err := e.Child(h); err != nil {
			return err
		}
		return e.Child(&heavy{name: "check"})
	case "check":
		runtime.GC()
		p.freed = p.heavy.Value() == nil
	}
	return nil
}

// TestCountReleasesExpandedRanges: a range expanded and popped is not kept
// alive by the stack's spare capacity while later ranges run.
func TestCountReleasesExpandedRanges(t *testing.T) {
	p := &retains{t: t}
	if _, err := Count(context.Background(), p, nil, Options{Workers: 1}); err != nil {
		t.Fatal(err)
	}
	if !p.freed {
		t.Fatal("the expanded heavy range was still reachable while a later range ran")
	}
}
