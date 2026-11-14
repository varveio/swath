// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// fencingProvider asks its emitter whether the drain is at its range,
// declares a span and a fence on it and reports a replay range.
type fencingProvider struct{}

func (fencingProvider) Root() any { return 0 }
func (fencingProvider) Expand(_ context.Context, _ any, e source.Emitter[any, retPayload]) error {
	if a, ok := e.(source.Awaiter); ok {
		a.Awaited()
	}
	f, ok := e.(source.Fencer)
	if !ok {
		return nil
	}
	f.Span("a", "z", false)
	f.Fence("m", false)
	return nil
}
func (fencingProvider) Fences() bool             { return true }
func (fencingProvider) Replay(r any) (int, bool) { return 7, true }

// fenceSink is an emitter that records the fences it is handed and the
// questions whether the drain is at its range.
type fenceSink struct{ spans, fences, awaited int }

func (*fenceSink) Rows([]schema.Row[retPayload]) error { return nil }
func (*fenceSink) Child(any) error                     { return nil }
func (s *fenceSink) Span(string, string, bool)         { s.spans++ }
func (s *fenceSink) Fence(string, bool)                { s.fences++ }
func (s *fenceSink) Awaited() bool                     { s.awaited++; return true }

// TestWrappersForwardDurableSpill: every provider wrapper between a
// provider and the order stage (the filters, the object sizes) forwards
// what durable spill needs: Fences, Replay, and the emitter's Span and
// Fence; and the emitter's Awaited, the split account's. The sizes wrapper of the 0.4 CLI did not, and durable spill
// silently captured nothing (no fence reached the order stage).
func TestWrappersForwardDurableSpill(t *testing.T) {
	var n atomic.Int64
	size := func(*retPayload) (int64, bool) { return 1, true }
	for name, p := range map[string]source.Provider[any, retPayload]{
		"filtered":        newFiltered[retPayload](fencingProvider{}, func(*schema.Row[retPayload]) bool { return true }, &retSchema, &n),
		"sized":           newSized[retPayload](fencingProvider{}, size, &n),
		"sized(filtered)": newSized(newFiltered[retPayload](fencingProvider{}, func(*schema.Row[retPayload]) bool { return true }, &retSchema, &n), size, &n),
	} {
		if f, ok := p.(source.Fencing); !ok || !f.Fences() {
			t.Errorf("%s: Fences not forwarded", name)
		}
		if rp, ok := p.(source.Replaying[any]); !ok {
			t.Errorf("%s: Replay not forwarded", name)
		} else if c, ok := rp.Replay(0); !ok || c != 7 {
			t.Errorf("%s: Replay = %d, %v", name, c, ok)
		}
		var s fenceSink
		if err := p.Expand(context.Background(), p.Root(), &s); err != nil {
			t.Fatal(err)
		}
		if s.spans != 1 || s.fences != 1 || s.awaited != 1 {
			t.Errorf("%s: %d spans, %d fences and %d awaited questions reached the emitter, want 1 each", name, s.spans, s.fences, s.awaited)
		}
	}
}
