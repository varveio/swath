// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/varveio/swath/go/internal/schema"
)

type reviewEmitter[P any] struct{ rows []schema.Row[P] }

func (e *reviewEmitter[P]) Rows(rows []schema.Row[P]) error {
	e.rows = append(e.rows, rows...)
	return nil
}
func (e *reviewEmitter[P]) Child(Range) error { return errors.New("unexpected split") }

func TestReviewSuppressedTokenCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	p, err := New(Options{Workers: 1, Resume: &Resume{After: "kb%"}}, Description[Object]{
		Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach,
		AcceptBound: SafeKey,
	}, func(_ context.Context, q *Query) (*Page[Object], error) {
		calls++
		if calls > 8 {
			cancel()
			return nil, context.Canceled
		}
		token := "A"
		if q.Token == "A" {
			token = "B"
		}
		return &Page[Object]{Rows: []Row{{Key: "kb!"}}, Truncated: true, Token: token}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = p.expandRange(ctx, Range{lo: "kb", after: "kb%", open: true}, &reviewEmitter[Object]{})
	if !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("suppressed A/B token cycle was not rejected: calls=%d error=%v", calls, err)
	}
}

type reviewPayload struct{ Text string }

func TestReviewEmittedArenaOwnership(t *testing.T) {
	detach := func(rows []schema.Row[reviewPayload]) []schema.Row[reviewPayload] {
		out := make([]schema.Row[reviewPayload], len(rows))
		for i, r := range rows {
			out[i] = schema.Row[reviewPayload]{Key: strings.Clone(r.Key), Data: reviewPayload{Text: strings.Clone(r.Data.Text)}}
		}
		return out
	}
	p, err := New(Options{}, Description[reviewPayload]{Target: "test", Listing: Listing, Groups: true, Detach: detach}, nil)
	if err != nil {
		t.Fatal(err)
	}
	arena := "a" + strings.Repeat("b", 8<<20)
	page := []schema.Row[reviewPayload]{{Key: arena[:1]}, {Key: arena[:1]}, {Key: arena[1:2], Data: reviewPayload{Text: arena[1:]}}}
	var g groups[reviewPayload]
	emit, _, _ := p.take(&g, page, false, "")
	if len(emit) != 2 || len(g.held) != 1 {
		t.Fatal("unexpected transition")
	}
	if unsafe.StringData(emit[0].Key) == unsafe.StringData(arena) {
		t.Errorf("two tiny emitted rows still pin the original %d-byte arena after the tail was detached", len(arena))
	}
	if unsafe.SliceData(emit) == unsafe.SliceData(page) {
		t.Errorf("emitted prefix still owns the page array, including its hidden tail header referencing %d bytes", len(page[2].Data.Text))
	}
	// Native-sized metadata also exposes the fromPage==0 && owned path:
	// a detached B is emitted while a page of C is held. The emitted B's
	// combined backing array still contains all of C's row headers.
	g = groups[reviewPayload]{}
	p.take(&g, []schema.Row[reviewPayload]{{Key: "a"}, {Key: "a"}, {Key: "b"}}, false, "")
	if !g.owned {
		t.Fatal("B was not detached")
	}
	held := g.held
	page = make([]schema.Row[reviewPayload], 1000)
	arena = "c" + strings.Repeat("x", 1000*1024)
	for i := range page {
		page[i] = schema.Row[reviewPayload]{Key: arena[:1], Data: reviewPayload{Text: arena[1+i*1024 : 1+(i+1)*1024]}}
	}
	emit, _, _ = p.take(&g, page, false, "a")
	if len(emit) != 1 {
		t.Fatal("expected only the detached B")
	}
	if unsafe.SliceData(emit) != unsafe.SliceData(held) {
		t.Errorf("already detached B was combined with the excluded page's row headers")
	}
}

func TestReviewMaxGroupIncludesMiddle(t *testing.T) {
	p := newGroupEngine(t, &versioned{}, Options{})
	rows := []Row{{Key: "a"}}
	for range 100 {
		rows = append(rows, Row{Key: "b"})
	}
	rows = append(rows, Row{Key: "c"})
	var g groups[Object]
	p.take(&g, rows, true, "")
	st := p.Stats()
	if got := st.Get(cGroupsMaxRows); got != 100 {
		t.Fatalf("groups.max_rows=%d, want 100", got)
	}
}

func TestGroupRawProgressBounded(t *testing.T) {
	for _, sequential := range []bool{false, true} {
		t.Run(fmt.Sprint(sequential), func(t *testing.T) {
			calls := 0
			stop := errors.New("test end")
			p, err := New(Options{Workers: 1, Sequential: sequential, Resume: &Resume{After: "kb\x02"}}, Description[Object]{
				Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach, AcceptBound: SafeKey,
			}, func(context.Context, *Query) (*Page[Object], error) {
				calls++
				if calls > EmptyChainLive+101 {
					return nil, stop
				}
				return &Page[Object]{Rows: []Row{{Key: "kb\x01"}}, Truncated: true, Token: fmt.Sprint(calls)}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if sequential {
				err = p.expandSequential(context.Background(), &reviewEmitter[Object]{})
			} else {
				err = p.expandRange(context.Background(), Range{lo: "kb", after: "kb\x02", open: true}, &reviewEmitter[Object]{})
			}
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			advanced, chained := p.Frontier()
			if advanced != EmptyChainLive+1 || chained != 100 {
				t.Fatalf("calls=%d frontier=%d chained=%d", calls, advanced, chained)
			}
		})
	}
}

func TestGroupRawSuppressedOrder(t *testing.T) {
	calls := 0
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
		func(context.Context, *Query) (*Page[Object], error) {
			calls++
			k := "kb!"
			if calls > 1 {
				k = "ka"
			}
			return &Page[Object]{Rows: []Row{{Key: k}}, Truncated: true, Token: fmt.Sprint(calls)}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	err = p.expandRange(context.Background(), Range{lo: "k", after: "kb%", open: true}, &reviewEmitter[Object]{})
	if !errors.Is(err, ErrInvalidPage) || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestGroupMaximumAllEmissionPaths(t *testing.T) {
	rows := []Row{{Key: "a"}}
	for range 100 {
		rows = append(rows, Row{Key: "b"})
	}
	rows = append(rows, Row{Key: "c"})
	for _, mode := range []string{"sequential", "seed-eof"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			p, err := New(Options{Workers: 1, Sequential: mode == "sequential"}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
				func(context.Context, *Query) (*Page[Object], error) {
					calls++
					return &Page[Object]{Rows: append([]Row(nil), rows...)}, nil
				})
			if err != nil {
				t.Fatal(err)
			}
			e := &reviewEmitter[Object]{}
			if mode == "seed-eof" {
				_, err = p.seedEmit(&Page[Object]{Rows: rows}, "", "", e)
			} else {
				err = p.Expand(context.Background(), p.Root(), e)
			}
			if err != nil {
				t.Fatal(err)
			}
			st := p.Stats()
			if len(e.rows) != 102 || st.Get(cGroupsMaxRows) != 100 {
				t.Fatalf("rows=%d max=%d", len(e.rows), st.Get(cGroupsMaxRows))
			}
			if mode == "sequential" && calls != 1 {
				t.Fatal(calls)
			}
		})
	}
}

func TestGroupOverfetchRows(t *testing.T) {
	calls := 0
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
		func(context.Context, *Query) (*Page[Object], error) {
			calls++
			return &Page[Object]{Rows: []Row{{Key: "a"}, {Key: "b"}, {Key: "b"}, {Key: "c"}, {Key: "c"}}, Truncated: true, Token: "next"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	e := &reviewEmitter[Object]{}
	if err := p.expandRange(context.Background(), Range{hi: "b"}, e); err != nil {
		t.Fatal(err)
	}
	st := p.Stats()
	if len(e.rows) != 3 || calls != 1 || st.Get(cGroupsOverfetchRows) != 2 || st.Get(cOverfetchKeys) != 2 {
		t.Fatalf("rows=%d calls=%d stats=%+v", len(e.rows), calls, p.Stats())
	}
}
