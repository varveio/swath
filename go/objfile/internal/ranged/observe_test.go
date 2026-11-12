// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"context"
	"errors"
	"testing"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// TestObserved: each request a read makes is observed once, in order, with
// its kind, extent, attempt, pin and the bytes its body gave.
func TestObserved(t *testing.T) {
	type ev struct {
		kind   objfile.RequestKind
		off, n int64
		retry  bool
		bytes  int64
		failed bool
	}
	run := func(t *testing.T, s *store, read func(ctx context.Context) error, want []ev) {
		t.Helper()
		obs := &objfiletest.Observer{}
		if err := read(objfile.WithObserver(context.Background(), obs)); err != nil {
			t.Fatal(err)
		}
		reqs, ends := obs.Events()
		if len(ends) != len(reqs) {
			t.Fatalf("%d requests began, %d ended", len(reqs), len(ends))
		}
		var got []ev
		for i, q := range reqs {
			got = append(got, ev{q.Kind, q.Off, q.Len, q.Retry, ends[i].Bytes, ends[i].Err != nil})
		}
		if len(got) != len(want) {
			t.Fatalf("observed %+v, want %+v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("observed %+v, want %+v", got, want)
			}
		}
	}
	t.Run("CutBodyResumed", func(t *testing.T) {
		s := newStore(1000)
		s.cut = func(req int) int {
			if req == 2 {
				return 30
			}
			return -1
		}
		f := NewSized("obj", 1000, s.get, s.stat, false)
		run(t, s, func(ctx context.Context) error {
			if _, err := f.WithContext(ctx).ReadAt(make([]byte, 10), 0); err != nil {
				return err
			}
			_, err := f.WithContext(ctx).ReadAt(make([]byte, 100), 500)
			return err
		}, []ev{
			{objfile.RangeRequest, 0, 10, false, 10, false},
			{objfile.RangeRequest, 500, 100, false, 30, true},
			{objfile.RangeRequest, 530, 70, true, 70, false},
		})
	})
	t.Run("NoTotalFallsBack", func(t *testing.T) {
		s := newStore(1000)
		s.noTotal = true
		f := NewSized("obj", 1000, s.get, s.stat, false)
		run(t, s, func(ctx context.Context) error {
			_, err := f.WithContext(ctx).ReadAt(make([]byte, 10), 0)
			return err
		}, []ev{
			{objfile.RangeRequest, 0, 10, false, 0, false}, // let go unread
			{objfile.MetadataRequest, 0, 0, false, 0, false},
			{objfile.RangeRequest, 0, 10, false, 10, false},
		})
	})
	t.Run("ReadObjectCutResumed", func(t *testing.T) {
		s := newStore(1000)
		s.cut = func(req int) int {
			if req == 1 {
				return 400
			}
			return -1
		}
		run(t, s, func(ctx context.Context) error {
			_, _, err := ReadObject(ctx, "obj", 1000, false, s.get)
			return err
		}, []ev{
			{objfile.WholeRequest, 0, -1, false, 400, true},
			{objfile.RangeRequest, 400, 600, true, 600, false},
		})
	})
	t.Run("Metadata", func(t *testing.T) {
		s := newStore(1000)
		fail := errors.New("no")
		run(t, s, func(ctx context.Context) error {
			if _, _, err := Metadata(ctx, "obj", false, s.stat); err != nil {
				return err
			}
			if _, _, err := Metadata(ctx, "obj", false, func(context.Context) (int64, Pin, error) { return 0, Pin{}, fail }); err != fail {
				return errors.New("Metadata lost its error")
			}
			return nil
		}, []ev{
			{objfile.MetadataRequest, 0, 0, false, 0, false},
			{objfile.MetadataRequest, 0, 0, false, 0, true},
		})
	})
}

// BenchmarkReadAtObserved is BenchmarkReadAt with an Observer that counts.
func BenchmarkReadAtObserved(b *testing.B) {
	benchmarkReadAt(b, objfile.WithObserver(context.Background(), &counter{}))
}

type counter struct{ n int }

func (c *counter) Begin(objfile.Request) func(objfile.RequestResult) { c.n++; return nil }
func (*counter) BatchPlanned(_, _, _ int64)                          {}
func (*counter) CacheHit(int64)                                      {}
