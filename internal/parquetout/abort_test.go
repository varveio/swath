// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/dataset"
)

// sealed is an artifact that keeps nothing and reports its seal.
type sealed struct {
	discard
	once *sync.Once
	ch   chan struct{}
}

func (s sealed) Sync() error { s.once.Do(func() { close(s.ch) }); return nil }

// TestAbortKeepsThePublicationTurn: parts publish, and add their page ends
// to the cut file's cuts, in ordinal order, each after the part before it.
// Part 1 is publishing when the run aborts; part 2 sees the abort and
// returns without waiting for part 1; part 3, sealed before the abort,
// waits for part 2 and must not take the turn from part 1 (CI, 2026-11-15:
// on a GCS listing that failed, part 3's Cuts.Part raced part 1's under
// -race). Only the parts published before the abort are in the cuts.
func TestAbortKeepsThePublicationTurn(t *testing.T) {
	inPublish, hold1, hold2 := make(chan struct{}), make(chan struct{}), make(chan struct{})
	seal3 := sealed{once: new(sync.Once), ch: make(chan struct{})}
	opt := Options{Out: t.TempDir(), PartSize: 1 << 20, MaxRows: 1, Encoders: 5,
		Resume: &Resume{Limit: 1 << 20}, // a frozen ratio: no cut waits for its part
		Publish: func(ordinal int, p dataset.Part, _, _ int64, _ bool) (dataset.Part, error) {
			if ordinal == 1 {
				close(inPublish)
				<-hold1
			}
			return p, nil
		},
		Create: func(ordinal int) (dataset.Artifact, error) {
			switch ordinal {
			case 2:
				<-hold2
			case 3:
				return seal3, nil
			}
			return discard{}, nil
		}}
	r, err := NewRouter(opt, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	// A row a part; part i is closed by the write after it.
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := r.Write(context.Background(), [][]row{{{Key: k, Data: tr{Kind: "FILE"}}}}); err != nil {
			t.Fatal(err)
		}
	}
	<-inPublish
	<-seal3.ch
	aborted := make(chan struct{})
	go func() {
		r.Abort()
		close(aborted)
	}()
	for !r.aborted.Load() {
		runtime.Gosched()
	}
	close(hold2) // part 2 returns on the abort, part 3 follows it
	close(hold1) // part 1 publishes
	<-aborted
	file, err := r.Cuts().File()
	if err != nil || string(file) != "a\t1\nb\t1\n" {
		t.Fatalf("cuts %q (%v), want parts 0 and 1 only", file, err)
	}
}
