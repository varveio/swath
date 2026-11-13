// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// reporting is an mstore that reports, or not, complete observation, as
// do its Objects.
type reporting struct {
	*mstore
	complete bool
}

func (r reporting) ReportsIO() bool { return r.complete }

func (r reporting) Open(ctx context.Context, name string) (objfile.Object, error) {
	f, err := r.mstore.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return reportingObject{f, r.complete}, nil
}

func (r reporting) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	f, err := r.mstore.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return reportingObject{f, r.complete}, nil
}

type reportingObject struct {
	objfile.Object
	complete bool
}

func (r reportingObject) ReportsIO() bool { return r.complete }

// TestReportsIOPreserved: every wrapper reports, on the Opener and on its
// Objects (by Open and OpenSized), what the store beneath reports.
func TestReportsIOPreserved(t *testing.T) {
	ctx := context.Background()
	for _, complete := range []bool{false, true} {
		st := newMstore()
		st.put("ds/obj", []byte("0123456789"))
		base := reporting{st, complete}
		restored, err := objfile.Restore(objfile.Root(base, "ds"), scope, []byte(`{"version":1,"scope":"`+scope+`","objects":{"obj":{"size":10,"identity":"1"}}}`))
		if err != nil {
			t.Fatal(err)
		}
		for name, o := range map[string]objfile.Opener{
			"Root":       objfile.Root(base, "ds"),
			"Limit":      objfile.Limit(objfile.Root(base, "ds"), 1),
			"Batch":      mustBatch(t, objfile.Root(base, "ds"), objfile.BatchPolicy{}),
			"KnownSizes": objfile.KnownSizes(objfile.Root(base, "ds"), map[string]int64{"obj": 10}),
			"Pin":        objfile.Pin(objfile.Root(base, "ds"), scope),
			"Restore":    restored,
		} {
			if got := o.(objfile.IOReporter).ReportsIO(); got != complete {
				t.Errorf("%s: Opener ReportsIO %v, want %v", name, got, complete)
			}
			for _, sized := range []bool{false, true} {
				var f objfile.Object
				if sized {
					f, err = o.OpenSized(ctx, "obj", 10)
				} else {
					f, err = o.Open(ctx, "obj")
				}
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if r, ok := f.(objfile.IOReporter); !ok || r.ReportsIO() != complete {
					t.Errorf("%s (sized %v): Object is an IOReporter %v, want ReportsIO %v", name, sized, ok, complete)
				}
			}
		}
	}
}

// TestObservedAfterAdmission: a request waiting for a Limit slot is not
// observed, and one cancelled while it waits never is.
func TestObservedAfterAdmission(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			st := newMstore()
			st.put("obj", []byte("0123456789"))
			o := objfile.Limit(st, 1)
			f, err := o.Open(context.Background(), "obj")
			if err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			st.mu.Lock()
			st.gate, st.first = gate, make(chan struct{})
			first := st.first
			st.mu.Unlock()
			held := make(chan error, 1)
			go func() { _, err := f.ReadAt(make([]byte, 4), 0); held <- err }() // holds the one slot
			<-first
			obs := &objfiletest.Observer{}
			ctx, cancel := context.WithCancel(objfile.WithObserver(context.Background(), obs))
			entered := make(chan struct{})
			var once sync.Once
			wctx := objfile.WithAdmissionWait(ctx, func() { once.Do(func() { close(entered) }) })
			waiting := make(chan error, 1)
			go func() {
				var err error
				if batch {
					err = objfile.ReadRanges(wctx, f, []objfile.Range{{Off: 0, Buf: make([]byte, 2)}, {Off: 6, Buf: make([]byte, 2)}})
				} else {
					_, err = f.WithContext(wctx).ReadAt(make([]byte, 2), 0)
				}
				waiting <- err
			}()
			<-entered // Limit has reached admission while the slot is held
			cancel()
			if err := <-waiting; !errors.Is(err, context.Canceled) {
				t.Fatalf("a read cancelled waiting for a slot = %v", err)
			}
			close(gate)
			if err := <-held; err != nil {
				t.Fatal(err)
			}
			if reqs, _ := obs.Events(); len(reqs) != 0 {
				t.Fatalf("requests observed before admission: %+v", reqs)
			}
		})
	}
}
