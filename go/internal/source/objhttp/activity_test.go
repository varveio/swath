// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"testing"

	"github.com/varveio/swath/go/internal/source"
)

type (
	counting  struct{}
	living    struct{}
	answering struct{}
	everyPart struct {
		counting
		living
		answering
	}
)

func (counting) Counters() Counts { return Counts{Throttles: 1, Timeouts: 2, Attempts: 99} }
func (living) Live() Live {
	return Live{Attempts: 3, Retries: 4, Throttles: 5, Timeouts: 6, Inflight: 7, InflightMax: 8, InflightNs: 9, Limit: 10}
}
func (answering) Answered() int64 { return 11 }

// TestActivity: Observe reads each part of a client's activity through the
// interface that client implements, and a part it does not implement reads
// zero, independently of the others.
func TestActivity(t *testing.T) {
	full := source.Requests{Attempts: 3, Retries: 4, Throttles: 5, Timeouts: 6, Inflight: 7, InflightMax: 8, InflightNs: 9, Limit: 10}
	for _, c := range []struct {
		name               string
		client             any
		throttles, timeout int64
		requests           source.Requests
		answered           int64
	}{
		{"every part", everyPart{}, 1, 2, full, 11},
		{"none", struct{}{}, 0, 0, source.Requests{}, 0},
		{"counters only", counting{}, 1, 2, source.Requests{}, 0},
		{"live only", living{}, 0, 0, full, 0},
		{"answered only", answering{}, 0, 0, source.Requests{}, 11},
		{"nil", nil, 0, 0, source.Requests{}, 0},
	} {
		a := Observe(c.client)
		th, to := a.Faults()
		if th != c.throttles || to != c.timeout || a.Requests() != c.requests || a.Answered() != c.answered {
			t.Errorf("%s: faults %d, %d, requests %+v, answered %d", c.name, th, to, a.Requests(), a.Answered())
		}
	}
}

// TestActivityIsLive: the activity is read from the client at each call,
// not captured when observed.
func TestActivityIsLive(t *testing.T) {
	var c Counters
	a := Observe(&c)
	if a.Requests().Attempts != 0 {
		t.Fatal("attempts before any")
	}
	c.Attempt()()
	if got := a.Requests().Attempts; got != 1 {
		t.Fatalf("attempts %d after one, want 1", got)
	}
}
