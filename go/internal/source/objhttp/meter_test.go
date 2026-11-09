// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMeterDefaults(t *testing.T) {
	m := NewMeter(Retry{})
	if m.Retry.MaxAttempts != 9 || m.Retry.BaseBackoff != 100*time.Millisecond || m.Retry.MaxBackoff != 5*time.Second {
		t.Fatalf("defaults: %+v", m.Retry)
	}
	if m.Answered() != 0 || m.Live().Limit != 0 || m.Counters() != (Counts{}) {
		t.Fatalf("fresh meter: %+v", m.Counters())
	}
}

func TestMeterActivityIsLive(t *testing.T) {
	m := NewMeter(Retry{MaxAttempts: 2, BaseBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond})
	a := Observe(m)
	calls := 0
	_, err := Do(t.Context(), m.Retry, &m.Tally, func() (int, error) {
		defer m.Tally.Attempt()()
		calls++
		if live := a.Requests(); live.Attempts != int64(calls) || live.Inflight != 1 || a.Answered() != 0 {
			t.Fatalf("during attempt %d: %+v, answered %d", calls, live, a.Answered())
		}
		if calls == 1 {
			return 0, ErrTimeout
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	th, to := a.Faults()
	if live := a.Requests(); live.Attempts != 2 || live.Retries != 1 || live.Inflight != 0 || a.Answered() != 1 || th != 0 || to != 1 {
		t.Fatalf("completed: %+v, answered %d, faults %d/%d", live, a.Answered(), th, to)
	}
	if n := m.Counters(); n.Attempts != 2 || n.Retries != 1 || n.Timeouts != 1 || n.InflightMax != 1 || n.InflightNs <= 0 {
		t.Fatalf("counts: %+v", n)
	}
}

func TestMeterLimiterAndRate(t *testing.T) {
	l := NewLimiter(8)
	m := NewMeter(Retry{MaxAttempts: 1, Limiter: l})
	a := Observe(m)
	if a.Requests().Limit != 8 {
		t.Fatal("initial limit")
	}
	for range 40 {
		_, err := Do(t.Context(), m.Retry, &m.Tally, func() (int, error) {
			defer m.Tally.Attempt()()
			return 0, &Failure{Class: ClassThrottle, Throttle: true, Transient: true, Cause: errors.New("busy")}
		})
		if err == nil {
			t.Fatal("throttle succeeded")
		}
	}
	if a.Requests().Limit >= 8 || a.Requests().Limit != int64(l.Limit()) {
		t.Fatalf("live limit: %+v", a.Requests())
	}
	r := NewRate(1)
	if err := r.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = r.Wait(ctx)
	m.Retry.Rate = r
	n := m.Counters()
	if n.Limiter != l.Counts() || n.Limiter.Decreases == 0 || n.Rate != r.Counts() || n.Rate.Waits != 1 || n.Throttles != 40 {
		t.Fatalf("augmented counts: %+v", n)
	}
}
