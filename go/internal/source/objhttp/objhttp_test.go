// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
)

func TestCanonicalRoute(t *testing.T) {
	for in, want := range map[string]string{
		"HTTP://Host:1/storage/v1/b/x/o": "http://host:1/storage/v1/b/x/o",
		"https://B.S3.Amazonaws.com/":    "https://b.s3.amazonaws.com/",
		"http://h/Base//K":               "http://h/Base//K", // path bytes kept
		"http://H":                       "http://h",
		"no-scheme":                      "no-scheme",
	} {
		if got := CanonicalRoute(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// TestCredentialDeadlineIsRequeued: a transient credential failure that
// wraps a deadline (a token exchange's own timeout, the caller's context
// still live) classifies as timeout or transport, not canceled, and its
// exhaustion is requeued; a refused credential stays auth.
func TestCredentialDeadlineIsRequeued(t *testing.T) {
	none := func(error) (Class, bool) { return "", false }
	class := func(err error) Class { return Classify(err, none) }
	for _, c := range []struct {
		err  error
		want Class
	}{
		{&CredentialError{Transient: true, Err: fmt.Errorf("token: %w", context.DeadlineExceeded)}, ClassTransport},
		{&CredentialError{Transient: true, Timeout: true, Err: fmt.Errorf("wait: %w", context.DeadlineExceeded)}, ClassTimeout},
		{&CredentialError{Err: fmt.Errorf("invalid_grant: %w", context.DeadlineExceeded)}, ClassAuth},
		{fmt.Errorf("list: %w", context.Canceled), ClassCanceled},
	} {
		ex := &ExhaustedError{Attempts: 9, Err: c.err}
		if got := class(ex); got != c.want || Requeue(class)(ex) != c.want.Transient() {
			t.Errorf("%v: class %q requeue %v, want %q", c.err, got, Requeue(class)(ex), c.want)
		}
	}
}

// TestCounterTable: every ClientCounter before the sentinel has a
// definition with a distinct key, and Store writes each from its own
// field: Counts filled with distinct values land at distinct IDs.
func TestCounterTable(t *testing.T) {
	defs := CounterDefs("throttle", "-timeout")
	seen := map[string]bool{}
	for id, d := range defs {
		if d.Key == "" || seen[d.Key] {
			t.Fatalf("counter %d: key %q missing or repeated", id, d.Key)
		}
		seen[d.Key] = true
	}
	var n Counts
	v := int64(1)
	fill := func(rv reflect.Value) {
		for i := 0; i < rv.NumField(); i++ {
			if f := rv.Field(i); f.Kind() == reflect.Int64 {
				f.SetInt(v)
				v++
			}
		}
	}
	rv := reflect.ValueOf(&n).Elem()
	fill(rv)
	fill(rv.FieldByName("Limiter"))
	fill(rv.FieldByName("Rate"))
	if int(v-1) != int(NumCounters) {
		t.Fatalf("Counts has %d values, the table %d", v-1, NumCounters)
	}
	set := counters.New(defs)
	n.Store(set, 0)
	got := map[int64]bool{}
	for id := range defs {
		x := set.Load(counters.ID(id))
		if x == 0 || got[x] {
			t.Fatalf("counter %s: value %d unset or stored twice", defs[id].Key, x)
		}
		got[x] = true
	}
}

// TestAdmissionFeatures: slow start has its own feature state, so a run
// with the limiter but without slow start reports it off, not on and
// unengaged; a supplied client has every admission feature off.
func TestAdmissionFeatures(t *testing.T) {
	for _, c := range []struct {
		a   Admission
		own bool
		off []string
	}{
		{Admission{Limiter: true}, true, []string{FeatureSlowStart, FeatureRequestRate}},
		{Admission{Limiter: true, SlowStart: true, RequestRate: 5}, true, nil},
		{Admission{SlowStart: true}, true, []string{FeatureLimiter, FeatureSlowStart, FeatureRequestRate}},
		{Admission{Limiter: true, SlowStart: true, RequestRate: 5}, false, []string{FeatureLimiter, FeatureSlowStart, FeatureRequestRate}},
	} {
		if got := c.a.Off(c.own); fmt.Sprint(got) != fmt.Sprint(c.off) {
			t.Fatalf("%+v own %v: off %v, want %v", c.a, c.own, got, c.off)
		}
	}
	for _, d := range CounterDefs("t", "-t") {
		if strings.HasPrefix(d.Key, "slow_start.") != (d.Feature == FeatureSlowStart) {
			t.Fatalf("%s: feature %q", d.Key, d.Feature)
		}
	}
}
