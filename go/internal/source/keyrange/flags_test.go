// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"flag"
	"testing"
)

// TestFlagsRegisteredOnce: two providers on the engine registering their
// flag groups on one flag set share one policy flag group; nothing panics
// on a duplicate, both get the parsed policy, and the defaults hold.
func TestFlagsRegisteredOnce(t *testing.T) {
	for _, diag := range []bool{false, true} {
		f := flag.NewFlagSet("t", flag.ContinueOnError)
		ca, cb := Flags(f, diag), Flags(f, diag)
		if ca != cb {
			t.Fatal("a second registration made a second policy")
		}
		a, b := &ca.Policy, &cb.Policy
		if !a.SeedDescent || !ca.Admission.Limiter || a.Seed || a.Floors {
			t.Fatalf("defaults: %+v", *ca)
		}
		args := []string{"-seed"}
		if diag {
			args = append(args, "-floors", "-seed-descent=false", "-sequential", "-max-keys", "9", "-limiter=false")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want, limiter := Policy{Seed: true, SeedDescent: true}, true
		if diag {
			want, limiter = Policy{Seed: true, Floors: true, Sequential: true, MaxKeys: 9}, false
		}
		if cb.Admission.Limiter != limiter {
			t.Fatalf("diag=%v: limiter %v", diag, cb.Admission.Limiter)
		}
		if *b != want {
			t.Fatalf("diag=%v: %+v, want %+v", diag, *b, want)
		}
		if o := b.Options(); o.Seed != want.Seed || o.Floors != want.Floors || o.MaxKeys != want.MaxKeys || o.SeedDescent != want.SeedDescent {
			t.Fatalf("options %+v", o)
		}
	}
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	f.Bool("seed", false, "someone else's")
	defer func() {
		if recover() == nil {
			t.Fatal("a foreign -seed flag was taken for the engine's")
		}
	}()
	Flags(f, false)
}
