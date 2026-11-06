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
		a, b := Flags(f, diag), Flags(f, diag)
		if a != b {
			t.Fatal("a second registration made a second policy")
		}
		if !a.SeedDescent || a.Seed || a.Floors {
			t.Fatalf("defaults: %+v", *a)
		}
		args := []string{"-seed"}
		if diag {
			args = append(args, "-floors", "-seed-descent=false", "-sequential", "-max-keys", "9")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want := Policy{Seed: true, SeedDescent: true}
		if diag {
			want = Policy{Seed: true, Floors: true, Sequential: true, MaxKeys: 9}
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
