// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"flag"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestFlags: the engine's flag group (registered once by the command) has
// the command's defaults and fills the policy and the admission apart.
func TestFlags(t *testing.T) {
	for _, diag := range []bool{false, true} {
		f := flag.NewFlagSet("t", flag.ContinueOnError)
		c := Flags(f, diag)
		if !c.Admission.Limiter || c.Policy.Seed || c.Policy.Floors {
			t.Fatalf("defaults: %+v", *c)
		}
		args := []string{"-seed", "-request-rate", "5"}
		if diag {
			args = append(args, "-floors", "-sequential", "-max-keys", "9", "-limiter=false")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want, adm := Policy{Seed: true}, objhttp.Admission{Limiter: true, RequestRate: 5}
		if diag {
			want, adm = Policy{Seed: true, Floors: true, Sequential: true, MaxKeys: 9}, objhttp.Admission{RequestRate: 5}
		}
		if c.Policy != want || c.Admission != adm {
			t.Fatalf("diag=%v: %+v, want %+v %+v", diag, *c, want, adm)
		}
		if o := c.Policy.Options(); o.Seed != want.Seed || o.Floors != want.Floors || o.MaxKeys != want.MaxKeys {
			t.Fatalf("options %+v", o)
		}
	}
}
