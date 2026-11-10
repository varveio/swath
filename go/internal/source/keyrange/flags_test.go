// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"flag"
	"io"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestFlags: the engine's flag group (registered once by the command) has
// the command's defaults and fills the policy and the admission apart.
func TestFlags(t *testing.T) {
	for _, diag := range []bool{false, true} {
		f := flag.NewFlagSet("t", flag.ContinueOnError)
		c := Flags(f, diag)
		if !c.Admission.Limiter || !c.Policy.Seed || c.Policy.Floors || c.Policy.Structure != StructureAuto {
			t.Fatalf("defaults: %+v", *c)
		}
		args := []string{"-limiter=false", "-request-rate", "5"}
		if diag {
			args = append(args, "-seed=false", "-limiter-slow-start", "-floors", "-sequential", "-max-keys", "9", "-structure=off")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want, adm := Policy{Seed: true, Structure: StructureAuto}, objhttp.Admission{RequestRate: 5}
		if diag {
			want, adm = Policy{Floors: true, Sequential: true, MaxKeys: 9}, objhttp.Admission{SlowStart: true, RequestRate: 5}
		}
		if c.Policy != want || c.Admission != adm {
			t.Fatalf("diag=%v: %+v, want %+v %+v", diag, *c, want, adm)
		}
		if o := c.Policy.Options(); o.Seed != want.Seed || o.Floors != want.Floors || o.MaxKeys != want.MaxKeys {
			t.Fatalf("options %+v", o)
		}
	}
}

// TestStructureFlag: --structure takes auto, on (true, or bare) and off
// (false); anything else is refused.
func TestStructureFlag(t *testing.T) {
	for _, c := range []struct {
		args []string
		want Structure
	}{{nil, StructureAuto}, {[]string{"-structure"}, StructureOn}, {[]string{"-structure=on"}, StructureOn},
		{[]string{"-structure=false"}, StructureOff}, {[]string{"-structure=off"}, StructureOff}, {[]string{"-structure=auto"}, StructureAuto}} {
		f := flag.NewFlagSet("t", flag.ContinueOnError)
		p := Flags(f, true)
		if err := f.Parse(c.args); err != nil || p.Policy.Structure != c.want {
			t.Fatalf("%q: %v %v", c.args, p.Policy.Structure, err)
		}
	}
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	Flags(f, true)
	if err := f.Parse([]string{"-structure=sometimes"}); err == nil {
		t.Fatal("-structure=sometimes accepted")
	}
}

// TestStructureFlagSyntax: the bare flag means on, so a mode needs '=':
// the usage shows --structure=auto|on|off and no separate argument.
func TestStructureFlagSyntax(t *testing.T) {
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	Flags(f, true)
	name, usage := flag.UnquoteUsage(f.Lookup("structure"))
	if name != "" || !strings.Contains(usage, "--structure=auto|on|off") {
		t.Fatalf("usage -structure %s: %s", name, usage)
	}

}
