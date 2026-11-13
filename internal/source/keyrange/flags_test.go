// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"io"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

// TestFlags: the engine's flag group (registered once by the command) has
// the command's defaults and fills the policy and the admission apart.
func TestFlags(t *testing.T) {
	for _, diag := range []bool{false, true} {
		f := pflag.NewFlagSet("t", pflag.ContinueOnError)
		c := Flags(f, diag)
		if !c.Admission.Limiter || !c.Policy.Seed || c.Policy.Structure != StructureAuto {
			t.Fatalf("defaults: %+v", *c)
		}
		args := []string{"--no-adaptive-limit", "--request-rate", "5"}
		if diag {
			args = append(args, "--no-seed", "--adaptive-limit-slow-start", "--sequential", "--max-keys", "9", "--structure=off")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want, adm := Policy{Seed: true, Structure: StructureAuto}, objhttp.Admission{RequestRate: 5}
		if diag {
			want, adm = Policy{Sequential: true, MaxKeys: 9}, objhttp.Admission{SlowStart: true, RequestRate: 5}
		}
		if c.Policy != want || c.Admission != adm {
			t.Fatalf("diag=%v: %+v, want %+v %+v", diag, *c, want, adm)
		}
		if o := c.Policy.Options(); o.Seed != want.Seed || o.Sequential != want.Sequential || o.MaxKeys != want.MaxKeys {
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
	}{{nil, StructureAuto}, {[]string{"--structure"}, StructureOn}, {[]string{"--structure=on"}, StructureOn},
		{[]string{"--structure=false"}, StructureOff}, {[]string{"--structure=off"}, StructureOff}, {[]string{"--structure=auto"}, StructureAuto}} {
		f := pflag.NewFlagSet("t", pflag.ContinueOnError)
		p := Flags(f, true)
		if err := f.Parse(c.args); err != nil || p.Policy.Structure != c.want {
			t.Fatalf("%q: %v %v", c.args, p.Policy.Structure, err)
		}
	}
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	f.SetOutput(io.Discard)
	Flags(f, true)
	if err := f.Parse([]string{"--structure=sometimes"}); err == nil {
		t.Fatal("--structure=sometimes accepted")
	}
}

// TestStructureFlagSyntax: the bare flag means on, so a mode needs '=':
// the help shows --structure[=auto|on|off] (an optional value), and
// "--structure off" leaves off as the next argument (the command's SOURCE).
func TestStructureFlagSyntax(t *testing.T) {
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	p := Flags(f, true)
	fl := f.Lookup("structure")
	if d := source.Described(fl); fl.NoOptDefVal != "on" || d.Metavar != "auto|on|off" || !strings.Contains(d.Long, "needs '='") {
		t.Fatalf("--structure: optional value %q, placeholder %q, %q", fl.NoOptDefVal, d.Metavar, d.Long)
	}
	if err := f.Parse([]string{"--structure", "off", "s3://bkt"}); err != nil || p.Policy.Structure != StructureOn || f.Arg(0) != "off" {
		t.Fatalf("%v %v %q", err, p.Policy.Structure, f.Args())
	}
}
