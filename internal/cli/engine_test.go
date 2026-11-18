// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"io"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
)

// TestEngineFlags: the engine's flag group (registered once by the command) has
// the command's defaults and fills the policy and the admission apart.
func TestEngineFlags(t *testing.T) {
	for _, diag := range []bool{false, true} {
		f := pflag.NewFlagSet("t", pflag.ContinueOnError)
		c := engineFlags(f, diag)
		if !c.Admission.Limiter || !c.Policy.Seed || c.Policy.Structure != keyrange.StructureAuto {
			t.Fatalf("defaults: %+v", *c)
		}
		args := []string{"--no-adaptive-limit", "--request-rate", "5"}
		if diag {
			args = append(args, "--no-seed", "--adaptive-limit-slow-start", "--sequential", "--max-keys", "9", "--structure=off")
		}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		want, adm := keyrange.Policy{Seed: true, Structure: keyrange.StructureAuto}, objhttp.Admission{RequestRate: 5}
		if diag {
			want, adm = keyrange.Policy{Sequential: true, MaxKeys: 9}, objhttp.Admission{SlowStart: true, RequestRate: 5}
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
		want keyrange.Structure
	}{{nil, keyrange.StructureAuto}, {[]string{"--structure"}, keyrange.StructureOn}, {[]string{"--structure=on"}, keyrange.StructureOn},
		{[]string{"--structure=false"}, keyrange.StructureOff}, {[]string{"--structure=off"}, keyrange.StructureOff}, {[]string{"--structure=auto"}, keyrange.StructureAuto}} {
		f := pflag.NewFlagSet("t", pflag.ContinueOnError)
		p := engineFlags(f, true)
		if err := f.Parse(c.args); err != nil || p.Policy.Structure != c.want {
			t.Fatalf("%q: %v %v", c.args, p.Policy.Structure, err)
		}
	}
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	f.SetOutput(io.Discard)
	engineFlags(f, true)
	if err := f.Parse([]string{"--structure=sometimes"}); err == nil {
		t.Fatal("--structure=sometimes accepted")
	}
}

// TestStructureFlagSyntax: the bare flag means on, so a mode needs '=':
// the help shows --structure[=auto|on|off] (an optional value), and
// "--structure off" leaves off as the next argument (the command's SOURCE).
func TestStructureFlagSyntax(t *testing.T) {
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	p := engineFlags(f, true)
	fl := f.Lookup("structure")
	if d := Described(fl); fl.NoOptDefVal != "on" || d.Metavar != "auto|on|off" || !strings.Contains(d.Long, "needs '='") {
		t.Fatalf("--structure: optional value %q, placeholder %q, %q", fl.NoOptDefVal, d.Metavar, d.Long)
	}
	if err := f.Parse([]string{"--structure", "off", "s3://bkt"}); err != nil || p.Policy.Structure != keyrange.StructureOn || f.Arg(0) != "off" {
		t.Fatalf("%v %v %q", err, p.Policy.Structure, f.Args())
	}
}
