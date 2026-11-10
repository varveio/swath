// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/source"
)

// panicOf runs f and returns what it panicked with, as text ("" if it did
// not panic).
func panicOf(f func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	f()
	return ""
}

// group is a shared flag group registering one string flag, name.
func group(title, name string) *source.FlagGroup {
	return &source.FlagGroup{Name: title, Flags: func(fs *pflag.FlagSet, _ bool) any {
		return fs.String(name, "", "")
	}}
}

// TestGroupIdentity: groups are told apart by identity, never by name, so
// two groups of one name are two groups; registering both on one flag set
// with the same flag fails loudly, naming the flag, rather than one group
// shadowing the other.
func TestGroupIdentity(t *testing.T) {
	a, b := group("same", "dup"), group("same", "dup")
	f := Factory{Shared: []*source.FlagGroup{a, b}, Flags: func(*pflag.FlagSet, bool, source.SharedValues) any { return nil }}
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if msg := panicOf(func() { f.RegisterFlags(fs, false) }); !strings.Contains(msg, "dup") {
		t.Fatalf("two groups registering --dup: panic %q, want one naming the flag", msg)
	}
}

// TestSharedAccessorRefuses: a provider reading a group it does not name,
// or reading it as another type, panics naming the group, rather than
// dropping the group's flags.
func TestSharedAccessorRefuses(t *testing.T) {
	named, other := group("named", "x"), group("other", "y")
	for _, c := range []struct {
		why   string
		flags func(source.SharedValues)
		want  string
	}{
		{"unnamed group", func(v source.SharedValues) { source.Shared[string](v, other) }, `"other" is not among`},
		{"wrong type", func(v source.SharedValues) { source.Shared[int](v, named) }, `"named" holds *string, not *int`},
	} {
		f := Factory{Shared: []*source.FlagGroup{named}, Flags: func(_ *pflag.FlagSet, _ bool, v source.SharedValues) any {
			c.flags(v)
			return nil
		}}
		if msg := panicOf(func() { f.RegisterFlags(pflag.NewFlagSet("", pflag.ContinueOnError), false) }); !strings.Contains(msg, c.want) {
			t.Errorf("%s: panic %q, want %q", c.why, msg, c.want)
		}
	}
	// The value of a named group of the right type is the group's own.
	f := Factory{Shared: []*source.FlagGroup{named}, Flags: func(fs *pflag.FlagSet, _ bool, v source.SharedValues) any {
		return source.Shared[string](v, named)
	}}
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	got := f.RegisterFlags(fs, false).(*string)
	if err := fs.Parse([]string{"--x", "v"}); err != nil || *got != "v" {
		t.Fatalf("named group: %q, %v", *got, err)
	}
}
