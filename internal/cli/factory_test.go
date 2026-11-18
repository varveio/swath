// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
)

// factory is the factory registered for root's scheme.
func factory(t *testing.T, root string) registry.Factory {
	t.Helper()
	f, err := registry.Lookup(root)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// openArgs opens root with the command-line flags args as the command
// does: the factory's flags and its shared groups' on a flag set of their
// own (diag adds the diagnostic ones), one worker, the warnings told to
// warn (nil: dropped).
func openArgs(t *testing.T, root string, diag bool, warn func(code, msg string), args ...string) (registry.Source, error) {
	t.Helper()
	f := factory(t, root)
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o := f.RegisterFlags(fs, diag)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if warn == nil {
		warn = func(string, string) {}
	}
	return f.Open(context.Background(), registry.Request{Root: root, Options: o, Workers: 1, Warn: warn})
}

// identityField is the value of id's field name, "" without one.
func identityField(id source.Identity, name string) string {
	for _, f := range id.Fields {
		if f.Name == name {
			return f.Value
		}
	}
	return ""
}
