// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"fmt"

	"github.com/spf13/pflag"
)

// A FlagGroup is a flag group several providers share (the store's, the
// range engine's: cli.StoreGroup, cli.EngineGroup): the command registers
// it once and passes its value to every provider that names it
// (Factory.Shared), so the flags are spelled the same for every provider
// and owned by each of them. A group is identified by its address:
// providers name the one declared value, so two groups never alias by
// name.
type FlagGroup struct {
	// Name names the group in messages.
	Name string
	// Flags registers the group's flags on fs and returns the value they
	// fill when fs is parsed; diag adds its diagnostic flags (swath diag
	// list only).
	Flags func(fs *pflag.FlagSet, diag bool) any
	// Operational are the group's flags a resume may change: how requests
	// are authenticated, timed and admitted, which no identity holds.
	Operational []string
	// URLs are the group's flags whose value is a URL: a recorded command
	// line redacts one that carries userinfo, or does not parse
	// (RedactArgv).
	URLs []string
}

// SharedValues are the values of the shared groups a provider names, by
// group, as its flags receive them (Factory.Flags).
type SharedValues map[*FlagGroup]any

// Shared is group g's value in values, the *T its Flags returned. A
// provider asking for a group it does not name, or for another type than
// the group's, is a programming error, so Shared panics rather than
// dropping the group's flags silently.
func Shared[T any](values SharedValues, g *FlagGroup) *T {
	v, ok := values[g]
	if !ok {
		panic(fmt.Sprintf("registry: flag group %q is not among the groups the factory names (Factory.Shared)", g.Name))
	}
	t, ok := v.(*T)
	if !ok {
		panic(fmt.Sprintf("registry: flag group %q holds %T, not %T", g.Name, v, t))
	}
	return t
}
