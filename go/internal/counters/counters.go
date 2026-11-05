// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package counters is the run's tally: a table of counter definitions
// (summary key, kind, unit, meaning) backed by one atomic per entry. The
// run summary is emitted by iterating the table, and the documentation
// table is generated from it, so a new engagement counter is one table row
// plus its increment.
package counters

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Kind says how a counter is updated and read.
type Kind uint8

const (
	Count Kind = iota // monotonic sum
	Max               // high-water mark
	Flag              // 0 or 1, reported as a boolean
)

func (k Kind) String() string { return [...]string{"count", "max", "flag"}[k] }

// Def defines one counter. Key is its dotted path in the summary JSON.
type Def struct {
	Key  string
	Kind Kind
	Unit string
	Help string
}

// Set holds the values of one table.
type Set struct {
	defs []Def
	v    []atomic.Int64
}

// New returns a zeroed set for defs; the index of a Def is its ID.
func New(defs []Def) *Set { return &Set{defs: defs, v: make([]atomic.Int64, len(defs))} }

// Add adds n to counter id.
func (s *Set) Add(id int, n int64) { s.v[id].Add(n) }

// Inc adds one to counter id.
func (s *Set) Inc(id int) { s.v[id].Add(1) }

// Set stores n (flags, or values copied from elsewhere).
func (s *Set) Set(id int, n int64) { s.v[id].Store(n) }

// Max raises counter id to n.
func (s *Set) Max(id int, n int64) {
	for {
		cur := s.v[id].Load()
		if n <= cur || s.v[id].CompareAndSwap(cur, n) {
			return
		}
	}
}

// Load reads counter id.
func (s *Set) Load(id int) int64 { return s.v[id].Load() }

// Map renders the set as nested maps by the dotted keys, flags as booleans.
func (s *Set) Map() map[string]any {
	root := map[string]any{}
	for i, d := range s.defs {
		parts := strings.Split(d.Key, ".")
		m := root
		for _, p := range parts[:len(parts)-1] {
			next, ok := m[p].(map[string]any)
			if !ok {
				next = map[string]any{}
				m[p] = next
			}
			m = next
		}
		var v any = s.v[i].Load()
		if d.Kind == Flag {
			v = s.v[i].Load() != 0
		}
		m[parts[len(parts)-1]] = v
	}
	return root
}

// Markdown renders defs as a documentation table.
func Markdown(defs []Def) string {
	var b strings.Builder
	b.WriteString("| Key | Kind | Unit | Meaning |\n|---|---|---|---|\n")
	for _, d := range defs {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", d.Key, d.Kind, d.Unit, d.Help)
	}
	return b.String()
}
