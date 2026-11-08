// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// A prior is a key distribution a range-engine source starts from instead
// of its delimiter seed (--seed-from; docs/internals/providers.md, "Warm start"):
// ordered, byte-exact cut keys, each with the keys expected in the interval
// it closes. It sets the initial tiles and their first estimates only
// (keyrange's SeedFrom). It is source-neutral: a previous dataset, a cut
// file or any other listing can fill it.

// Prior is a set of cut keys taken under one scope.
type Prior struct {
	// Source names where the cuts came from, for the run report: "dataset"
	// (a committed format-2 dataset's parts and key page index) or "cuts"
	// (a cut file).
	Source string
	// Scope is the prefix the cuts were taken under; it must be the run's.
	Scope string
	// Unscoped: the cuts record no scope (a cut file); the run's is
	// assumed, and every cut must still lie inside it.
	Unscoped bool
	// Cuts ascend in unsigned byte order without repeats, each inside the
	// scope. Cut i closes the interval (cut i-1, cut i], the first from
	// the scope's start; keys after the last are expected in the open
	// frontier, none by the prior.
	Cuts []Cut
	// Counted: every cut carries the expected keys of its interval (Rows).
	// Without counts the cuts weigh the same.
	Counted bool
	// Parts, IndexedParts, IndexFailed: a dataset's parts, those whose key
	// page index was read for finer cuts, and those whose index could not
	// be read (their part bounds were used instead).
	Parts, IndexedParts, IndexFailed int
}

// Cut is one cut key and the keys expected in the interval it closes.
type Cut struct {
	Key  string
	Rows int64
}

// ErrPrior marks a prior that cannot seed the run (out of order, outside
// the scope, another scope): the caller's to fix, nothing is listed.
var ErrPrior = errors.New("unusable prior")

// Check validates the prior against the run's scope: the same scope, cuts
// strictly ascending bytewise, valid UTF-8, each carrying the scope and no
// longer than a key may be (maxKey bytes).
func (pr *Prior) Check(scope string, maxKey int) error {
	bad := func(f string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrPrior, fmt.Sprintf(f, a...))
	}
	if !pr.Unscoped && pr.Scope != scope {
		return bad("the prior was taken under the prefix %q, the run lists %q", pr.Scope, scope)
	}
	for i, c := range pr.Cuts {
		switch {
		case !utf8.ValidString(c.Key):
			return bad("cut %d is not valid UTF-8: %q", i+1, c.Key)
		case len(c.Key) > maxKey:
			return bad("cut %d is longer than %d bytes", i+1, maxKey)
		case !strings.HasPrefix(c.Key, scope):
			return bad("cut %d %q is outside the scope %q", i+1, c.Key, scope)
		case i > 0 && c.Key <= pr.Cuts[i-1].Key:
			return bad("cut %d %q does not sort after cut %d %q (cuts ascend bytewise, without repeats)", i+1, c.Key, i, pr.Cuts[i-1].Key)
		case c.Rows < 0:
			return bad("cut %d has a negative count", i+1)
		}
	}
	return nil
}

// Seeded is a source that can start from a prior (Resumable's sibling):
// SeedFrom is called before Root.
type Seeded interface {
	SeedFrom(pr *Prior) error
}
