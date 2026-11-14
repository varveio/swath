// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// A prior is a key distribution a range-engine source starts from instead
// of its delimiter seed (--seed-from; docs/internals/range-engine.md, "Warm start"):
// ordered, byte-exact cut keys, each with the keys expected in the interval
// it closes. It sets the initial tiles and their first estimates only
// (keyrange's SeedFrom). It is source-neutral: a previous dataset, a cut
// file or any other listing can fill it.

// Prior is a set of cut keys taken under one scope.
type Prior struct {
	// Source names where the cuts came from, for the run report: "dataset"
	// (a committed dataset's parts and key page index) or "cuts"
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
	// Stream, if set, is a counted prior read lazily in key order instead
	// of Cuts (a dataset's key page index, part by part): the engine plans
	// its tiles from it as the order stage admits them (Planner).
	Stream CutStream
	// Rows and Bytes are a dataset's rows and part bytes (0: unknown): its
	// stored bytes per row, a first estimate of a spilled row's.
	Rows, Bytes int64
	// Parts, IndexedParts, IndexFailed: a dataset's parts, those whose key
	// page index was read for finer cuts, and those whose index could not
	// be read (their part bounds were used instead).
	Parts, IndexedParts, IndexFailed int
}

// CutStream is a counted prior's cuts, read lazily in key order
// (Prior.Stream).
type CutStream interface {
	// Start reads ahead, from the first cut above after ("" none), until
	// ctx ends, calling wake (without its caller's locks) whenever Next
	// may answer anew; stop ends the read-ahead and waits for it.
	Start(ctx context.Context, after string, wake func()) (stop func())
	// Next returns the next batch without blocking: nil when none is ready
	// yet, io.EOF after the last, or the failure that ended the stream
	// (the context's cause once stopped). Cuts ascend across batches; a
	// key repeated from the previous cut continues it (a version group
	// across key pages).
	Next() (*CutBatch, error)
}

// CutBatch is a run of a CutStream's cuts: one dataset part's (Part), from
// its key page index (Indexed) or, where that could not be read, its own
// last key.
type CutBatch struct {
	Cuts          []Cut
	Part, Indexed bool
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
		if err := CheckCut(c, i+1, scope, maxKey); err != nil {
			return err
		}
		if i > 0 && c.Key <= pr.Cuts[i-1].Key {
			return bad("cut %d %q does not sort after cut %d %q (cuts ascend bytewise, without repeats)", i+1, c.Key, i, pr.Cuts[i-1].Key)
		}
	}
	return nil
}

// CheckCut validates one cut, the n-th (from 1), by itself: valid UTF-8,
// no longer than maxKey bytes, inside the scope, a count that is not
// negative. Check applies it to every cut, a plan to every cut it reads.
func CheckCut(c Cut, n int, scope string, maxKey int) error {
	bad := func(f string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrPrior, fmt.Sprintf(f, a...))
	}
	switch {
	case !utf8.ValidString(c.Key):
		return bad("cut %d is not valid UTF-8: %q", n, c.Key)
	case len(c.Key) > maxKey:
		return bad("cut %d is longer than %d bytes", n, maxKey)
	case !strings.HasPrefix(c.Key, scope):
		return bad("cut %d %q is outside the scope %q", n, c.Key, scope)
	case c.Rows < 0:
		return bad("cut %d has a negative count", n)
	}
	return nil
}

// Seeded is a source that can start from a prior (Resumable's sibling):
// SeedFrom is called before Root.
type Seeded interface {
	SeedFrom(pr *Prior) error
}
