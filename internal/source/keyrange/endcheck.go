// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/internal/source"
)

// The end check. A provider whose requests carry no end bound
// (Description.Unbounded, S3) says with a page that is not truncated that
// the scope holds no key after the page's last key L (or after the
// request's start, for an empty page): not just that the range is done.
// In a healthy run only ranges above the scope's last key end that way, so
// a key or a folder seen anywhere in the run past L proves the page wrong.
// Some services answer a start-after inside a folder with such a page while
// keys follow (MinIO, docs/sources/s3.md).
//
// The check runs on a range's final page, before the range declares its
// end (the fence that a resume trusts), and only for a chain that started
// after a key: a chain from the scope's start follows the service's own
// tokens. It costs no request unless it finds a witness past L. A witness
// is a listed key (any page: ranges, overfetch, the seed, probes) or a
// common prefix (a folder: some key at or after it exists). Synthesized
// pivots, prior cuts and resume keys are not witnesses.
//
// A witness past L is not yet a lie: keys may be written or deleted while
// the run lists. checkEnd asks again (endTries times, with the requeue
// backoff, for a service whose listing lags its writes): the witness, with
// a listing of its own prefix that sends no start-after, and the claim,
// with start-after L. New keys after L are a race: the range goes on from
// them. A witness gone is a race: the page stands. A witness that stays
// while start-after L still ends the listing below it is an omission: the
// run fails, resumable (source.ErrOmitted), with nothing past the range's
// last fence declared.
//
// A claim with no witness past it (the scope's last range; neighbours that
// all lie with no folder seen between them, as --no-seed or one worker can
// leave them) is confirmed by the ladder (confirm): requests, one round
// trip, that ask whether any key follows L in shapes those services answer
// whole, measured on MinIO (2026-11-25: 30,393 claims under four scopes, no
// wrong answer). A confirmed claim is a floor: no later claim at or above
// it needs another ladder. A key the ladder finds is a witness as above.
//
// The check covers range ends, the sequential chain's resume and the
// seed's single first page from a resume key; a chain from the scope's
// start is the service's own. Not covered: versions listings, and a claim
// whose last key the provider refuses as a start bound
// (end_check.unverified).

// endTries is how many times checkEnd asks again before it calls a
// contradicted page an omission.
const endTries = 3

// witnesses are the greatest key and common prefix the run has listed,
// and the least claim a ladder confirmed (the floor).
type witnesses struct {
	key, prefix, floor atomic.Pointer[string]
	mu                 sync.Mutex
	flights            []*flight // ladders in flight (confirm)
}

// raise sets *w to s when s is greater (a copy: page strings are views of
// the page's parser arena).
func raise(w *atomic.Pointer[string], s string) {
	for {
		cur := w.Load()
		if cur != nil && s <= *cur {
			return
		}
		v := strings.Clone(s)
		if w.CompareAndSwap(cur, &v) {
			return
		}
	}
}

// endChecks: this engine checks the ends of chains that start after a key
// (Description.Unbounded, not a versions listing).
func (p *Engine[P]) endChecks() bool {
	return p.d.Unbounded && !p.d.Groups
}

// witness records a page's greatest key and common prefix.
func (p *Engine[P]) witness(out *Page[P]) {
	if n := len(out.Rows); n > 0 {
		raise(&p.seen.key, out.Rows[n-1].Key)
	}
	if n := len(out.Prefixes); n > 0 {
		raise(&p.seen.prefix, out.Prefixes[n-1]) // sorted by list
	}
}

// past is the greatest witness above l and whether it is a folder; "" when
// none is.
func (p *Engine[P]) past(l string) (w string, folder bool) {
	if k := p.seen.key.Load(); k != nil && *k > l {
		w = *k
	}
	if f := p.seen.prefix.Load(); f != nil && *f > l && *f > w {
		w, folder = *f, true
	}
	return w, folder
}

// checkEnd checks out, the final page of the chain in (not truncated),
// whose cursor before it was c. It returns the page the range goes on
// with: out itself, or out followed by keys a request asked again found
// after it (a race), or an error when the service omitted keys.
func (p *Engine[P]) checkEnd(ctx context.Context, in *Query, out *Page[P], c string) (*Page[P], error) {
	for !out.Truncated {
		l := c
		if n := len(out.Rows); n > 0 {
			l = out.Rows[n-1].Key
		}
		p.c.Inc(cEndClaims)
		w, folder := p.past(l)
		if w == "" {
			var err error
			if w, err = p.confirm(ctx, l); err != nil || w == "" {
				return out, err
			}
		}
		p.c.Inc(cEndContradicted)
		more, err := p.askAgain(ctx, in, l, w, folder)
		if err != nil || more == nil {
			return out, err
		}
		// Keys after l, listed by a chain that started after l: the range
		// goes on from them (their order and bound are checked as any
		// page's), and their page's end is checked in turn.
		out = &Page[P]{Rows: slices.Concat(out.Rows, more.Rows), Truncated: more.Truncated, Token: more.Token}
		c = l
	}
	return out, nil
}

// askAgain settles a page that ended the listing after l while w, a key or
// folder past l, was seen: the page of keys after l when there now are
// some, nil when w is gone, or the omission.
func (p *Engine[P]) askAgain(ctx context.Context, in *Query, l, w string, folder bool) (*Page[P], error) {
	wait := requeueBase
	for try := 0; ; try++ {
		if try > 0 {
			if err := RequeueBackoff(ctx, &wait); err != nil {
				return nil, err
			}
		}
		there, err := p.exists(ctx, w)
		if err != nil {
			return nil, err
		}
		if !there {
			p.c.Inc(cEndRacedGone)
			return nil, nil
		}
		if try == endTries {
			p.c.Inc(cEndOmitted)
			kind := "key"
			if folder {
				kind = "folder"
			}
			return nil, source.Omitted(fmt.Errorf("list %s: the service ended the listing after %q (prefix %q, start-after %q, max-keys %d), "+
				"but the %s %q after it exists, and %d requests asked again ended the listing before it too: the service omitted keys "+
				"(a known S3-compatible-store fault; swath diag list --sequential lists with the service's own continuation tokens)",
				p.d.Target, l, in.Prefix, in.After, in.Max, kind, w, endTries))
		}
		p.c.Inc(cEndAskedAgain)
		again, err := p.after(ctx, in, l)
		if err != nil {
			return nil, err
		}
		if len(again.Rows) > 0 {
			p.c.Inc(cEndRacedNew)
			return again, nil
		}
	}
}

// after lists the first page of keys after l in the scope: a chain that
// starts after l, or, when the provider refuses l as a start bound (S3
// refuses control characters), after its longest accepted prefix, with the
// keys up to l dropped as a resume drops them. A key at or below l on the
// chain that starts after l itself is a broken listing, never progress: a
// service that kept answering with l would otherwise be asked forever.
func (p *Engine[P]) after(ctx context.Context, in *Query, l string) (*Page[P], error) {
	q := &Query{Prefix: in.Prefix, After: p.safeStart(l), Max: in.Max}
	for {
		out, err := p.list(ctx, q)
		if err != nil {
			return nil, err
		}
		n := 0
		for n < len(out.Rows) && out.Rows[n].Key <= l {
			if q.After == l {
				return nil, fmt.Errorf("%w: a page asked after %q listed %q", p.d.invalid(), l, out.Rows[n].Key)
			}
			n++
		}
		if out.Rows = out.Rows[n:]; len(out.Rows) > 0 || !out.Truncated {
			return out, nil
		}
		if out.Token == "" {
			return nil, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		q.Token = out.Token
	}
}

// exists: a key at or under w is listed by a request with prefix w that
// sends no start-after (the shape the services that omit keys after a
// start-after answer whole). w lies in the scope: every witness was listed
// under it.
func (p *Engine[P]) exists(ctx context.Context, w string) (bool, error) {
	p.c.Inc(cEndWitnessChecks)
	out, err := p.list(ctx, &Query{Prefix: w, Max: 1})
	if err != nil {
		return false, err
	}
	return len(out.Rows) > 0, nil
}

// confirm asks whether any key follows l in the scope, for a claim no
// witness contradicts: the key found, or "" when none follows. The ladder
// asks, with max-keys 1 and in one round trip, the folder of l after l,
// then each folder above it up to the scope after its child folder (the
// child's name and '0', the byte after '/'), and the keys under that name
// and '0' itself. No request starts after a key below a folder of its
// prefix, the shape that omits keys.
func (p *Engine[P]) confirm(ctx context.Context, l string) (string, error) {
	for {
		if f := p.seen.floor.Load(); f != nil && l >= *f {
			p.c.Inc(cEndFloor)
			return "", nil
		}
		// A ladder in flight from at or below l answers for l too: the
		// ranges above the scope's last key end together, and one ladder
		// covers them all. Its key found past l is l's witness; past it
		// only, or its failure, leaves l its own ladder.
		p.seen.mu.Lock()
		var wait *flight
		for _, f := range p.seen.flights {
			if f.l <= l && (wait == nil || f.l > wait.l) {
				wait = f
			}
		}
		if wait == nil {
			break
		}
		p.seen.mu.Unlock()
		p.c.Inc(cEndLadderWaits)
		select {
		case <-wait.done:
		case <-ctx.Done():
			return "", context.Cause(ctx)
		}
		if wait.err == nil && wait.found > l {
			return wait.found, nil
		}
		if wait.err != nil || wait.found != "" {
			p.seen.mu.Lock()
			break
		}
	}
	f := &flight{l: l, done: make(chan struct{})}
	p.seen.flights = append(p.seen.flights, f)
	p.seen.mu.Unlock()
	defer func() {
		p.seen.mu.Lock()
		p.seen.flights = slices.DeleteFunc(p.seen.flights, func(x *flight) bool { return x == f })
		p.seen.mu.Unlock()
		close(f.done)
	}()
	f.found, f.err = p.climb(ctx, l)
	return f.found, f.err
}

// flight is a ladder in flight: its claim, and when done the key it found
// past it ("" for none) or its error.
type flight struct {
	l, found string
	err      error
	done     chan struct{}
}

// climb runs the ladder for a claim after l (confirm).
func (p *Engine[P]) climb(ctx context.Context, l string) (string, error) {
	steps := ladder(l, p.opt.Prefix)
	for _, q := range steps {
		if q.After != "" && !p.safe(q.After) || !p.safe(q.Prefix) {
			p.c.Inc(cEndUnverified)
			return "", nil
		}
	}
	p.c.Inc(cEndLadders)
	p.c.Add(cEndLadderRequests, int64(len(steps)))
	found := make([]string, len(steps))
	errs := make([]error, len(steps))
	var wg sync.WaitGroup
	for i := range steps {
		wg.Go(func() {
			out, err := p.list(ctx, &steps[i])
			if err == nil && len(out.Rows) > 0 {
				found[i] = strings.Clone(out.Rows[0].Key)
			}
			errs[i] = err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return "", err
	}
	for _, k := range found {
		if k > l {
			p.c.Inc(cEndLadderFound)
			return k, nil
		}
	}
	lower(&p.seen.floor, l)
	return "", nil
}

// ladder is confirm's requests for a claim after l in the scope prefix.
func ladder(l, prefix string) []Query {
	j := strings.LastIndexByte(l, '/')
	if j < len(prefix) {
		return []Query{{Prefix: prefix, After: l, Max: 1}}
	}
	folder := l[:j+1]
	steps := []Query{{Prefix: folder, After: l, Max: 1}}
	for len(folder) > len(prefix) {
		name := folder[:len(folder)-1]
		parent := prefix
		if k := strings.LastIndexByte(name, '/'); k >= len(prefix) {
			parent = name[:k+1]
		}
		steps = append(steps, Query{Prefix: parent, After: name + "0", Max: 1}, Query{Prefix: name + "0", Max: 1})
		folder = parent
	}
	return steps
}

// lower sets *w to s when s is less (or *w is unset).
func lower(w *atomic.Pointer[string], s string) {
	for {
		cur := w.Load()
		if cur != nil && s >= *cur {
			return
		}
		v := strings.Clone(s)
		if w.CompareAndSwap(cur, &v) {
			return
		}
	}
}
