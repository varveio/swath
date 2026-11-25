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
// with start-after L. New keys after L are a race: the page is not final,
// and the chain goes on after L through the caller's own page path. A witness gone is a race: the page stands. A witness that stays
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
// whose cursor before it was c: more is true when keys follow the page's
// last key l after all (written meanwhile, or a service whose listing
// lags). The caller then takes the page as not final: it emits its rows and
// goes on with a chain that starts after l, through its own page path, so
// every page is emitted, fenced and checked as any other, one at a time.
// err is the omission, when the service keeps omitting the keys.
func (p *Engine[P]) checkEnd(ctx context.Context, in *Query, out *Page[P], c string) (more bool, l string, err error) {
	l = c
	if n := len(out.Rows); n > 0 {
		l = out.Rows[n-1].Key
	}
	p.c.Inc(cEndClaims)
	w, folder := p.past(l)
	if w == "" {
		if w, err = p.confirm(ctx, l); err != nil || w == "" {
			return false, l, err
		}
	}
	p.c.Inc(cEndContradicted)
	more, err = p.askAgain(ctx, in, l, w, folder)
	return more, l, err
}

// askAgain settles a page that ended the listing after l while w, a key or
// folder past l, was seen: true when keys follow l now, false when w is
// gone, or the omission.
func (p *Engine[P]) askAgain(ctx context.Context, in *Query, l, w string, folder bool) (bool, error) {
	wait := requeueBase
	for try := 0; ; try++ {
		if try > 0 {
			if err := RequeueBackoff(ctx, &wait); err != nil {
				return false, err
			}
		}
		there, err := p.exists(ctx, w)
		if err != nil {
			return false, err
		}
		if !there {
			p.c.Inc(cEndRacedGone)
			return false, nil
		}
		if try == endTries {
			p.c.Inc(cEndOmitted)
			kind := "key"
			if folder {
				kind = "folder"
			}
			return false, source.Omitted(fmt.Errorf("list %s: the service ended the listing after %q (prefix %q, start-after %q, max-keys %d), "+
				"but the %s %q after it exists, and %d requests asked again ended the listing before it too: the service omitted keys "+
				"(a known S3-compatible-store fault; swath diag list --sequential lists with the service's own continuation tokens)",
				p.d.Target, l, in.Prefix, in.After, in.Max, kind, w, endTries))
		}
		p.c.Inc(cEndAskedAgain)
		more, err := p.follows(ctx, in, l)
		if err != nil {
			return false, err
		}
		if more {
			p.c.Inc(cEndRacedNew)
			return true, nil
		}
	}
}

// stalled is the omission of a service that keeps answering a chain that
// starts after l with an empty final page while asking again lists keys
// after l.
func (p *Engine[P]) stalled(in *Query, l string) error {
	p.c.Inc(cEndOmitted)
	return source.Omitted(fmt.Errorf("list %s: the service ended the listing after %q (prefix %q, max-keys %d) %d times while asking again "+
		"listed keys after it: the service omitted keys (a known S3-compatible-store fault; swath diag list --sequential lists with "+
		"the service's own continuation tokens)", p.d.Target, l, in.Prefix, in.Max, endTries+1))
}

// follows asks the claim again: whether the chain after l, with the
// claim's page size, lists a key after l (listAfter).
func (p *Engine[P]) follows(ctx context.Context, in *Query, l string) (bool, error) {
	out, err := p.listAfter(ctx, in, l)
	return err == nil && len(out.Rows) > 0, err
}

// listAfter lists the first page of the chain after l in in's scope, with
// in's page size: every row on it is above l, and its token continues the
// chain. It is the only way the end check starts a chain after a key the
// service listed. When the provider refuses l as a start bound (S3 refuses
// control characters) the chain starts after l's longest accepted prefix
// and the keys up to l are dropped, as a resume drops them; those pages
// must ascend and their tokens make progress (a token cycle, or a key at
// or below the last, is a broken listing), within EmptyChainLive pages. On
// the chain that starts after l itself, a key at or below l is a broken
// listing too: a service that kept answering with l would otherwise be
// asked forever.
func (p *Engine[P]) listAfter(ctx context.Context, in *Query, l string) (*Page[P], error) {
	q := &Query{Prefix: in.Prefix, After: p.safeStart(l), Max: in.Max}
	prev := q.After
	var tokens *tokenWindow
	for pages := 1; ; pages++ {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		out, err := p.list(ctx, q)
		if err != nil {
			return nil, err
		}
		n := 0
		for ; n < len(out.Rows) && out.Rows[n].Key <= l; n++ {
			k := out.Rows[n].Key
			if q.After == l || k <= prev {
				return nil, fmt.Errorf("%w: a page asked after %q listed %q after %q", p.d.invalid(), l, k, prev)
			}
			prev = k
		}
		if n > 0 {
			p.c.Add(cOverfetchKeys, int64(n))
		}
		if n < len(out.Rows) || !out.Truncated {
			out.Rows = out.Rows[n:]
			return out, nil
		}
		if out.Token == "" {
			return nil, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		if tokens == nil {
			tokens = newTokenWindow(q.Token)
		}
		if !tokens.add(out.Token) || pages == EmptyChainLive {
			return nil, fmt.Errorf("%w: the pages up to %q after %q make no progress", p.d.invalid(), l, q.After)
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
