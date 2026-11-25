// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"strings"
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
// Not covered here: a claim with no witness past it (the scope's last
// range; a range whose neighbours lie as well with no folder between, as
// --no-seed or one worker can leave them), the seed's single first page,
// the sequential chain, and versions listings. end_check.claims less
// end_check.contradicted counts the claims left unverified.

// endTries is how many times checkEnd asks again before it calls a
// contradicted page an omission.
const endTries = 3

// witnesses are the greatest key and common prefix the run has listed.
type witnesses struct {
	key, prefix atomic.Pointer[string]
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

// endChecks: this engine checks range ends (Description.Unbounded, not a
// versions listing, not the sequential chain).
func (p *Engine[P]) endChecks() bool {
	return p.d.Unbounded && !p.d.Groups && !p.opt.Sequential
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
		return false, l, nil
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

// follows asks the claim again: whether a chain that starts after l in the
// scope, with the claim's page size, lists a key after l. When the provider
// refuses l as a start bound (S3 refuses control characters) the chain
// starts after l's longest accepted prefix and the keys up to l are
// dropped, as a resume drops them; those pages must ascend and their tokens
// make progress (a token cycle, or a page at or below the last, is a
// broken listing), within EmptyChainLive pages. On the chain that starts
// after l itself, a key at or below l is a broken listing too: a service
// that kept answering with l would otherwise be asked forever.
func (p *Engine[P]) follows(ctx context.Context, in *Query, l string) (bool, error) {
	q := &Query{Prefix: in.Prefix, After: p.safeStart(l), Max: in.Max}
	prev := q.After
	var tokens *tokenWindow
	for pages := 1; ; pages++ {
		if ctx.Err() != nil {
			return false, context.Cause(ctx)
		}
		out, err := p.list(ctx, q)
		if err != nil {
			return false, err
		}
		for i := range out.Rows {
			k := out.Rows[i].Key
			if k > l {
				return true, nil
			}
			if q.After == l || k <= prev {
				return false, fmt.Errorf("%w: a page asked after %q listed %q after %q", p.d.invalid(), l, k, prev)
			}
			prev = k
		}
		if !out.Truncated {
			return false, nil
		}
		if out.Token == "" {
			return false, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		if tokens == nil {
			tokens = newTokenWindow(q.Token)
		}
		if !tokens.add(out.Token) || pages == EmptyChainLive {
			return false, fmt.Errorf("%w: the pages up to %q after %q make no progress", p.d.invalid(), l, q.After)
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
