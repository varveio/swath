// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// RenewableCredentials are credentials refreshed ahead of their expiry
// (RefreshAhead), which a request refused as expired can renew.
type RenewableCredentials interface {
	aws.CredentialsProvider
	// Generation is how many times credentials were fetched: read right
	// after Retrieve, it names those a request signs with.
	Generation() uint64
	// Renew fetches new credentials after a request signed with
	// generation gen was refused as expired, unless that was done since.
	Renew(ctx context.Context, gen uint64) error
	// Refreshes is how many times credentials were fetched after the
	// first, and how many of those fetches Renew forced.
	Refreshes() (refreshes, forced int64)
}

// RefreshAhead returns p's credentials, refreshed ahead of their expiry.
// An aws.CredentialsCache does the caching: one fetch at a time, the
// others waiting each until its own context ends. Below it, each fetch
// moves the credentials' expiry earlier by the smaller of 5 minutes and a
// quarter of the lifetime they had when fetched, so a request is never
// signed with credentials that may expire on its way or by the server's
// clock, and a short lifetime is fetched once, not on every request. A
// fetch that returns credentials already expired, panics or runs past
// fetchTimeout is an error. p is fetched afresh each time: a cache (the
// one config.LoadDefaultConfig returns) is invalidated first, and should
// have no expiry window of its own.
func RefreshAhead(p aws.CredentialsProvider) RenewableCredentials {
	a := &ahead{p: p, now: time.Now}
	return &renewable{CredentialsCache: aws.NewCredentialsCache(a), a: a, lock: make(chan struct{}, 1)}
}

// fetchTimeout bounds one fetch of the provider: a source that never
// answers does not hold every request's credentials for good.
const fetchTimeout = 2 * time.Minute

// ahead is the provider under the cache: each fetch's expiry moved earlier.
type ahead struct {
	p   aws.CredentialsProvider
	now func() time.Time
	gen atomic.Uint64 // fetches that returned usable credentials
}

func (a *ahead) Retrieve(ctx context.Context) (c aws.Credentials, err error) {
	// The cache fetches on a goroutine of its own, where a panic could not
	// be recovered by any caller.
	defer func() {
		if r := recover(); r != nil {
			c, err = aws.Credentials{}, fmt.Errorf("the credential source panicked: %v", r)
		}
	}()
	inv, cached := a.p.(interface{ Invalidate() })
	if cached {
		inv.Invalidate()
	}
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	if c, err = a.p.Retrieve(ctx); err != nil {
		if u := errors.Unwrap(err); cached && u != nil {
			// The cache above says "failed to refresh cached credentials"
			// too: the source's error, once.
			err = u
		}
		return aws.Credentials{}, err
	}
	if c.CanExpire {
		life := c.Expires.Sub(a.now())
		if life <= 0 {
			return aws.Credentials{}, errExpired
		}
		c.Expires = c.Expires.Add(-min(5*time.Minute, life/4))
	}
	a.gen.Add(1)
	return c, nil
}

var errExpired = errors.New("the credential source returned credentials that have already expired")

// renewable is RefreshAhead's credentials.
type renewable struct {
	*aws.CredentialsCache
	a      *ahead
	lock   chan struct{} // one renewal at a time, waited for until the caller's context ends
	forced atomic.Int64
	failed struct { // the last renewal that failed (under lock)
		gen uint64
		err error
	}
}

func (r *renewable) Generation() uint64 { return r.a.gen.Load() }

func (r *renewable) Refreshes() (refreshes, forced int64) {
	return max(int64(r.a.gen.Load())-1, 0), r.forced.Load()
}

// Renew renews generation gen once: the requests refused for it that ask
// while or after one renewal runs find the generation moved on, or share
// its error when it failed. A request that read the generation just after
// a refresh, though it signed before it, may renew once more.
func (r *renewable) Renew(ctx context.Context, gen uint64) error {
	select {
	case r.lock <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("credentials: %w", context.Cause(ctx))
	}
	defer func() { <-r.lock }()
	switch {
	case r.a.gen.Load() != gen:
		return nil
	case r.failed.err != nil && r.failed.gen == gen:
		return r.failed.err
	}
	r.forced.Add(1)
	r.Invalidate()
	_, err := r.Retrieve(ctx)
	if err != nil && ctx.Err() == nil {
		r.failed.gen, r.failed.err = gen, err
	}
	return err
}
