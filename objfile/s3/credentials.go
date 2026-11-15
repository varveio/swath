// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// RenewableCredentials are credentials refreshed ahead of their expiry
// (RefreshAhead), which a request refused as expired can renew.
type RenewableCredentials interface {
	aws.CredentialsProvider
	// Get is Retrieve with the generation of the fetch that gave the
	// credentials, for Renew: 0 when a later fetch has replaced them.
	Get(ctx context.Context) (aws.Credentials, uint64, error)
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
// fetch that returns credentials already expired is an error, and so is
// one of p that panics. p is fetched afresh each time: a cache (the one
// config.LoadDefaultConfig returns) is invalidated first, and should have
// no expiry window of its own.
//
// What it does not change, the SDK's behaviour as before: beneath such a
// cache the source runs on the cache's own goroutine, so its panic ends
// the process, and a source that ignores its context keeps that fetch,
// which later fetches join, running past fetchTimeout. Instance-role
// credentials (ec2rolecreds) lose the SDK's rule that keeps the expiry of
// those with under 15 minutes left: if the instance keeps returning the
// same expiry, each fetch comes when a quarter of what remained is left,
// closer together until it rotates them.
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

	mu   sync.Mutex
	gen  uint64          // fetches that returned usable credentials
	last aws.Credentials // the last of them, as given to the cache
}

// current is the latest generation, and c's when c is its credentials (0
// for an older fetch's).
func (a *ahead) current(c aws.Credentials) (latest, of uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	l := a.last
	if c.AccessKeyID == l.AccessKeyID && c.SecretAccessKey == l.SecretAccessKey && c.SessionToken == l.SessionToken && c.Expires.Equal(l.Expires) {
		of = a.gen
	}
	return a.gen, of
}

func (a *ahead) Retrieve(ctx context.Context) (c aws.Credentials, err error) {
	// The cache fetches on a goroutine of its own, where a panic of p
	// could not be recovered by any caller.
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
	a.mu.Lock()
	a.gen, a.last = a.gen+1, c
	a.mu.Unlock()
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

func (r *renewable) Get(ctx context.Context) (aws.Credentials, uint64, error) {
	c, err := r.Retrieve(ctx)
	if err != nil {
		return c, 0, err
	}
	_, gen := r.a.current(c)
	return c, gen, nil
}

func (r *renewable) Refreshes() (refreshes, forced int64) {
	gen, _ := r.a.current(aws.Credentials{})
	return max(int64(gen)-1, 0), r.forced.Load()
}

// Renew renews generation gen once: the requests refused for it that ask
// while or after one renewal runs find the generation moved on, or share
// its error when it failed. A renewal whose every caller left before it
// failed records nothing: a later one for the same generation fetches once
// more.
func (r *renewable) Renew(ctx context.Context, gen uint64) error {
	select {
	case r.lock <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("credentials: %w", context.Cause(ctx))
	}
	defer func() { <-r.lock }()
	switch latest, _ := r.a.current(aws.Credentials{}); {
	case gen != latest:
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
