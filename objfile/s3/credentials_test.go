// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// issuer issues credentials valid for life from each call (AK1, AK2, ...),
// or the same ones every time with same. With gate set, every call after
// the first waits for it to close, then fails with fail if set. hold, when
// set, is how long each call takes.
type issuer struct {
	life  time.Duration
	now   func() time.Time
	same  bool
	gate  chan struct{}
	fail  error
	hold  time.Duration
	calls atomic.Int64
}

func (p *issuer) Retrieve(context.Context) (aws.Credentials, error) {
	n := p.calls.Add(1)
	if p.gate != nil && n > 1 {
		<-p.gate
		if p.fail != nil {
			return aws.Credentials{}, p.fail
		}
	}
	time.Sleep(p.hold)
	if p.same {
		n = 1
	}
	now := time.Now
	if p.now != nil {
		now = p.now
	}
	return aws.Credentials{AccessKeyID: fmt.Sprintf("AK%d", n), SecretAccessKey: "s", SessionToken: "t", CanExpire: true,
		Expires: now().Add(p.life)}, nil
}

// TestRefreshAheadOncePerLifetime: credentials of 30 seconds and 1, 5 and
// 15 minutes, read 200 times a lifetime for ten lifetimes through a cache
// keeping them until the expiry the wrapper gives (as aws.CredentialsCache
// does), are fetched once per lifetime less the window, the smaller of 5
// minutes and a quarter of it: never once per read, never read expired.
func TestRefreshAheadOncePerLifetime(t *testing.T) {
	for _, life := range []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute} {
		t.Run(life.String(), func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			p := &issuer{life: life, now: clock}
			a := &ahead{p: p, now: clock}
			var cached aws.Credentials
			for range 2000 {
				if !now.Before(cached.Expires) {
					c, err := a.Retrieve(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					cached = c
				}
				now = now.Add(life / 200)
			}
			used := life - min(5*time.Minute, life/4)
			if want := int64(10*life/used) + 1; p.calls.Load() != want || a.gen.Load() != uint64(want) {
				t.Fatalf("%d fetches, generation %d: want %d", p.calls.Load(), a.gen.Load(), want)
			}
		})
	}
}

// TestRefreshAheadRefusesExpired: a source that returns credentials already
// expired is an error, each lookup one fetch: never a success, never a loop.
func TestRefreshAheadRefusesExpired(t *testing.T) {
	p := &issuer{life: -time.Second}
	c := RefreshAhead(p)
	for i := 1; i <= 2; i++ {
		if _, err := c.Retrieve(context.Background()); !errors.Is(err, errExpired) || p.calls.Load() != int64(i) {
			t.Fatalf("lookup %d: %v, %d fetches", i, err, p.calls.Load())
		}
	}
}

// TestRefreshAheadPanic: a source that panics fails the lookup instead of
// the process.
func TestRefreshAheadPanic(t *testing.T) {
	c := RefreshAhead(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { panic("boom") }))
	if _, err := c.Retrieve(context.Background()); err == nil || !strings.Contains(err.Error(), "panicked: boom") {
		t.Fatalf("%v", err)
	}
}

// TestRefreshAheadOneFetchForMany: callers that overlap at an expiry share
// one fetch, and those still waiting get the credentials when the one that
// started it gives up.
func TestRefreshAheadOneFetchForMany(t *testing.T) {
	p := &issuer{life: time.Hour, hold: 200 * time.Millisecond}
	c := RefreshAhead(p)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Retrieve(ctx); err == nil {
		t.Fatal("the starter did not give up")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if creds, err := c.Retrieve(context.Background()); err != nil || creds.AccessKeyID != "AK1" {
				t.Errorf("%v %v", creds.AccessKeyID, err)
			}
		})
	}
	wg.Wait()
	if p.calls.Load() != 1 {
		t.Fatalf("%d fetches for overlapping callers", p.calls.Load())
	}
}

// TestRenewOncePerGeneration: 64 requests refused together for one
// generation renew it once, though the source gives the same credentials
// again; a renewal for an older generation fetches nothing.
func TestRenewOncePerGeneration(t *testing.T) {
	p := &issuer{life: time.Hour, same: true, hold: 20 * time.Millisecond}
	c := RefreshAhead(p)
	if _, err := c.Retrieve(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if err := c.Renew(context.Background(), gen); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := c.Renew(context.Background(), gen); err != nil {
		t.Fatal(err)
	}
	if refreshes, forced := c.Refreshes(); p.calls.Load() != 2 || refreshes != 1 || forced != 1 {
		t.Fatalf("%d fetches, %d refreshes, %d forced: want 2, 1, 1", p.calls.Load(), refreshes, forced)
	}
}

// TestRenewFailureShared: a renewal that fails is fetched once, and its
// error reaches every request refused for that generation, those asking
// after it ended included; the next lookup fetches again.
func TestRenewFailureShared(t *testing.T) {
	boom := errors.New("refused")
	p := &issuer{life: time.Hour, gate: make(chan struct{}), fail: boom}
	c := RefreshAhead(p)
	if _, err := c.Retrieve(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := c.Renew(context.Background(), gen); !errors.Is(err, boom) {
				t.Errorf("renewal: %v", err)
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(p.gate)
	wg.Wait()
	if err := c.Renew(context.Background(), gen); !errors.Is(err, boom) || p.calls.Load() != 2 {
		t.Fatalf("after the renewal: %v, %d fetches", err, p.calls.Load())
	}
	if _, err := c.Retrieve(context.Background()); !errors.Is(err, boom) || p.calls.Load() != 3 {
		t.Fatalf("next lookup: %v, %d fetches", err, p.calls.Load())
	}
}

// TestRenewWaitEndsWithContext: a request waiting for another's slow
// renewal stops at its own deadline.
func TestRenewWaitEndsWithContext(t *testing.T) {
	p := &issuer{life: time.Hour, gate: make(chan struct{})}
	defer close(p.gate)
	c := RefreshAhead(p)
	if _, err := c.Retrieve(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()
	go c.Renew(context.Background(), gen)
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Renew(ctx, gen); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("%v after %v", err, time.Since(start))
	}
}
