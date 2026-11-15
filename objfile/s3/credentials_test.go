// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// clock is a test's time, moved by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// issuer issues credentials valid for life from each call (AK1, AK2, ...),
// or the same ones every time with same; block, when set, holds every call
// after the first until it closes, which then fails with fail.
type issuer struct {
	life  time.Duration
	clock func() time.Time
	same  bool
	block chan struct{}
	fail  error
	calls atomic.Int64
}

func (p *issuer) Retrieve(context.Context) (aws.Credentials, error) {
	n := p.calls.Add(1)
	if p.block != nil && n > 1 {
		<-p.block
		return aws.Credentials{}, p.fail
	}
	if p.same {
		n = 1
	}
	return aws.Credentials{AccessKeyID: fmt.Sprintf("AK%d", n), SecretAccessKey: "s", SessionToken: "t", CanExpire: true,
		Expires: p.clock().Add(p.life)}, nil
}

// settle waits until no fetch is running.
func settle(c *Credentials) {
	for {
		c.mu.Lock()
		idle := c.flight == nil
		c.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRefreshAheadOncePerLifetime: credentials of 1, 5 and 15 minutes,
// read 200 times a lifetime for ten lifetimes, are fetched once per
// lifetime less the window (a quarter of the lifetime, at most 5 minutes),
// never once per read, and never read past their expiry.
func TestRefreshAheadOncePerLifetime(t *testing.T) {
	for _, life := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute} {
		t.Run(life.String(), func(t *testing.T) {
			clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			p := &issuer{life: life, clock: clk.now}
			c := RefreshAhead(p)
			c.now = clk.now
			for range 2000 {
				creds, err := c.Retrieve(context.Background())
				if err != nil || !clk.now().Before(creds.Expires) {
					t.Fatalf("at %v: %v, expires %v", clk.now(), err, creds.Expires)
				}
				settle(c)
				clk.advance(life / 200)
			}
			used := life - min(MaxRefreshWindow, life/4)
			want := int64(10*life/used) + 1 // the first fetch, then one each used
			if refreshes, forced := c.Refreshes(); p.calls.Load() != want || refreshes != want-1 || forced != 0 {
				t.Fatalf("%d fetches (want %d), %d refreshes, %d forced", p.calls.Load(), want, refreshes, forced)
			}
		})
	}
}

// TestRenewOncePerGeneration: requests refused together for one
// generation renew it once, even when the provider gives the same
// credentials again; a renewal asked for an older generation fetches
// nothing.
func TestRenewOncePerGeneration(t *testing.T) {
	p := &issuer{life: time.Hour, clock: time.Now, same: true}
	c := RefreshAhead(p)
	_, gen, err := c.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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
	if _, forced := c.Refreshes(); p.calls.Load() != 2 || forced != 1 {
		t.Fatalf("%d fetches, %d forced: want 2 and 1", p.calls.Load(), forced)
	}
}

// TestRenewFailureShared: a renewal that fails is fetched once, and its
// error reaches every request refused for that generation, those that ask
// after it ended included; the next Get fetches again.
func TestRenewFailureShared(t *testing.T) {
	boom := errors.New("refused")
	p := &issuer{life: time.Hour, clock: time.Now, block: make(chan struct{}), fail: boom}
	c := RefreshAhead(p)
	_, gen, err := c.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			if i == 15 {
				time.Sleep(10 * time.Millisecond)
				close(p.block)
			}
			if err := c.Renew(context.Background(), gen); !errors.Is(err, boom) {
				t.Errorf("renewal: %v", err)
			}
		})
	}
	wg.Wait()
	if err := c.Renew(context.Background(), gen); !errors.Is(err, boom) || p.calls.Load() != 2 {
		t.Fatalf("after the renewal: %v, %d fetches", err, p.calls.Load())
	}
	if _, _, err := c.Get(context.Background()); !errors.Is(err, boom) || p.calls.Load() != 3 {
		t.Fatalf("next lookup: %v, %d fetches", err, p.calls.Load())
	}
}

// TestRenewWaitEndsWithContext: a request waiting for another's slow
// renewal stops at its own deadline.
func TestRenewWaitEndsWithContext(t *testing.T) {
	p := &issuer{life: time.Hour, clock: time.Now, block: make(chan struct{}), fail: errors.New("late")}
	defer close(p.block)
	c := RefreshAhead(p)
	_, gen, err := c.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go c.Renew(context.Background(), gen)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Renew(ctx, gen); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("%v after %v", err, time.Since(start))
	}
}
