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
	life    time.Duration
	same    bool
	gate    chan struct{}
	fail    error
	hold    time.Duration
	entered chan struct{} // receives once a gated call waits, when set
	calls   atomic.Int64
}

func (p *issuer) Retrieve(context.Context) (aws.Credentials, error) {
	n := p.calls.Add(1)
	if p.gate != nil && n > 1 {
		if p.entered != nil && n == 2 {
			close(p.entered)
		}
		<-p.gate
		if p.fail != nil {
			return aws.Credentials{}, p.fail
		}
	}
	time.Sleep(p.hold)
	if p.same {
		n = 1
	}
	// Each fetch's expiry is later than the one before, as a renewed
	// session's is, whatever the clock's resolution: two fetches within one
	// tick of a coarse clock would otherwise return identical credentials,
	// which no generation can tell apart.
	return aws.Credentials{AccessKeyID: fmt.Sprintf("AK%d", n), SecretAccessKey: "s", SessionToken: "t", CanExpire: true,
		Expires: time.Now().Add(p.life + time.Duration(p.calls.Load())*time.Millisecond)}, nil
}

// TestRefreshAheadOncePerLifetime: through the SDK's cache, whose clock a
// test cannot set (internal/sdk), credentials of 30 seconds and 1, 5 and 15
// minutes, scaled down 200 times (the window is a quarter of each, as
// unscaled), read by 8 workers for two lifetimes, are fetched three times:
// once at the start and once each three quarters of a lifetime, never once
// per read, never read expired.
func TestRefreshAheadOncePerLifetime(t *testing.T) {
	for _, life := range []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute} {
		life := life / 200
		t.Run(life.String(), func(t *testing.T) {
			t.Parallel()
			p := &issuer{life: life}
			c := RefreshAhead(p)
			end := time.Now().Add(2 * life)
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for time.Now().Before(end) {
						creds, err := c.Retrieve(context.Background())
						if err != nil || !time.Now().Before(creds.Expires) {
							t.Errorf("%v, expires %v", err, creds.Expires)
							return
						}
						time.Sleep(time.Millisecond) // pace the readers
					}
				})
			}
			wg.Wait()
			if p.calls.Load() != 3 {
				t.Fatalf("%d fetches in two lifetimes: want 3", p.calls.Load())
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

// TestRenewOncePerGeneration: 64 requests hold the first generation's
// credentials when one of them renews it; the source gives the same keys
// again. Each then renews with the generation it got with its credentials,
// and none fetches: one renewal, two fetches in all. The credentials the
// renewal replaced are of no generation any more.
func TestRenewOncePerGeneration(t *testing.T) {
	p := &issuer{life: time.Hour, same: true}
	c := RefreshAhead(p)
	var held sync.WaitGroup
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 64 {
		held.Add(1)
		wg.Go(func() {
			creds, gen, err := c.Get(context.Background())
			held.Done()
			if err != nil || gen != 1 {
				t.Errorf("held %v of generation %d: %v", creds.AccessKeyID, gen, err)
			}
			<-start
			if err := c.Renew(context.Background(), gen); err != nil {
				t.Error(err)
			}
		})
	}
	held.Wait()
	old, gen, _ := c.Get(context.Background())
	if err := c.Renew(context.Background(), gen); err != nil {
		t.Fatal(err)
	}
	close(start)
	wg.Wait()
	if _, of := c.(*renewable).a.current(old); of != 0 {
		t.Fatalf("replaced credentials are of generation %d", of)
	}
	if _, gen, _ := c.Get(context.Background()); gen != 2 {
		t.Fatalf("after the renewal: generation %d", gen)
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
	p := &issuer{life: time.Hour, gate: make(chan struct{}), fail: boom, entered: make(chan struct{})}
	c := RefreshAhead(p)
	_, gen, err := c.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := c.Renew(context.Background(), gen); !errors.Is(err, boom) {
				t.Errorf("renewal: %v", err)
			}
		})
	}
	<-p.entered // one renewal is fetching; the others wait for it, or come after
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
	p := &issuer{life: time.Hour, gate: make(chan struct{}), entered: make(chan struct{})}
	defer close(p.gate)
	c := RefreshAhead(p)
	_, gen, err := c.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go c.Renew(context.Background(), gen)
	<-p.entered // that renewal holds the lock, fetching
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Renew(ctx, gen); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("%v after %v", err, time.Since(start))
	}
}
