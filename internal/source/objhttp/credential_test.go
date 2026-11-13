// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/varveio/swath/internal/source/tokensource"
)

type tokenFunc func() (*oauth2.Token, error)

func (f tokenFunc) Token() (*oauth2.Token, error) { return f() }

func TestWaitToken(t *testing.T) {
	if tok, err := WaitToken(t.Context(), nil, "anonymous", false, time.Second, nil); tok != nil || err != nil {
		t.Fatalf("nil source: %v, %v", tok, err)
	}
	want := &oauth2.Token{AccessToken: "token", Expiry: time.Now().Add(time.Hour)}
	ts := tokensource.NewShared(oauth2.StaticTokenSource(want))
	if tok, err := WaitToken(t.Context(), ts, "test", false, time.Second, nil); tok != want || err != nil {
		t.Fatalf("token: %v, %v", tok, err)
	}
	for _, tc := range []struct {
		name               string
		err                error
		transient, timeout bool
	}{
		{"refused", errors.New("invalid grant"), false, false},
		{"transient", errors.New("unavailable"), true, false},
		{"exchange timeout", fmt.Errorf("exchange: %w", context.DeadlineExceeded), true, true},
		{"terminal deadline", fmt.Errorf("refused: %w", context.DeadlineExceeded), false, true},
	} {
		for _, discovered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s discovered=%v", tc.name, discovered), func(t *testing.T) {
				ts := tokensource.NewShared(tokenFunc(func() (*oauth2.Token, error) { return nil, tc.err }))
				calls := 0
				tok, err := WaitToken(t.Context(), ts, "test source", discovered, time.Second, func(err error) bool {
					calls++
					if err != tc.err {
						t.Errorf("predicate got %v", err)
					}
					return tc.transient
				})
				var ce *CredentialError
				if tok != nil || !errors.As(err, &ce) || ce.Source != "test source" || ce.Discovered != discovered || ce.Transient != tc.transient || ce.Timeout != tc.timeout || ce.Err != tc.err || calls != 1 {
					t.Fatalf("token %v, error %+v, predicate calls %d", tok, ce, calls)
				}
				if ce.CredentialRefused() != !tc.transient || ce.CredentialDiscovered() != discovered || ce.CredentialSource() != "test source" || !strings.Contains(err.Error(), tc.err.Error()) {
					t.Fatalf("credential error: %v", err)
				}
			})
		}
	}
}

func TestWaitTokenContextEnded(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			}
			cancel()
			release := make(chan struct{})
			defer close(release)
			ts := tokensource.NewShared(tokenFunc(func() (*oauth2.Token, error) {
				<-release
				return &oauth2.Token{AccessToken: "late"}, nil
			}))
			tok, err := WaitToken(ctx, ts, "test source", true, 7*time.Second, func(error) bool { t.Error("classified a wait timeout"); return false })
			var ce *CredentialError
			if tok != nil || !errors.As(err, &ce) || !ce.Transient || !ce.Timeout || ce.Source != "test source" || !ce.Discovered {
				t.Fatalf("token %v, error %+v", tok, ce)
			}
			if want := fmt.Sprintf("no token within 7s: %v", ctx.Err()); ce.Err.Error() != want {
				t.Fatalf("message %q, want %q", ce.Err, want)
			}
		})
	}
}

func TestTransientStatus(t *testing.T) {
	for status, want := range map[int]bool{-1: false, 0: false, 200: false, 400: false, 401: false, 403: false,
		407: false, 408: true, 409: false, 428: false, 429: true, 430: false, 499: false, 500: true, 599: true, 600: true} {
		if got := TransientStatus(status); got != want {
			t.Errorf("%d: %v, want %v", status, got, want)
		}
	}
}
