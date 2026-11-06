// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tokensource

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// counting is a token source that counts its refreshes and blocks them
// until released.
type counting struct {
	n       atomic.Int64
	release chan struct{}
	fail    atomic.Bool
}

func (c *counting) Token() (*oauth2.Token, error) {
	n := c.n.Add(1)
	<-c.release
	if c.fail.Load() {
		return nil, fmt.Errorf("refresh %d failed", n)
	}
	return &oauth2.Token{AccessToken: fmt.Sprint("t", n), Expiry: time.Now().Add(time.Hour)}, nil
}

// TestOneRefreshInFlight: many concurrent callers share one refresh; a
// caller whose context ends stops waiting.
func TestOneRefreshInFlight(t *testing.T) {
	c := &counting{release: make(chan struct{})}
	s := NewShared(c, 0)
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := s.Token(context.Background())
			if err != nil || tok.AccessToken != "t1" {
				t.Errorf("%v %v", tok, err)
			}
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Token(ctx); err == nil {
		t.Fatal("a cancelled caller waited")
	}
	close(c.release)
	wg.Wait()
	if n := c.n.Load(); n != 1 {
		t.Fatalf("%d refreshes", n)
	}
}

// TestCommand: Java's contract: the token is the command's trimmed stdout,
// fetched again after the refresh interval; a failing command fails
// closed with its exit code and bounded, escaped stderr, never a stale
// token; String hides the command.
func TestCommand(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "n")
	cmd := fmt.Sprintf(`echo x >> %s; printf ' tok-%%s\n' "$(wc -l < %s | tr -d ' ')"`, counter, counter)
	c := &Command{Cmd: cmd, Refresh: 45 * time.Minute}
	s := NewShared(c, 0)
	get := func() string {
		tok, err := s.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return tok.AccessToken
	}
	if a, b := get(), get(); a != "tok-1" || b != "tok-1" {
		t.Fatalf("%q %q", a, b)
	}
	if tok, _ := c.Token(); tok.Expiry.Sub(time.Now()) < 44*time.Minute {
		t.Fatalf("expiry %v: not the refresh interval", tok.Expiry)
	}
	if strings.Contains(c.String(), "echo") || strings.Contains(fmt.Sprint(c), counter) {
		t.Fatal("the command shows")
	}
	_ = os.Remove(counter)
	for _, tc := range []struct{ cmd, want string }{
		{"echo 'denied\x1b[2J' >&2; exit 3", `--bearer-token-command exited 3: denied\x1b[2J`},
		{"true", "produced no output"},
		{"printf 'a\\nb\\n'", "more than one line"},
	} {
		_, err := (&Command{Cmd: tc.cmd}).Token()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.cmd, err)
		}
	}
	if _, err := (&Command{Cmd: "sleep 5", Timeout: 100 * time.Millisecond}).Token(); err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Errorf("timeout: %v", err)
	}
}

// TestRefreshAfterInterval: an expired token (the refresh interval passed)
// runs the command again; a failure then is an error, not the old token.
func TestRefreshAfterInterval(t *testing.T) {
	c := &counting{release: make(chan struct{})}
	close(c.release)
	exp := &expiring{src: c}
	exp.expired.Store(true)
	s := NewShared(exp, 0)
	if tok, _ := s.Token(context.Background()); tok.AccessToken != "t1" {
		t.Fatal(tok)
	}
	if tok, _ := s.Token(context.Background()); tok.AccessToken != "t2" {
		t.Fatal(tok)
	}
	c.fail.Store(true)
	if tok, err := s.Token(context.Background()); err == nil {
		t.Fatalf("a failed refresh gave %v", tok)
	}
}

// expiring returns tokens already expired once told to.
type expiring struct {
	src     oauth2.TokenSource
	expired atomic.Bool
}

func (e *expiring) Token() (*oauth2.Token, error) {
	tok, err := e.src.Token()
	if err == nil && e.expired.Load() {
		tok.Expiry = time.Now().Add(-time.Second)
	}
	return tok, err
}
