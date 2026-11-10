// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package tokensource shares an OAuth token among a client's concurrent
// requests: golang.org/x/oauth2's ReuseTokenSource caches it
// until it expires, and golang.org/x/sync/singleflight lets one refresh run
// while the other callers wait on it or on their own context. Command is a
// token source that runs a shell command (--s3-bearer-token-command).
package tokensource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"
)

// Shared is a token source safe for concurrent use: the cached token is
// returned until it expires; a refresh runs once however many callers need
// it, and a caller whose context ends stops waiting (the refresh goes on).
type Shared struct {
	ts oauth2.TokenSource
	g  singleflight.Group
}

// NewShared caches src's tokens until expiry. A src that is already a
// reuse source keeps its own expiry margin; any other src gets oauth2's
// default.
func NewShared(src oauth2.TokenSource) *Shared {
	return &Shared{ts: oauth2.ReuseTokenSource(nil, src)}
}

// Token is the current token.
func (s *Shared) Token(ctx context.Context) (*oauth2.Token, error) {
	ch := s.g.DoChan("token", func() (any, error) { return s.ts.Token() })
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*oauth2.Token), nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// DefaultRefresh is Java swath's --s3-bearer-token-refresh-interval default:
// under a typical one-hour OAuth token lifetime.
const DefaultRefresh = 45 * time.Minute

const (
	cmdTimeout    = 30 * time.Second // one run of the command
	cmdWaitDelay  = 2 * time.Second  // pipes held open after it exits
	cmdMaxCapture = 64 << 10         // stdout and stderr, each
	cmdMaxDiag    = 4096             // stderr quoted in an error
)

// Command is an oauth2.TokenSource running a shell command (/bin/sh -c)
// whose stdout is a bearer token, valid for Refresh (Java's rule: the
// token is fetched again after the interval, not from its own expiry).
// A command that fails, times out, prints nothing or more than one line
// is an error: a stale token is never reused. String never shows the
// command, which may hold a secret.
type Command struct {
	Cmd     string
	Refresh time.Duration // 0: DefaultRefresh
	Timeout time.Duration // 0: 30 s
	Now     func() time.Time
}

func (c *Command) String() string { return "tokensource.Command[<redacted>]" }

// Token runs the command once.
func (c *Command) Token() (*oauth2.Token, error) {
	now, refresh, timeout := time.Now, c.Refresh, c.Timeout
	if c.Now != nil {
		now = c.Now
	}
	if refresh <= 0 {
		refresh = DefaultRefresh
	}
	if timeout <= 0 {
		timeout = cmdTimeout
	}
	at := now()
	tok, err := run(c.Cmd, timeout)
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{AccessToken: tok, TokenType: "Bearer", Expiry: at.Add(refresh)}, nil
}

// cappedBuffer keeps the first max bytes written and notes an overflow.
type cappedBuffer struct {
	bytes.Buffer
	max      int
	overflow bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.max - c.Len(); room < len(p) {
		c.overflow = true
		if room > 0 {
			c.Buffer.Write(p[:room])
		}
		return len(p), nil
	}
	return c.Buffer.Write(p)
}

// run runs the command once, bounded in time and output.
func run(command string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := shellCommand(ctx, command)
	cmd.WaitDelay = cmdWaitDelay
	out, errb := &cappedBuffer{max: cmdMaxCapture}, &cappedBuffer{max: cmdMaxCapture}
	cmd.Stdout, cmd.Stderr = out, errb
	err := cmd.Run()
	switch {
	case ctx.Err() != nil:
		return "", fmt.Errorf("--s3-bearer-token-command did not exit within %s", timeout)
	case err != nil:
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			return "", fmt.Errorf("--s3-bearer-token-command failed to start: %v", err)
		}
		msg := fmt.Sprintf("--s3-bearer-token-command exited %d", ee.ExitCode())
		if d := diagText(strings.TrimSpace(errb.String())); d != "" {
			msg += ": " + d
		}
		if errb.overflow {
			msg += " [stderr truncated]"
		}
		return "", errors.New(msg)
	case out.overflow:
		return "", fmt.Errorf("--s3-bearer-token-command stdout exceeded %d bytes", cmdMaxCapture)
	}
	tok := strings.TrimSpace(out.String())
	if tok == "" {
		return "", errors.New("--s3-bearer-token-command produced no output")
	}
	if strings.ContainsAny(tok, "\r\n") {
		return "", errors.New("--s3-bearer-token-command printed more than one line")
	}
	return tok, nil
}

// diagText bounds the command's stderr and escapes control characters, so
// it cannot forge lines in a log.
func diagText(s string) string {
	var b strings.Builder
	for _, r := range s {
		e := string(r)
		if r < 0x20 || r == 0x7f {
			e = fmt.Sprintf(`\x%02x`, r)
		}
		if b.Len()+len(e) > cmdMaxDiag {
			break
		}
		b.WriteString(e)
	}
	return b.String()
}
