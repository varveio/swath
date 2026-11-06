// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"fmt"
	"testing"
)

func TestCanonicalRoute(t *testing.T) {
	for in, want := range map[string]string{
		"HTTP://Host:1/storage/v1/b/x/o": "http://host:1/storage/v1/b/x/o",
		"https://B.S3.Amazonaws.com/":    "https://b.s3.amazonaws.com/",
		"http://h/Base//K":               "http://h/Base//K", // path bytes kept
		"http://H":                       "http://h",
		"no-scheme":                      "no-scheme",
	} {
		if got := CanonicalRoute(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// TestCredentialDeadlineIsRequeued: a transient credential failure that
// wraps a deadline (a token exchange's own timeout, the caller's context
// still live) classifies as timeout or transport, not canceled, and its
// exhaustion is requeued; a refused credential stays auth.
func TestCredentialDeadlineIsRequeued(t *testing.T) {
	none := func(error) (Class, bool) { return "", false }
	class := func(err error) Class { return Classify(err, none) }
	for _, c := range []struct {
		err  error
		want Class
	}{
		{&CredentialError{Transient: true, Err: fmt.Errorf("token: %w", context.DeadlineExceeded)}, ClassTransport},
		{&CredentialError{Transient: true, Timeout: true, Err: fmt.Errorf("wait: %w", context.DeadlineExceeded)}, ClassTimeout},
		{&CredentialError{Err: fmt.Errorf("invalid_grant: %w", context.DeadlineExceeded)}, ClassAuth},
		{fmt.Errorf("list: %w", context.Canceled), ClassCanceled},
	} {
		ex := &ExhaustedError{Attempts: 9, Err: c.err}
		if got := class(ex); got != c.want || Requeue(class)(ex) != c.want.Transient() {
			t.Errorf("%v: class %q requeue %v, want %q", c.err, got, Requeue(class)(ex), c.want)
		}
	}
}
