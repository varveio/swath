// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// classes is every error class, the two retryable ones last.
var classes = append(append([]error{}, lasting...), ErrThrottled, ErrUnavailable)

// TestClassesAreDistinct: each class matches itself, wrapped or joined
// with a cause, and no other class.
func TestClassesAreDistinct(t *testing.T) {
	cause := errors.New("store said no")
	for _, class := range classes {
		for _, err := range []error{
			class,
			fmt.Errorf("list s3://bucket/a/: %w", class),
			fmt.Errorf("%w: %w", class, cause),
			errors.Join(cause, class),
		} {
			if !errors.Is(err, class) {
				t.Errorf("%q does not match %q", err, class)
			}
			for _, other := range classes {
				if other != class && errors.Is(err, other) {
					t.Errorf("%q matches %q", err, other)
				}
			}
		}
	}
}

func TestRetryable(t *testing.T) {
	cause := errors.New("connection reset")
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no class", cause, false},
		{"cancelled", context.Canceled, false},
		{"deadline", fmt.Errorf("walk: %w", context.DeadlineExceeded), false},
		{"throttled", ErrThrottled, true},
		{"unavailable", ErrUnavailable, true},
		{"wrapped unavailable", fmt.Errorf("list: %w: %w", ErrUnavailable, cause), true},
		{"both retryable", errors.Join(ErrThrottled, ErrUnavailable), true},
		{"auth beside unavailable", errors.Join(ErrUnavailable, fmt.Errorf("probe: %w", ErrAuth)), false},
		{"not found beside throttled", fmt.Errorf("%w: %w", ErrThrottled, ErrNotFound), false},
	} {
		if got := Retryable(c.err); got != c.want {
			t.Errorf("%s: Retryable(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
	for _, class := range lasting {
		if Retryable(class) || Retryable(fmt.Errorf("page: %w", class)) {
			t.Errorf("Retryable(%q) = true", class)
		}
	}
}
