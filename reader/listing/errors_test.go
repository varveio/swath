// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// classes names every exported error class, listed here rather than taken
// from lasting, so that two names for one value fail
// TestClassesAreDistinct.
var classes = []struct {
	name      string
	err       error
	retryable bool
}{
	{"ErrInvalidRequest", ErrInvalidRequest, false},
	{"ErrInvalidToken", ErrInvalidToken, false},
	{"ErrNotFound", ErrNotFound, false},
	{"ErrNotDirectory", ErrNotDirectory, false},
	{"ErrOtherDevice", ErrOtherDevice, false},
	{"ErrAuth", ErrAuth, false},
	{"ErrUnsupported", ErrUnsupported, false},
	{"ErrInvalidPage", ErrInvalidPage, false},
	{"ErrOutOfScope", ErrOutOfScope, false},
	{"ErrBudgetExceeded", ErrBudgetExceeded, false},
	{"ErrThrottled", ErrThrottled, true},
	{"ErrUnavailable", ErrUnavailable, true},
}

// TestClassesAreDistinct: each class matches itself, wrapped or joined
// with a cause, and no class at another name.
func TestClassesAreDistinct(t *testing.T) {
	cause := errors.New("store said no")
	for i, class := range classes {
		for _, err := range []error{
			class.err,
			fmt.Errorf("list s3://bucket/a/: %w", class.err),
			fmt.Errorf("%w: %w", class.err, cause),
			errors.Join(cause, class.err),
		} {
			if !errors.Is(err, class.err) {
				t.Errorf("%s: %q does not match it", class.name, err)
			}
			for j, other := range classes {
				if i != j && errors.Is(err, other.err) {
					t.Errorf("%s: %q matches %s", class.name, err, other.name)
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
	for _, class := range classes {
		if got := Retryable(fmt.Errorf("page: %w", class.err)); got != class.retryable {
			t.Errorf("Retryable(%s) = %v, want %v", class.name, got, class.retryable)
		}
		if !class.retryable && Retryable(errors.Join(ErrThrottled, class.err)) {
			t.Errorf("Retryable(ErrThrottled beside %s) = true", class.name)
		}
	}
}
