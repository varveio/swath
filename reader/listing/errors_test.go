// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"errors"
	"fmt"
	"testing"
)

// classes names every exported error class, so that two names for one
// value fail TestClassesAreDistinct.
var classes = []struct {
	name string
	err  error
}{
	{"ErrInvalidRequest", ErrInvalidRequest},
	{"ErrInvalidToken", ErrInvalidToken},
	{"ErrNotFound", ErrNotFound},
	{"ErrNotDirectory", ErrNotDirectory},
	{"ErrOtherDevice", ErrOtherDevice},
	{"ErrAuth", ErrAuth},
	{"ErrUnsupported", ErrUnsupported},
	{"ErrInvalidPage", ErrInvalidPage},
	{"ErrOutOfScope", ErrOutOfScope},
	{"ErrUnavailable", ErrUnavailable},
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
