// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

func TestIsRefusal(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{semantic(KindRow, "size", nil, "x"), true},
		{fmt.Errorf("wrapped: %w", semantic(KindSchema, "", nil, "x")), true},
		{&seekparquet.RefusalError{Code: seekparquet.CodeDigestMismatch}, true},
		{errors.Join(semantic(KindMetadata, "", nil, "x"), nil), true},
		{&seekparquet.UnsupportedError{Code: "x"}, false},
		{seekparquet.ErrClosed, false},
		{fmt.Errorf("%w: limit", seekparquet.ErrInvalidRequest), false},
		{seekparquet.ErrLimitExceeded, false},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
		{fs.ErrNotExist, false},
		{fs.ErrPermission, false},
		{errors.New("storage"), false},
		{nil, false},
	} {
		if got := IsRefusal(tc.err); got != tc.want {
			t.Errorf("IsRefusal(%v) = %t", tc.err, got)
		}
	}
}
