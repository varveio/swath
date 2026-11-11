// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"testing"

	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestPageSizeCapped: the diagnostic --page-size takes up to Azure's
// 5,000 (keyrange.MaxPageFlag); this provider asks for at most the 1,000
// its service returns.
func TestPageSizeCapped(t *testing.T) {
	for _, c := range []struct{ flag, want int }{{0, 0}, {7, 7}, {keyrange.MaxPageFlag, keyrange.PageKeys}} {
		p := &provider[Object]{opt: Options{Policy: keyrange.Policy{PageSize: c.flag}}}
		if got := p.engineOptions(false).PageSize; got != c.want {
			t.Errorf("--page-size %d: pages of %d, want %d", c.flag, got, c.want)
		}
	}
}
