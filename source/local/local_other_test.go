// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package local_test

import (
	"errors"
	"os"
	"testing"

	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/source/local"
)

// TestUnsupported: a local source lists on Linux and macOS only.
func TestUnsupported(t *testing.T) {
	root, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := local.Open(root); !errors.Is(err, listing.ErrUnsupported) {
		t.Errorf("%v, want ErrUnsupported", err)
	}
}
