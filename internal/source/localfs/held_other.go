// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/reader/listing"
)

// errHeldUnsupported: a held root is listed on Linux and macOS only.
var errHeldUnsupported = fmt.Errorf("localfs: a held root is listed on Linux and macOS only: %w", errors.ErrUnsupported)

// OpenDir is not supported here (errors.ErrUnsupported).
func OpenDir(*os.File, string, Options) (*Opened, error) { return nil, errHeldUnsupported }

// Lookup is not supported here (errors.ErrUnsupported).
func (o *Opened) Lookup(context.Context, string) (LookupResult, error) {
	return LookupResult{}, errHeldUnsupported
}

// OpenLive is not supported here: a held root is listed on Linux and
// macOS only (listing.ErrUnsupported).
func OpenLive(*os.File) (live.Store, error) {
	return nil, fmt.Errorf("%w: a local source lists on Linux and macOS only", listing.ErrUnsupported)
}
