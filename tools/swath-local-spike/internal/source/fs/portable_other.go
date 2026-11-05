// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package fs

import (
	"context"

	"github.com/varveio/swath/tools/swath-local-spike/internal/source"
)

// No native engine yet: getattrlistbulk(2) on macOS and directory
// enumeration with FindFirstFileEx on Windows (both return sizes and times
// with the names) are the natural next ones.
const nativeEngine = false

type chunk struct{}

func (p *Provider) expandNative(ctx context.Context, r Range, e source.Emitter[Range], st *Stats) error {
	return p.expandPortable(ctx, r, e, st)
}
