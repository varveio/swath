// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin && !windows

package localfs

import "github.com/varveio/swath/go/internal/source"

// measureSpace: other platforms do not measure their filesystem's space.
func measureSpace(string, bool) *source.FilesystemSpace { return nil }
