// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build race

package store

// raceEnabled shortens the one-row paging walks, which decode a page per
// row and run about ten times slower under the race detector.
const raceEnabled = true
