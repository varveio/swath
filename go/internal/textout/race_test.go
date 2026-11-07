// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build race

package textout

// raceEnabled: the race detector drops a quarter of sync.Pool puts, so
// allocation counts measure it, not the compressor.
const raceEnabled = true
