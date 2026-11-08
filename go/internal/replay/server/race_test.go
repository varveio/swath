// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build race

package server

// raceEnabled shortens or skips the load tests whose bounds are the
// server's own cost, which the race detector multiplies.
const raceEnabled = true
