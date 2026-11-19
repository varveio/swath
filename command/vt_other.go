// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package command

// enableVT: Unix terminals interpret the redraw's escape sequences.
func enableVT() bool { return true }
