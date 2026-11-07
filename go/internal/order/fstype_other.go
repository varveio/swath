// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package order

// fsType is not read on this platform.
func fsType(string) string { return "" }
