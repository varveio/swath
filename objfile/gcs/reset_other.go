// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package gcs

// connReset is false: elsewhere a reset's message is the one the SDK's
// policy (storage.ShouldRetry) matches.
func connReset(error) bool { return false }
