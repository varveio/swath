// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package dataset

func dirSyncUnsupported(error) bool { return false }
