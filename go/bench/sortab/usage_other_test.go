// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package sortab

import "os"

// usage leaves the CPU times and peak RSS zero: this platform has no
// rusage (the harness measures on Linux).
func usage(*os.ProcessState, *Attempt) {}
