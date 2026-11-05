// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package walk

import "github.com/varveio/swath/tools/swath-local-spike/record"

func readFast(*worker, dirJob, uint32) ([]record.Row, []kid, bool) { return nil, nil, false }

func fastReaderAvailable(*Options) bool { return false }

func statxMaskFor(*Options) uint32 { return 0 }
