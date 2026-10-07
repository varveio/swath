// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCodecCleanupSuppressesSuccessAndPreservesErrors(t *testing.T) {
	failure := errors.New("release failed")
	primary := errors.New("write failed")
	for _, initial := range []error{nil, primary} {
		result := encodeResult{Encode: time.Second}
		err := initial
		calls := 0
		finishCodec(&result, &err, func() error { calls++; return failure })
		if calls != 1 || !errors.Is(err, failure) || initial != nil && !errors.Is(err, initial) || !reflect.DeepEqual(result, encodeResult{}) {
			t.Fatalf("cleanup result=%+v error=%v calls=%d", result, err, calls)
		}
	}
}
