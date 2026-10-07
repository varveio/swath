//go:build !nativezstd || !cgo

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestNativeCodecUnavailableWithoutBuildTagAndCgo(t *testing.T) {
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40, Codec: "native"}
	var output bytes.Buffer
	result, err := encodeRows(&output, probeRows(1), config)
	if err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=1") || output.Len() != 0 || !reflect.DeepEqual(result, encodeResult{}) {
		t.Fatalf("unavailable codec produced result=%+v bytes=%d error=%v", result, output.Len(), err)
	}
}
