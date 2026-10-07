//go:build !nativezstd || !cgo

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"

	"github.com/parquet-go/parquet-go/compress"
)

func newNativeCodec() (compress.Codec, func() error, error) {
	return nil, nil, errors.New("native codec requires CGO_ENABLED=1 and -tags nativezstd")
}
