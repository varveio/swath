// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

func newWriterCodec(name string) (compress.Codec, func() error, error) {
	switch name {
	case "", "go":
		return &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}, nil, nil
	case "go-entropy":
		return newEntropyCodec()
	case "native":
		return newNativeCodec()
	default:
		return nil, nil, fmt.Errorf("unknown codec %q", name)
	}
}

// Final codec release belongs to the footer clock on success. Any release
// failure joins the primary error and suppresses successful measurements.
func finishCodec(result *encodeResult, err *error, cleanup func() error) {
	if cleanup == nil {
		return
	}
	start := time.Now()
	*err = errors.Join(*err, cleanup())
	if *err != nil {
		*result = encodeResult{}
		return
	}
	result.Footer += time.Since(start)
}
