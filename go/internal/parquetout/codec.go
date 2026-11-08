// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	pqzstd "github.com/parquet-go/parquet-go/compress/zstd"
)

// codec is parquet-go's ZSTD codec (String, CompressionCodec, Decode) with
// Encode overridden: pure-Go ZSTD (klauspost, SpeedDefault) with all-literal
// entropy compression enabled, which parquet-go's own encoder does not set.
// Each writer owns one; EncodeAll is safe for concurrent use, so page
// columns can be compressed in parallel. The writer closes it once, after
// its page work has joined: encodePage waits for its column goroutines, and
// close and abort run on the part's one encoder goroutine.
type codec struct {
	pqzstd.Codec
	encoder *zstd.Encoder
}

func newCodec(concurrency int) (*codec, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithZeroFrames(true),
		zstd.WithAllLitEntropyCompression(true),
	)
	if err != nil {
		return nil, err
	}
	return &codec{encoder: encoder}, nil
}

var _ compress.Codec = (*codec)(nil)

func (c *codec) Encode(dst, src []byte) ([]byte, error) {
	return c.encoder.EncodeAll(src, dst[:0]), nil
}

func (c *codec) Close() error { return c.encoder.Close() }
