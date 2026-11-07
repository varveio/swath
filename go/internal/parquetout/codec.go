// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
)

// codec is pure-Go ZSTD (klauspost, SpeedDefault) with all-literal entropy
// compression enabled. A router's writers share one (a writer made alone
// owns one); EncodeAll is safe for concurrent use, so page columns, of one
// part or several, are compressed in parallel, each call on one of its
// states.
type codec struct {
	mu      sync.RWMutex
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	closed  bool
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

func (c *codec) String() string { return "ZSTD" }

func (c *codec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (c *codec) Encode(dst, src []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, io.ErrClosedPipe
	}
	return c.encoder.EncodeAll(src, dst[:0]), nil
}

func (c *codec) Decode(dst, src []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, io.ErrClosedPipe
	}
	if c.decoder == nil {
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		c.decoder = d
	}
	return c.decoder.DecodeAll(src, dst[:0])
}

func (c *codec) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.decoder != nil {
		c.decoder.Close()
	}
	return c.encoder.Close()
}
