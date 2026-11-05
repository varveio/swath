// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
)

// This optional pure-Go codec changes only all-literal entropy compression.
// Its other settings match parquet-go's SpeedDefault codec. Contexts belong
// to one writer and are explicitly closed through the factory's cleanup.
type entropyCodec struct {
	mu      sync.RWMutex // Encode holds R: klauspost EncodeAll is safe for concurrent use
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	closed  bool
}

func newEntropyCodec(concurrency int, level zstd.EncoderLevel) (compress.Codec, func() error, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderCRC(false),
		zstd.WithZeroFrames(true),
		zstd.WithAllLitEntropyCompression(true),
	)
	if err != nil {
		return nil, nil, err
	}
	codec := &entropyCodec{encoder: encoder}
	return codec, codec.Close, nil
}

func (codec *entropyCodec) String() string { return "ZSTD" }

func (codec *entropyCodec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (codec *entropyCodec) Encode(dst, src []byte) ([]byte, error) {
	codec.mu.RLock()
	defer codec.mu.RUnlock()
	if codec.closed {
		return nil, io.ErrClosedPipe
	}
	return codec.encoder.EncodeAll(src, dst[:0]), nil
}

func (codec *entropyCodec) Decode(dst, src []byte) ([]byte, error) {
	codec.mu.Lock()
	defer codec.mu.Unlock()
	if codec.closed {
		return nil, io.ErrClosedPipe
	}
	if codec.decoder == nil {
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		codec.decoder = decoder
	}
	return codec.decoder.DecodeAll(src, dst[:0])
}

func (codec *entropyCodec) Close() error {
	codec.mu.Lock()
	defer codec.mu.Unlock()
	if codec.closed {
		return nil
	}
	codec.closed = true
	if codec.decoder != nil {
		codec.decoder.Close()
	}
	err := codec.encoder.Close()
	codec.decoder, codec.encoder = nil, nil
	return err
}

// newCodec returns the selected pure-Go ZSTD codec and its cleanup.
// "go" is parquet-go's SpeedDefault wrapper; "go-entropy" (default) enables
// all-literal entropy compression (the Oct spike's recommended pure-Go policy).
func newCodec(name string, concurrency int) (compress.Codec, func() error, error) {
	switch name {
	case "go":
		return &parquet.Zstd, func() error { return nil }, nil
	case "", "go-entropy":
		return newEntropyCodec(concurrency, zstd.SpeedDefault)
	case "go-entropy-fastest": // experiment: klauspost SpeedFastest (~zstd level 1)
		return newEntropyCodec(concurrency, zstd.SpeedFastest)
	}
	return nil, nil, fmt.Errorf("unknown codec %q (go|go-entropy|go-entropy-fastest)", name)
}
