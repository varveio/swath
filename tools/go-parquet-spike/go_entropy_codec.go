// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
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
	mu      sync.Mutex
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	closed  bool
}

func newEntropyCodec() (compress.Codec, func() error, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
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
	codec.mu.Lock()
	defer codec.mu.Unlock()
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
