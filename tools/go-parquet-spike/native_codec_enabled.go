//go:build nativezstd && cgo

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
	"github.com/valyala/gozstd"
)

// This optional experiment uses gozstd v1.26.0 / bundled ZSTD 1.5.7. It uses
// level 3 streaming frames with the library's default disabled checksum; frame
// headers and C I/O-buffer copies differ from one-shot JNI compression.
// Writer.Close finalizes each frame; Release frees the owned native state.
type nativeCodec struct {
	mu     sync.Mutex
	writer *gozstd.Writer
	reader *gozstd.Reader
	buffer bytes.Buffer
	closed bool
}

func newNativeCodec() (compress.Codec, func() error, error) {
	codec := &nativeCodec{}
	codec.writer = gozstd.NewWriterLevel(&codec.buffer, 3)
	return codec, codec.Close, nil
}

func (codec *nativeCodec) String() string { return "ZSTD" }

func (codec *nativeCodec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (codec *nativeCodec) Encode(dst, src []byte) (result []byte, err error) {
	codec.mu.Lock()
	defer codec.mu.Unlock()
	defer nativeError(&err)
	if codec.closed {
		return nil, io.ErrClosedPipe
	}
	codec.buffer = *bytes.NewBuffer(dst[:0])
	codec.writer.Reset(&codec.buffer, nil, 3)
	if _, err := codec.writer.Write(src); err != nil {
		return nil, err
	}
	if err := codec.writer.Close(); err != nil {
		return nil, err
	}
	return codec.buffer.Bytes(), nil
}

func (codec *nativeCodec) Decode(dst, src []byte) (result []byte, err error) {
	codec.mu.Lock()
	defer codec.mu.Unlock()
	defer nativeError(&err)
	if codec.closed {
		return nil, io.ErrClosedPipe
	}
	codec.buffer = *bytes.NewBuffer(dst[:0])
	input := bytes.NewReader(src)
	if codec.reader == nil {
		codec.reader = gozstd.NewReader(input)
	} else {
		codec.reader.Reset(input, nil)
	}
	if _, err := codec.reader.WriteTo(&codec.buffer); err != nil {
		return nil, err
	}
	return codec.buffer.Bytes(), nil
}

func (codec *nativeCodec) Close() (err error) {
	codec.mu.Lock()
	defer codec.mu.Unlock()
	defer nativeError(&err)
	if codec.closed {
		return nil
	}
	codec.closed = true
	if codec.reader != nil {
		codec.reader.Release()
		codec.reader = nil
	}
	if codec.writer != nil {
		codec.writer.Release()
		codec.writer = nil
	}
	codec.buffer = bytes.Buffer{}
	return nil
}

// gozstd may panic on internal or allocation failures. Translate those at the
// codec boundary; ordinary errors keep the usual zero-metadata failure path.
func nativeError(err *error) {
	if failure := recover(); failure != nil {
		*err = fmt.Errorf("native zstd: %v", failure)
	}
}
