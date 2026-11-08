// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/format"
)

// codec is pure-Go ZSTD (klauspost, SpeedDefault) with all-literal entropy
// compression enabled. A router's writers share one (a writer made alone
// owns one). Its states are single-state zstd encoders, made as calls
// need them up to limit and reused last-in first-out, so a state is only
// made, and only grows (to the largest page it compressed, about 11 MiB
// once a page passes one zstd block with lower encoder memory, which
// changes allocation only, not the bytes: EncoderBytes), when that many calls
// run at once: a pool that hands its states round in turn warms every one
// of them. The limit is the encoders admitted (setLimit, from the
// router's Admit). Each call compresses on one state, so page columns, of
// one part or several, are compressed in parallel.
type codec struct {
	mu      sync.Mutex
	cond    sync.Cond
	free    []*zstd.Encoder // idle states, the most recently used last
	made    int             // states made
	limit   int             // states that may be made
	decoder *zstd.Decoder
	closed  bool
}

func newCodec(limit int) (*codec, error) {
	c := &codec{limit: max(1, limit)}
	c.cond.L = &c.mu
	return c, nil
}

// newState is one zstd state with the writer's options (EncodeAll's
// output does not depend on the state: it resets it every call).
func newState() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithZeroFrames(true),
		zstd.WithAllLitEntropyCompression(true),
		zstd.WithLowerEncoderMem(true),
	)
}

// setLimit raises the states that may be made to n (never lowers it).
func (c *codec) setLimit(n int) {
	c.mu.Lock()
	if n > c.limit {
		c.limit = n
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

// states is the states made so far (tests).
func (c *codec) states() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.made
}

// take leases a state: the most recently returned, a new one while fewer
// than limit are made, or else the next returned.
func (c *codec) take() (*zstd.Encoder, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.closed {
			return nil, io.ErrClosedPipe
		}
		if n := len(c.free); n > 0 {
			e := c.free[n-1]
			c.free = c.free[:n-1]
			return e, nil
		}
		if c.made < c.limit {
			e, err := newState()
			if err != nil {
				return nil, err
			}
			c.made++
			return e, nil
		}
		c.cond.Wait()
	}
}

func (c *codec) give(e *zstd.Encoder) {
	c.mu.Lock()
	c.free = append(c.free, e)
	c.cond.Signal()
	c.mu.Unlock()
}

var _ compress.Codec = (*codec)(nil)

func (c *codec) String() string { return "ZSTD" }

func (c *codec) CompressionCodec() format.CompressionCodec { return format.Zstd }

func (c *codec) Encode(dst, src []byte) ([]byte, error) {
	e, err := c.take()
	if err != nil {
		return nil, err
	}
	defer c.give(e)
	return e.EncodeAll(src, dst[:0]), nil
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

// Close closes every state; the callers have returned theirs (the router
// closes it once every writer has ended).
func (c *codec) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cond.Broadcast()
	if c.decoder != nil {
		c.decoder.Close()
	}
	var err error
	for _, e := range c.free {
		err = errors.Join(err, e.Close())
	}
	c.free = nil
	return err
}
