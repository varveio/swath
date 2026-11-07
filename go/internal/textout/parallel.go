// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/schema"
)

// stopper is a sink's formatter pipeline as its Out sees it.
type stopper interface {
	// finish ends the pipeline and joins its goroutines: with abort
	// false after every queued batch is written (the first error, if
	// any), with abort true dropping what is queued.
	finish(abort bool) error
}

// pipeline formats the order stage's batches on Formatters goroutines and
// writes them in the order they came, one stream: the drainer hands each
// batch on (send) to the formatters and, in the same order, to the
// sequencer, which waits for each batch's chunk and writes it. A batch's
// row bytes are charged to Config.Account from send until its chunk is
// written or dropped, so rows waiting here count against the order
// stage's budget as rows in its own storage do; the queue is also bounded
// in batches (queueDepth per formatter), and with it the chunks held.
type pipeline[P any] struct {
	k      *Sink[P]
	s      *schema.Schema[P]
	work   chan *chunk[P] // to the formatters
	order  chan *chunk[P] // to the sequencer, in send order
	wg     sync.WaitGroup
	done   chan struct{}         // closed when the sequencer returns
	abort  atomic.Bool           // drop queued batches instead of writing them
	err    atomic.Pointer[error] // the sequencer's first error: send refuses after it
	pool   sync.Pool             // *[]byte chunk buffers
	closed bool                  // finish ran (the drainer's goroutine)
}

// chunk is one batch and, once formatted, its lines.
type chunk[P any] struct {
	segs  [][]schema.Row[P]
	bytes int64 // charged to Account
	rows  int
	buf   *[]byte
	ready chan struct{}
}

// queueDepth is the batches queued per formatter beyond the one it
// formats: one keeps every formatter fed while the sequencer writes.
const queueDepth = 1

func startPipeline[P any](k *Sink[P], s *schema.Schema[P]) *pipeline[P] {
	n := k.o.cfg.Formatters
	p := &pipeline[P]{k: k, s: s, work: make(chan *chunk[P], n*queueDepth), order: make(chan *chunk[P], n*(1+queueDepth)),
		done: make(chan struct{})}
	p.wg.Add(n)
	for range n {
		go p.format()
	}
	go p.sequence()
	return p
}

func (p *pipeline[P]) account(d int64) {
	if a := p.k.o.cfg.Account; a != nil && d != 0 {
		a(d)
	}
}

// send hands segs on; it waits while the queue is full.
func (p *pipeline[P]) send(ctx context.Context, segs [][]schema.Row[P]) error {
	if err := p.loadErr(); err != nil {
		return err
	}
	c := &chunk[P]{segs: segs, ready: make(chan struct{})}
	for _, seg := range segs {
		c.rows += len(seg)
	}
	if p.k.o.cfg.Account != nil {
		for _, seg := range segs {
			c.bytes += p.s.RowsBytes(seg)
		}
		p.account(c.bytes)
	}
	select {
	case p.order <- c:
	case <-ctx.Done():
		p.account(-c.bytes)
		return context.Cause(ctx)
	}
	p.work <- c // never blocks long: order's capacity exceeds work's by the formatters
	return nil
}

// format formats queued batches into chunks of their own.
func (p *pipeline[P]) format() {
	defer p.wg.Done()
	for c := range p.work {
		if !p.abort.Load() {
			b, _ := p.pool.Get().(*[]byte)
			if b == nil {
				b = new([]byte)
			}
			buf := (*b)[:0]
			for _, seg := range c.segs {
				for i := range seg {
					buf = p.k.appendRow(buf, &seg[i])
				}
			}
			*b, c.buf = buf, b
		}
		c.segs = nil // the rows are no longer needed
		close(c.ready)
	}
}

// sequence writes the chunks in send order; after an error or an abort it
// drops the rest, releasing their bytes.
func (p *pipeline[P]) sequence() {
	defer close(p.done)
	o := p.k.o
	for c := range p.order {
		<-c.ready
		if p.loadErr() == nil && !p.abort.Load() && c.buf != nil {
			if _, err := o.bw.Write(*c.buf); err != nil {
				p.fail(err)
			} else {
				o.rows += int64(c.rows)
				if err := o.endBatch(); err != nil {
					p.fail(err)
				}
			}
		}
		if c.buf != nil {
			p.pool.Put(c.buf)
		}
		p.account(-c.bytes)
	}
}

func (p *pipeline[P]) fail(err error) { p.err.CompareAndSwap(nil, &err) }

func (p *pipeline[P]) loadErr() error {
	if e := p.err.Load(); e != nil {
		return *e
	}
	return nil
}

func (p *pipeline[P]) finish(abort bool) error {
	if p.closed {
		<-p.done
		return p.loadErr()
	}
	p.closed = true
	if abort {
		p.abort.Store(true)
	}
	close(p.work)
	close(p.order)
	p.wg.Wait()
	<-p.done
	return p.loadErr()
}
