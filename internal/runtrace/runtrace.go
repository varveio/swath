// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package runtrace writes a run trace (swath diag list --trace): JSON
// Lines, one event an object, each with its event name and ts_ns, the
// nanoseconds since the trace was opened (docs/internals/trace.md is the
// schema). The range engine writes the events (keyrange); the command
// opens the file and closes the trace.
//
// An event never waits on the file: it is encoded on the caller's
// goroutine and handed through a bounded buffer to the writer's goroutine,
// and when the buffer is full it is dropped and counted (Dropped, the
// run_end event, the engine's trace.dropped counter). A slow disk can
// lose events; it cannot slow the listing.
package runtrace

import (
	"bufio"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Version is the trace schema's version, run_start's trace_version.
const Version = 1

// Buffer is how many encoded events wait for the writer before one is
// dropped: at most a few MiB with keys of S3's 1,024-byte limit.
const Buffer = 4096

// Writer is an open trace.
type Writer struct {
	start   time.Time
	out     io.Writer
	lines   chan []byte // a nil line ends the writer
	done    chan struct{}
	closing atomic.Bool
	sent    atomic.Int64
	dropped atomic.Int64
	err     error // the first write error: the writer goroutine's until done is closed
}

// New starts a trace written to out; ts_ns counts from now.
func New(out io.Writer) *Writer {
	w := &Writer{start: time.Now(), out: out, lines: make(chan []byte, Buffer), done: make(chan struct{})}
	go w.run()
	return w
}

// run writes the lines as they come, flushing whenever none waits, so a
// killed process leaves the trace up to its last quiet moment (and at most
// a torn last line).
func (w *Writer) run() {
	defer close(w.done)
	b := bufio.NewWriterSize(w.out, 64<<10)
	for line := range w.lines {
		if line == nil {
			break
		}
		if w.err == nil {
			_, w.err = b.Write(line)
		}
		if len(w.lines) == 0 && w.err == nil {
			w.err = b.Flush()
		}
	}
	if w.err == nil {
		w.err = b.Flush()
	}
}

// Start is when the trace was opened, ts_ns 0.
func (w *Writer) Start() time.Time { return w.start }

// Sent is the events handed to the writer; Dropped those dropped because
// the buffer was full (or the trace closing).
func (w *Writer) Sent() int64    { return w.sent.Load() }
func (w *Writer) Dropped() int64 { return w.dropped.Load() }

// Close writes the events still buffered and the run_end event with the
// run's status, the events written and those dropped, and returns the
// first write error. Events emitted after it are dropped; a second Close
// does nothing.
func (w *Writer) Close(status string) error {
	if !w.closing.CompareAndSwap(false, true) {
		return nil
	}
	w.lines <- nil
	<-w.done
	if w.err != nil {
		return w.err
	}
	end := w.Event("run_end").Str("status", status).Int("events", w.Sent()).Int("dropped", w.Dropped())
	_, err := w.out.Write(append(end.b, '}', '\n'))
	return err
}

// Event is one event being encoded; Emit hands it to the writer.
type Event struct {
	w *Writer
	b []byte
}

// Event begins event name, timestamped now.
func (w *Writer) Event(name string) Event {
	b := make([]byte, 0, 256)
	b = append(b, `{"event":`...)
	b = appendText(b, name)
	b = append(b, `,"ts_ns":`...)
	b = strconv.AppendInt(b, int64(time.Since(w.start)), 10)
	return Event{w: w, b: b}
}

func (e Event) field(name string) []byte {
	b := append(e.b, ',')
	b = appendText(b, name)
	return append(b, ':')
}

// Int adds an integer field.
func (e Event) Int(name string, v int64) Event {
	e.b = strconv.AppendInt(e.field(name), v, 10)
	return e
}

// Bool adds a boolean field.
func (e Event) Bool(name string, v bool) Event {
	e.b = strconv.AppendBool(e.field(name), v)
	return e
}

// Str adds a text field (an invalid UTF-8 sequence is replaced by U+FFFD).
func (e Event) Str(name, v string) Event {
	e.b = appendText(e.field(name), strings.ToValidUTF8(v, "�"))
	return e
}

// Key adds a key or bound: a JSON string when it is valid UTF-8, else
// {"hex": its bytes in hexadecimal}, so every byte survives.
func (e Event) Key(name, k string) Event {
	b := e.field(name)
	if utf8.ValidString(k) {
		e.b = appendText(b, k)
		return e
	}
	b = append(b, `{"hex":"`...)
	b = hex.AppendEncode(b, []byte(k))
	e.b = append(b, `"}`...)
	return e
}

// Emit hands the event to the writer, or drops it when the buffer is
// full: it never waits.
func (e Event) Emit() {
	w := e.w
	if w.closing.Load() {
		w.dropped.Add(1)
		return
	}
	select {
	case w.lines <- append(e.b, '}', '\n'):
		w.sent.Add(1)
	default:
		w.dropped.Add(1)
	}
}

// appendText appends s, valid UTF-8, as a JSON string.
func appendText(b []byte, s string) []byte {
	const hexDigits = "0123456789abcdef"
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		b = append(b, s[start:i]...)
		switch c {
		case '"', '\\':
			b = append(b, '\\', c)
		default:
			b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = i + 1
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}
