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
// lose events; it cannot slow the listing. Nor can it hold up the run's
// end: Close waits for the file a bounded time, then abandons it.
package runtrace

import (
	"bufio"
	"encoding/hex"
	"fmt"
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
	flushed atomic.Int64 // lines the file has taken (flushed), run_end included
	err     error        // the first write error: the writer goroutine's until done is closed
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
	var pending int64 // lines in b, not yet flushed
	flush := func() {
		if w.err = b.Flush(); w.err == nil {
			w.flushed.Add(pending)
			pending = 0
		}
	}
	for line := range w.lines {
		if line == nil {
			break
		}
		if w.err == nil {
			if _, w.err = b.Write(line); w.err == nil {
				pending++
			}
		}
		if len(w.lines) == 0 && w.err == nil {
			flush()
		}
	}
	if w.err == nil {
		flush()
	}
}

// Start is when the trace was opened, ts_ns 0.
func (w *Writer) Start() time.Time { return w.start }

// Sent is the events handed to the writer; Dropped those dropped because
// the buffer was full (or the trace closing).
func (w *Writer) Sent() int64    { return w.sent.Load() }
func (w *Writer) Dropped() int64 { return w.dropped.Load() }

// Stalled is Close's error when the file took nothing for its whole
// wait: the writer was abandoned, and the events it had not written
// (Unwritten, now counted in Dropped) and run_end are not in the trace.
type Stalled struct {
	Wait      time.Duration
	Unwritten int64
}

func (s *Stalled) Error() string {
	return fmt.Sprintf("the file took no write for %s: abandoned, with %d events and run_end not written", s.Wait, s.Unwritten)
}

// Close hands the writer the run_end event (the run's status and outcome
// class, "" for none, the events handed to the writer and those dropped)
// and waits at most wait for it to write what it holds, so a file that
// stops taking writes (a pipe nobody reads, a hung mount) cannot hold up
// the run's end. Past wait it abandons the writer, counts the events not
// written as dropped and returns a *Stalled; the caller must not wait on
// the file either (closing it may block where a write is stuck). It
// returns the first write error otherwise. Events emitted after Close are
// dropped; a second Close does nothing.
func (w *Writer) Close(wait time.Duration, status, class string) error {
	if !w.closing.CompareAndSwap(false, true) {
		return nil
	}
	end := w.Event("run_end").Str("status", status)
	if class != "" {
		end = end.Str("class", class)
	}
	end = end.Int("events", w.Sent()).Int("dropped", w.Dropped())
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for _, line := range [][]byte{append(end.b, '}', '\n'), nil} {
		select {
		case w.lines <- line:
		case <-deadline.C:
			return w.abandon(wait)
		}
	}
	select {
	case <-w.done:
		return w.err
	case <-deadline.C:
		return w.abandon(wait)
	}
}

// abandon counts the events the file has not taken as dropped.
func (w *Writer) abandon(wait time.Duration) error {
	n := max(0, w.Sent()-w.flushed.Load())
	w.dropped.Add(n)
	return &Stalled{Wait: wait, Unwritten: n}
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
