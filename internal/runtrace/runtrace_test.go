// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package runtrace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// lines decodes a trace, each line a JSON object.
func lines(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

// TestEncoding: every field kind round-trips through a JSON decoder, a
// key that is not UTF-8 keeps its bytes as hex, and text that is not is
// repaired rather than written invalid.
func TestEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := New(&buf)
	w.Event("e").Int("n", -7).Bool("b", true).Key("k", "a\"b\\c\x01\n/é").Key("raw", "a\xffb").Str("s", "x\xfey").Emit()
	if err := w.Close(time.Second, "ok", ""); err != nil {
		t.Fatal(err)
	}
	got := lines(t, buf.Bytes())
	if len(got) != 2 {
		t.Fatalf("%d lines, want the event and run_end:\n%s", len(got), buf.String())
	}
	e := got[0]
	if e["event"] != "e" || e["n"] != -7.0 || e["b"] != true || e["k"] != "a\"b\\c\x01\n/é" || e["s"] != "x�y" {
		t.Fatalf("event decoded as %v", e)
	}
	if raw, ok := e["raw"].(map[string]any); !ok || raw["hex"] != "61ff62" {
		t.Fatalf("a key that is not UTF-8: %v, want {\"hex\":\"61ff62\"}", e["raw"])
	}
	if _, ok := e["ts_ns"].(float64); !ok {
		t.Fatalf("no ts_ns: %v", e)
	}
	end := got[1]
	if end["event"] != "run_end" || end["status"] != "ok" || end["events"] != 1.0 || end["dropped"] != 0.0 {
		t.Fatalf("run_end %v", end)
	}
	if err := w.Close(time.Second, "again", ""); err != nil || strings.Count(buf.String(), "run_end") != 1 {
		t.Fatalf("a second Close wrote again or failed: %v", err)
	}
}

// gate is a writer that blocks until opened.
type gate struct {
	open chan struct{}
	buf  bytes.Buffer
}

func (g *gate) Write(b []byte) (int, error) {
	<-g.open
	return g.buf.Write(b)
}

// TestDropsWhenFull: with the file stalled, Emit never waits; the events
// past the buffer are dropped and counted, and run_end accounts for every
// event emitted (written + dropped).
func TestDropsWhenFull(t *testing.T) {
	g := &gate{open: make(chan struct{})}
	w := New(g)
	const n = 3 * Buffer
	for i := range n {
		w.Event("e").Int("i", int64(i)).Emit() // would block forever if Emit waited
	}
	if w.Dropped() == 0 || w.Sent()+w.Dropped() != n {
		t.Fatalf("sent %d dropped %d, want drops and %d in all", w.Sent(), w.Dropped(), n)
	}
	close(g.open)
	if err := w.Close(time.Second, "ok", ""); err != nil {
		t.Fatal(err)
	}
	got := lines(t, g.buf.Bytes())
	end := got[len(got)-1]
	if int64(len(got)-1) != w.Sent() || end["events"] != float64(w.Sent()) || end["dropped"] != float64(w.Dropped()) {
		t.Fatalf("%d events written, run_end %v; sent %d dropped %d", len(got)-1, end, w.Sent(), w.Dropped())
	}
	w.Event("late").Emit()
	if w.Dropped() != n-w.Sent()+1 {
		t.Fatalf("an event after Close was not dropped")
	}
}

// failing is a writer that refuses every write.
type failing struct{}

func (failing) Write([]byte) (int, error) { return 0, io.ErrShortWrite }

// TestWriteError: Close returns the file's error.
func TestWriteError(t *testing.T) {
	w := New(failing{})
	w.Event("e").Emit()
	if err := w.Close(time.Second, "ok", ""); err == nil {
		t.Fatal("Close succeeded on a failing file")
	}
}

// TestCloseAbandonsStalledFile: a file that never takes a write (a pipe
// nobody reads) holds up Close only for its wait. Close then abandons the
// writer, counts the events not written as dropped and says so; events
// after it are dropped too.
func TestCloseAbandonsStalledFile(t *testing.T) {
	g := &gate{open: make(chan struct{})}
	t.Cleanup(func() { close(g.open) }) // let the abandoned writer finish
	w := New(g)
	const n = 10
	for i := range n {
		w.Event("e").Int("i", int64(i)).Emit()
	}
	start := time.Now()
	err := w.Close(100*time.Millisecond, "committed", "completed")
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Close took %s on a stalled file", took)
	}
	var st *Stalled
	if !errors.As(err, &st) || st.Unwritten != n || w.Dropped() != n {
		t.Fatalf("Close: %v, dropped %d; want %d abandoned", err, w.Dropped(), n)
	}
	if !strings.Contains(err.Error(), "10 events") {
		t.Fatalf("the warning does not count the events: %q", err)
	}
	w.Event("late").Emit()
	if w.Dropped() != n+1 {
		t.Fatalf("an event after Close was not dropped: %d", w.Dropped())
	}
}
