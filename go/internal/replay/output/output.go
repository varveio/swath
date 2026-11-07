// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package output is the replay server's response memory: a byte budget
// shared by every response in flight, and the chunked sink a response is
// rendered into, which charges the budget before it allocates and stops at
// the first byte that would exceed either the per-response cap or the
// budget.
//
// The accounting is the replay wire contract's, because it decides which
// request is refused when memory is short: a response reserves an estimate
// before its page is read (New), its first chunk is sized from the page it
// actually got (SetFirstChunkHint), chunks consume the reservation before
// charging more, the unused reservation goes back when rendering ends
// (Body), before any injected delay, and only the chunks backing the
// response stay charged until it is written (Close).
package output

import (
	"math"
	"strconv"
	"sync/atomic"
)

// ChunkBytes is the size of every chunk after the first.
const ChunkBytes = 256 << 10

// Reason is why a chunk was allocated, as the chunk meters tag it.
type Reason int

// The chunk allocation reasons.
const (
	// Initial is a response's first chunk.
	Initial Reason = iota
	// InitialCapPartial is a first chunk cut short by the per-response cap.
	InitialCapPartial
	// Continuation is any later chunk.
	Continuation
	// CapPartial is a later chunk cut short by the per-response cap.
	CapPartial
)

// Reasons lists every reason, in meter order.
var Reasons = [...]Reason{Initial, InitialCapPartial, Continuation, CapPartial}

// String is the reason's tag value.
func (r Reason) String() string {
	switch r {
	case InitialCapPartial:
		return "initial_cap_partial"
	case Continuation:
		return "continuation"
	case CapPartial:
		return "cap_partial"
	}
	return "initial"
}

// Error is a response that could not be rendered within its bounds.
type Error struct {
	// TooLarge is the per-response cap; otherwise the shared budget (or
	// the response count) refused.
	TooLarge bool
	// Reason is the x-swath-replay-error value.
	Reason string
}

func (e *Error) Error() string {
	if e.TooLarge {
		return "response exceeded configured per-response cap"
	}
	return "response buffer budget exhausted"
}

// The refusals.
var (
	ErrTooLarge = &Error{TooLarge: true, Reason: "response_too_large"}
	ErrBudget   = &Error{Reason: "response_budget_exhausted"}
	ErrCount    = &Error{Reason: "response_count_exhausted"}
)

// Budget is the shared byte budget: charges never block, they succeed or
// are refused.
type Budget struct {
	limit         int64
	charged, peak atomic.Int64
	onChunk       func(Reason, int)
}

// NewBudget returns a budget of limit bytes; onChunk, when set, observes
// every chunk allocated against it.
func NewBudget(limit int64, onChunk func(Reason, int)) *Budget {
	if limit <= 0 {
		panic("output: response buffer budget must be positive")
	}
	return &Budget{limit: limit, onChunk: onChunk}
}

// TryCharge charges n bytes, or reports false and charges nothing.
func (b *Budget) TryCharge(n int64) bool {
	for {
		before := b.charged.Load()
		if n < 0 || n > b.limit-before {
			return false
		}
		if b.charged.CompareAndSwap(before, before+n) {
			for {
				p := b.peak.Load()
				if before+n <= p || b.peak.CompareAndSwap(p, before+n) {
					return true
				}
			}
		}
	}
}

// Release returns n charged bytes.
func (b *Budget) Release(n int64) {
	if b.charged.Add(-n) < 0 {
		panic("output: response byte budget released twice")
	}
}

// Charged is the bytes charged now; Peak the most ever charged at once.
func (b *Budget) Charged() int64 { return b.charged.Load() }
func (b *Budget) Peak() int64    { return b.peak.Load() }

// Limit is the budget's size.
func (b *Budget) Limit() int64 { return b.limit }

// Output is one response's sink. Appends after a refusal do nothing; Err
// reports the refusal. Using an Output after Body or Close is a
// programming error and panics.
type Output struct {
	budget     *Budget
	cap        int
	chunkBytes int
	alloc      func(int) []byte

	chunks    [][]byte // each chunk's len is its bytes used, its cap its size
	reserved  int      // reservation not yet consumed by a chunk
	allocated int      // the sizes of the chunks
	hint      int
	length    int
	err       error
	frozen    bool
	closed    bool

	onePass, fallback int
	scratch           [20]byte
}

// New reserves initial bytes of budget for a response of at most cap bytes,
// or returns ErrBudget with nothing charged.
func New(b *Budget, initial, cap int) (*Output, error) {
	return NewChunked(b, initial, cap, ChunkBytes, nil)
}

// NewChunked is New with another chunk size and, when alloc is set, its own
// chunk allocator (tests use both to reach chunk edges and count
// allocations).
func NewChunked(b *Budget, initial, cap, chunkBytes int, alloc func(int) []byte) (*Output, error) {
	if cap <= 0 || initial <= 0 || initial > cap || chunkBytes <= 0 {
		panic("output: invalid response output bounds")
	}
	if alloc == nil {
		alloc = func(n int) []byte { return make([]byte, 0, n) }
	}
	if !b.TryCharge(int64(initial)) {
		return nil, ErrBudget
	}
	return &Output{budget: b, cap: cap, chunkBytes: chunkBytes, alloc: alloc, reserved: initial, hint: initial}, nil
}

// Standalone is an Output with no shared budget and no cap, for rendering
// outside the server.
func Standalone(initial int) *Output {
	o, _ := NewChunked(NewBudget(math.MaxInt64, nil), initial, math.MaxInt32, ChunkBytes, nil)
	return o
}

// SetFirstChunkHint sizes the first chunk from what will actually be
// rendered (at least 4 KiB, at most ChunkBytes); it must come before the
// first append. A non-positive hint is ignored.
func (o *Output) SetFirstChunkHint(hint int) {
	if hint <= 0 {
		return
	}
	if o.closed || o.frozen || len(o.chunks) > 0 {
		panic("output: first chunk hint must precede rendering")
	}
	o.hint = hint
}

// Err is the refusal that stopped rendering: ErrTooLarge or ErrBudget.
func (o *Output) Err() error { return o.err }

// Len is the bytes written; Capacity the sizes of the chunks allocated.
func (o *Output) Len() int      { return o.length }
func (o *Output) Capacity() int { return o.allocated }

// Charged is what this output holds of the budget: its chunks and any
// unconsumed reservation.
func (o *Output) Charged() int { return o.allocated + o.reserved }

// PercentPaths counts PercentEncoded values written in one pass into an
// already charged chunk, and those that took the exact-length path.
func (o *Output) PercentPaths() (onePass, fallback int) { return o.onePass, o.fallback }

// Body freezes the response and returns its bytes as views of its chunks,
// returning the unused reservation to the budget: from here until Close
// the output holds only the chunks backing the response.
func (o *Output) Body() [][]byte {
	if o.closed {
		panic("output: response output is closed")
	}
	if !o.frozen {
		o.frozen = true
		if o.reserved > 0 {
			o.budget.Release(int64(o.reserved))
			o.reserved = 0
		}
	}
	return o.chunks
}

// Close returns everything the output holds to the budget. It is
// idempotent.
func (o *Output) Close() {
	if o.closed {
		return
	}
	o.closed = true
	o.chunks = nil
	o.budget.Release(int64(o.allocated + o.reserved))
	o.allocated, o.reserved = 0, 0
}

// ensure admits n more bytes against the cap.
func (o *Output) ensure(n int) bool {
	if o.closed {
		panic("output: response output is closed")
	}
	if o.frozen {
		panic("output: response output is frozen")
	}
	if o.err != nil {
		return false
	}
	if int64(o.length)+int64(n) > int64(o.cap) {
		o.err = ErrTooLarge
		return false
	}
	return true
}

// writable returns the index of a chunk with room, allocating (and
// charging) one when the last is full; -1 when the budget refused.
func (o *Output) writable() int {
	if n := len(o.chunks); n > 0 && len(o.chunks[n-1]) < cap(o.chunks[n-1]) {
		return n - 1
	}
	first := len(o.chunks) == 0
	preferred := o.chunkBytes
	if first {
		preferred = min(o.chunkBytes, max(4096, o.hint))
	}
	size := min(preferred, o.cap-o.allocated)
	if size <= 0 {
		o.err = ErrTooLarge
		return -1
	}
	credit := min(o.reserved, size)
	if extra := size - credit; extra > 0 && !o.budget.TryCharge(int64(extra)) {
		o.err = ErrBudget
		return -1
	}
	chunk := o.alloc(size)
	if cap(chunk) != size || len(chunk) != 0 {
		panic("output: chunk allocator returned unexpected capacity")
	}
	o.chunks = append(o.chunks, chunk)
	o.reserved -= credit
	o.allocated += size
	if o.budget.onChunk != nil {
		reason := Continuation
		switch {
		case first && size < preferred:
			reason = InitialCapPartial
		case first:
			reason = Initial
		case size < preferred:
			reason = CapPartial
		}
		o.budget.onChunk(reason, size)
	}
	return len(o.chunks) - 1
}

// put copies b, already admitted, chunk by chunk.
func (o *Output) put(b []byte) {
	for len(b) > 0 {
		i := o.writable()
		if i < 0 {
			return
		}
		c := o.chunks[i]
		n := min(len(b), cap(c)-len(c))
		o.chunks[i] = append(c, b[:n]...)
		o.length += n
		b = b[n:]
	}
}

// Byte appends one byte.
func (o *Output) Byte(c byte) {
	if !o.ensure(1) {
		return
	}
	if i := o.writable(); i >= 0 {
		o.chunks[i] = append(o.chunks[i], c)
		o.length++
	}
}

// Write appends b.
func (o *Output) Write(b []byte) {
	if o.ensure(len(b)) {
		o.put(b)
	}
}

// ASCII appends s, which callers guarantee is ASCII (tags and fixed text).
func (o *Output) ASCII(s string) {
	if !o.ensure(len(s)) {
		return
	}
	for len(s) > 0 {
		i := o.writable()
		if i < 0 {
			return
		}
		c := o.chunks[i]
		n := min(len(s), cap(c)-len(c))
		o.chunks[i] = append(c, s[:n]...)
		o.length += n
		s = s[n:]
	}
}

// Int appends v in decimal.
func (o *Output) Int(v int64) {
	o.Write(strconv.AppendInt(o.scratch[:0], v, 10))
}

// ThreeDigits appends v (0..999) as three digits.
func (o *Output) ThreeDigits(v int) {
	o.Byte(byte('0' + v/100))
	o.Byte(byte('0' + v/10%10))
	o.Byte(byte('0' + v%10))
}

// Escaped appends text with the five XML metacharacters escaped (an apostrophe as
// &apos;) and every other byte as it is. Text without a metacharacter or a
// non-ASCII byte is admitted whole; otherwise each entity and each byte is
// admitted on its own, so a refusal lands on the same byte as the replay
// wire contract's.
func (o *Output) Escaped(s string) {
	safe := true
	for i := 0; i < len(s) && safe; i++ {
		switch c := s[i]; {
		case c >= 0x80, c == '&', c == '<', c == '>', c == '"', c == '\'':
			safe = false
		}
	}
	if safe {
		o.ASCII(s)
		return
	}
	for i := 0; i < len(s) && o.err == nil; i++ {
		switch c := s[i]; c {
		case '&':
			o.ASCII("&amp;")
		case '<':
			o.ASCII("&lt;")
		case '>':
			o.ASCII("&gt;")
		case '"':
			o.ASCII("&quot;")
		case '\'':
			o.ASCII("&apos;")
		default:
			o.Byte(c)
		}
	}
}

const hexDigits = "0123456789ABCDEF"

// urlSafe are the bytes encoding-type=url leaves as they are.
var urlSafe = func() (t [256]bool) {
	for c := 'A'; c <= 'Z'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	t['-'], t['_'], t['.'], t['/'] = true, true, true, true
	return t
}()

// PercentEncoded appends v percent-encoded (every byte but A-Z a-z 0-9
// - _ . / as %XX, upper-case hex). When three bytes per input byte fit in
// the room left in the current chunk it writes in one pass, with nothing
// to admit or allocate; otherwise it admits the exact encoded length and
// lets an escape straddle a chunk edge.
func (o *Output) PercentEncoded(v []byte) {
	if o.closed {
		panic("output: response output is closed")
	}
	if o.frozen {
		panic("output: response output is frozen")
	}
	if o.err != nil {
		return
	}
	if n := len(o.chunks); n > 0 {
		last := o.chunks[n-1]
		if len(v) <= (cap(last)-len(last))/3 {
			before := len(last)
			for _, b := range v {
				if urlSafe[b] {
					last = append(last, b)
				} else {
					last = append(last, '%', hexDigits[b>>4], hexDigits[b&0x0f])
				}
			}
			o.chunks[n-1] = last
			o.length += len(last) - before
			o.onePass++
			return
		}
	}
	encoded := len(v)
	for _, b := range v {
		if !urlSafe[b] {
			encoded += 2
		}
	}
	if !o.ensure(encoded) {
		return
	}
	for _, b := range v {
		if urlSafe[b] {
			o.put([]byte{b})
		} else {
			o.put([]byte{'%', hexDigits[b>>4], hexDigits[b&0x0f]})
		}
		if o.err != nil {
			return
		}
	}
	o.fallback++
}
