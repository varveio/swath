// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/source/azblob/client"
)

// The lead bytes that bound a scalar group under a parent Q: Q+leadE starts
// E(Q) (U+E000..U+FFFF), Q+leadS starts S(Q) (U+10000 and above), and
// Q+leadBeyond is above every name under Q. Strings ending in one of them
// are only compared, never sent.
const (
	leadE      = 0xEE
	leadS      = 0xF0
	leadBeyond = 0xF5
)

// rank is a byte's position in Azure's order (UTF-16 code units for valid
// UTF-8): 00..ED, F0..F4, EE..EF, F5..FF (replay/azure.rank).
func rank(v byte) int {
	switch {
	case v >= 0xf0 && v <= 0xf4:
		return int(v) - 2
	case v == 0xee || v == 0xef:
		return int(v) + 5
	}
	return int(v)
}

// pcmp compares two names in Azure's order.
func pcmp(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			if rank(a[i]) < rank(b[i]) {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// cut is q followed by the single byte v.
func cut(q string, v byte) string { return q + string([]byte{v}) }

// grouped reports whether v is a lead byte of a scalar at U+E000 and
// above, where the two orders can disagree.
func grouped(v byte) bool { return v >= leadE && v <= 0xF4 }

// seekFrom is the name a span whose lower bound is lo is entered at, its
// P-minimum (§3.1): lo itself when lo is a name, Q+U+E000 for Q+leadE and
// Q+U+10000 for Q+leadS.
func seekFrom(lo string) string {
	if n := len(lo); n > 0 {
		switch lo[n-1] {
		case leadE:
			return lo[:n-1] + ""
		case leadS:
			return lo[:n-1] + "\U00010000"
		}
	}
	return lo
}

// succ is the subtree successor of q (§3.1): q with its last scalar
// incremented, skipping the surrogates, a last U+10FFFF dropped and the one
// before incremented instead. ok is false when no successor lies inside the
// prefix window (the scalars left to increment are the prefix's).
func succ(q string, floor int) (string, bool) {
	for len(q) > floor {
		r, size := utf8.DecodeLastRuneInString(q)
		head := q[:len(q)-size]
		if len(head) < floor {
			return "", false
		}
		if r < utf8.MaxRune {
			if r++; r == 0xD800 {
				r = 0xE000
			}
			return head + string(r), true
		}
		q = head
	}
	return "", false
}

type state uint8

const (
	unread    state = iota // nothing read yet: the next read seeks seekFrom(lo)
	reading                // sk.marker continues the span's read
	exhausted              // no read left: the buffer is all that remains
)

// seek is a read in progress: the name it started at, the startFrom it was
// sent with (a marker is resent only with it), and the §4.1 state that
// spans its continuation chain.
type seek struct {
	from     string
	wire     string
	excl     bool // drop an item equal to from (rule 1)
	marker   string
	accepted bool // a member was accepted (rule 2 no longer applies)
}

// span is a byte interval of the scope (§3): the names n under the prefix
// with lo < n (lo <= n when loInc) and n < hi; hi "" is the end of the
// prefix window. Its member set is also an interval in Azure's order.
type span struct {
	id       int
	lo       string
	loInc    bool
	hi       string
	buf      []client.Item // members read, not emitted, in Azure's order
	st       state
	sk       seek
	frontier string // the last item consumed for this span, member or not
	// rest: for the E child of a split, its rest siblings, which may take
	// over the E read's stream (overflow transfer, §4.3).
	rest []int
}

// contains reports whether name n is in s's interval; prefix is the
// query's.
func (s *span) contains(n, prefix string) bool {
	return strings.HasPrefix(n, prefix) && (n > s.lo || s.loInc && n == s.lo) && (s.hi == "" || n < s.hi)
}

// empty reports whether s's interval can hold no name.
func (s *span) empty() bool {
	if s.hi == "" {
		return false
	}
	return s.lo >= s.hi || !s.loInc && s.hi == s.lo+"\x00"
}

// clip is [lo, hi) intersected with s, the bounds only.
func (s *span) clip(lo, hi string) span {
	c := span{lo: lo, loInc: true, hi: hi}
	if s.lo > lo || s.lo == lo && !s.loInc {
		c.lo, c.loInc = s.lo, s.loInc
	}
	if s.hi != "" && (hi == "" || s.hi < hi) {
		c.hi = s.hi
	}
	return c
}

// scalarCuts appends the cut points of name x at its scalar boundaries
// from floor whose lead byte is in EE..F4: Q+leadE, Q+leadS and
// Q+leadBeyond for each such Q = x[:i] (§3.1).
func scalarCuts(dst []string, x string, floor int) []string {
	for i := floor; i < len(x); {
		_, size := utf8.DecodeRuneInString(x[i:])
		if grouped(x[i]) {
			q := x[:i]
			dst = append(dst, cut(q, leadE), cut(q, leadS), cut(q, leadBeyond))
		}
		i += size
	}
	return dst
}

// pieces splits [lo, hi) at the cuts strictly inside it and rewrites each
// piece that starts beyond a subtree (§3.1's successor rewrite and re-cut);
// the result is in byte order, empty pieces dropped. Only bounds are set.
func pieces(lo string, loInc bool, hi string, cuts []string, floor int) []span {
	cuts = slices.DeleteFunc(slices.Clone(cuts), func(c string) bool { return c <= lo || hi != "" && c >= hi })
	slices.Sort(cuts)
	cuts = slices.Compact(cuts)
	var out []span
	for k := 0; k <= len(cuts); k++ {
		s := span{lo: lo, loInc: loInc, hi: hi}
		if k > 0 {
			s.lo, s.loInc = cuts[k-1], true
		}
		if k < len(cuts) {
			s.hi = cuts[k]
		}
		if n := len(s.lo); n > 0 && s.lo[n-1] == leadBeyond {
			out = append(out, rewrite(s, floor)...)
		} else if !s.empty() {
			out = append(out, s)
		}
	}
	return out
}

// rewrite replaces s's lower bound Q+leadBeyond with Q's subtree successor
// and, when the successor's last scalar lies in E or S, re-cuts it under
// the successor's parent.
func rewrite(s span, floor int) []span {
	n, ok := succ(s.lo[:len(s.lo)-1], floor)
	if !ok || s.hi != "" && n >= s.hi {
		return nil
	}
	if r, size := utf8.DecodeLastRuneInString(n); r >= 0xE000 {
		q := n[:len(n)-size]
		return pieces(n, true, s.hi, []string{cut(q, leadE), cut(q, leadS), cut(q, leadBeyond)}, floor)
	}
	return []span{{lo: n, loInc: true, hi: s.hi}}
}

// bounds are a query's scope normalised to the prefix window (§3.0).
type bounds struct {
	prefix string
	lo     string
	loInc  bool
	hi     string // "" : the end of the prefix window
	empty  bool
}

func normalise(prefix, after, through string, bounded bool) bounds {
	b := bounds{prefix: prefix, lo: after}
	switch {
	case after < prefix:
		b.lo, b.loInc = prefix, true
	case !strings.HasPrefix(after, prefix):
		b.empty = true
	}
	if bounded {
		switch {
		case through < prefix:
			b.empty = true
		case strings.HasPrefix(through, prefix):
			b.hi = through + "\x00"
		}
	}
	return b
}

// decompose is the spans of a query's scope, in byte order (§3.1).
func decompose(b bounds, through string) []span {
	if b.empty {
		return nil
	}
	var cuts []string
	if !b.loInc {
		cuts = scalarCuts(cuts, b.lo, len(b.prefix))
	}
	if b.hi != "" {
		cuts = scalarCuts(cuts, through, len(b.prefix))
	}
	return pieces(b.lo, b.loInc, b.hi, cuts, len(b.prefix))
}
