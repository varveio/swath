// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package scalefake serves a keyspace over the S3 ListObjectsV2 and GCS
// objects.list wire shapes, for tests, scale runs and long measured runs,
// with optional service behaviours (latency, jitter, throttling, a rate cap,
// slow prefixes, slow continuations, capped pages: Options). The keyspace
// is either generated lazily from a Shape (New: up to billions of keys,
// nothing materialized: key i is computed from the seed and the shape in
// O(depth)) or a given sorted key list (FromKeys, ReadKeys). Keys are
// strictly ascending in i, and every query (prefix, start-after or
// startOffset, endOffset, delimiter, page size, continuation) is answered
// by binary search over i plus O(page) key computations. Test and bench
// support only: nothing in the tool imports it (TestImportBoundary).
package scalefake

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sort"
	"strings"
)

// Shape defines a keyspace. The zero Fanout is flat (every key in one
// directory); otherwise keys are ROOT + SEG_1/ ... SEG_d/ + LEAF with d =
// len(Fanout) directory levels, Fanout[k] children per directory at level
// k, and Keys spread over the leaf directories by weight.
type Shape struct {
	Keys   int64 // total keys, exactly
	Fanout []int // children per directory, root level first; empty: flat
	// Skew is the sigma of a hierarchical log-normal density: every
	// directory at every level draws a factor exp(Skew*z), z standard
	// normal from the seed, and a leaf directory's weight is the product
	// along its path, so whole subtrees are dense or sparse. 0: uniform.
	Skew float64
	// SharedPrefix is the length of a root prefix (ending in "/") every
	// key shares; 0: none.
	SharedPrefix int
	// SegmentPad is the length of a fixed run of bytes at the start of
	// every directory segment (the same at each level), so siblings share
	// long prefixes.
	SegmentPad int
	// KeyLen stretches leaf names (with a stem fixed per directory, before
	// the varying digits: an 8-byte head drawn per directory, then one
	// shared run) so every key is at least this long; 0: natural.
	KeyLen int
	Seed   uint64
}

// stemHead is the bytes of a stretched leaf name's stem drawn per
// directory; the rest of the stem is one run shared by every directory.
const stemHead = 8

// MaxDirs bounds the number of leaf directories (one int64 each).
const MaxDirs = 1 << 22

const digits36 = "0123456789abcdefghijklmnopqrstuvwxyz"

// Keyspace is a Shape made ready to answer queries. It is immutable and
// safe for concurrent use.
type Keyspace struct {
	shape  Shape
	root   string
	levels []level
	cum    []int64 // cum[j]: keys in leaf directories before j; len dirs+1
	leafW  int     // base-36 digits of a leaf slot
	slots  uint64  // 36^leafW
	stemN  int     // bytes of the per-directory stem in a leaf name
	stem   string  // the stem's shared tail (all but its per-directory head)
	maxLen int
	// A given key list (FromKeys) instead of a generated shape: key i is
	// list[offs[i] : offs[i+1]-1], each key followed by a newline.
	list string
	offs []int
}

type level struct {
	radix int
	width int // base-36 digits
	pad   string
}

// New prepares s, in O(leaf directories) time and memory.
func New(s Shape) (*Keyspace, error) {
	if s.Keys < 0 {
		return nil, errors.New("scalefake: negative key count")
	}
	k := &Keyspace{shape: s}
	dirs := int64(1)
	for i, r := range s.Fanout {
		if r < 1 {
			return nil, fmt.Errorf("scalefake: fanout %d at level %d", r, i)
		}
		dirs *= int64(r)
		if dirs > MaxDirs {
			return nil, fmt.Errorf("scalefake: more than %d leaf directories", MaxDirs)
		}
		k.levels = append(k.levels, level{radix: r, width: width36(uint64(r - 1)),
			pad: filler(s.Seed, 0x5e6+uint64(i), s.SegmentPad)})
	}
	if s.SharedPrefix > 0 {
		k.root = filler(s.Seed, 0x7007, s.SharedPrefix-1) + "/"
	}
	// Weights: the product of per-directory factors along each path.
	w := make([]float64, dirs)
	for j := range w {
		w[j] = 1
	}
	if s.Skew != 0 && len(s.Fanout) > 0 {
		span := dirs // leaf directories under one directory at the level
		for lv, r := range s.Fanout {
			span /= int64(r)
			for j := range w {
				node := uint64(j) / uint64(span) // index of the level-lv ancestor
				w[j] *= math.Exp(s.Skew * normal(s.Seed, uint64(lv)<<40|node))
			}
		}
	}
	total := 0.0
	for _, x := range w {
		total += x
	}
	k.cum = make([]int64, dirs+1)
	acc, most := 0.0, int64(0)
	for j, x := range w {
		acc += x
		c := int64(math.Round(float64(s.Keys) * (acc / total)))
		c = min(max(c, k.cum[j]), s.Keys)
		if j == len(w)-1 {
			c = s.Keys
		}
		k.cum[j+1] = c
		most = max(most, c-k.cum[j])
	}
	// Twice the slots the densest directory needs, so keys have gaps.
	k.leafW = max(1, width36(uint64(2*most)))
	if k.leafW > 12 {
		return nil, errors.New("scalefake: too many keys per directory")
	}
	k.slots = 1
	for range k.leafW {
		k.slots *= 36
	}
	natural := len(k.root) + k.leafW
	for _, l := range k.levels {
		natural += len(l.pad) + l.width + 1
	}
	k.stemN = max(0, s.KeyLen-natural)
	k.stem = filler(s.Seed, 0x57e3, max(0, k.stemN-stemHead))
	k.maxLen = natural + k.stemN
	return k, nil
}

// FromKeys serves keys, which must be strictly ascending as bytes and
// hold no newline. Metadata is synthetic, from the key's index (Meta).
func FromKeys(keys []string) (*Keyspace, error) {
	var b strings.Builder
	n := 0
	for _, key := range keys {
		n += len(key) + 1
	}
	b.Grow(n)
	for _, key := range keys {
		if strings.IndexByte(key, '\n') >= 0 {
			return nil, fmt.Errorf("scalefake: key %q holds a newline", key)
		}
		b.WriteString(key)
		b.WriteByte('\n')
	}
	return fromList(b.String())
}

// ReadKeys serves the key file r holds: one key per line (newline
// terminated, the last newline optional), strictly ascending as bytes.
func ReadKeys(r io.Reader) (*Keyspace, error) {
	b, err := io.ReadAll(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	if len(b) > 0 && b[len(b)-1] != '\n' {
		b = append(b, '\n')
	}
	return fromList(string(b))
}

// fromList indexes list, keys each followed by a newline.
func fromList(list string) (*Keyspace, error) {
	k := &Keyspace{list: list, offs: make([]int, 0, strings.Count(list, "\n")+1)}
	prev := ""
	for off := 0; off < len(list); {
		e := off + strings.IndexByte(list[off:], '\n')
		key := list[off:e]
		if len(k.offs) > 0 && key <= prev {
			return nil, fmt.Errorf("scalefake: key %d %q is not above the key before it %q", len(k.offs), key, prev)
		}
		k.offs = append(k.offs, off)
		k.maxLen = max(k.maxLen, len(key))
		prev, off = key, e+1
	}
	k.shape.Keys = int64(len(k.offs))
	k.offs = append(k.offs, len(list))
	return k, nil
}

// listed is key i of a given list.
func (k *Keyspace) listed(i int64) string { return k.list[k.offs[i] : k.offs[i+1]-1] }

// String describes the keyspace for logs and reports.
func (k *Keyspace) String() string {
	if k.offs != nil {
		return fmt.Sprintf("keys=%d list", k.shape.Keys)
	}
	return k.shape.String()
}

// Len is the number of keys.
func (k *Keyspace) Len() int64 { return k.shape.Keys }

// Shape is the shape k was built from (a given list's: its key count).
func (k *Keyspace) Shape() Shape { return k.shape }

// Dirs is the number of leaf directories (some may be empty; none for a
// given list).
func (k *Keyspace) Dirs() int { return max(0, len(k.cum)-1) }

// Key is key i (0 <= i < Len).
func (k *Keyspace) Key(i int64) string { return string(k.AppendKey(nil, i)) }

// AppendKey appends key i to b.
func (k *Keyspace) AppendKey(b []byte, i int64) []byte {
	if k.offs != nil {
		return append(b, k.listed(i)...)
	}
	// The leaf directory holding i: the last j with cum[j] <= i.
	j := sort.Search(len(k.cum)-1, func(j int) bool { return k.cum[j+1] > i })
	n := uint64(k.cum[j+1] - k.cum[j])
	hi, lo := bits.Mul64(uint64(i-k.cum[j]), k.slots)
	slot, _ := bits.Div64(hi, lo, n) // t*slots/n: strictly increasing in t, below slots
	b = append(b, k.root...)
	rest := uint64(j)
	div := uint64(k.Dirs())
	for _, l := range k.levels {
		div /= uint64(l.radix)
		b = append(b, l.pad...)
		b = appendBase36(b, rest/div, l.width)
		b = append(b, '/')
		rest %= div
	}
	if k.stemN > 0 { // a per-directory head, then the shared run
		b = appendFiller(b, k.shape.Seed, 0xd1e<<32|uint64(j), min(k.stemN, stemHead))
		b = append(b, k.stem...)
	}
	return appendBase36(b, slot, k.leafW)
}

// Search is the first index whose key is at least s (strict: greater than
// s), in [lo, Len].
func (k *Keyspace) Search(s string, strict bool, lo int64) int64 {
	n := k.shape.Keys - lo
	if k.offs != nil {
		return lo + int64(sort.Search(int(n), func(d int) bool {
			key := k.listed(lo + int64(d))
			return key > s || (key == s && !strict)
		}))
	}
	buf, target := make([]byte, 0, k.maxLen), []byte(s)
	return lo + int64(sort.Search(int(n), func(d int) bool {
		buf = k.AppendKey(buf[:0], lo+int64(d))
		c := bytes.Compare(buf, target)
		return c > 0 || (c == 0 && !strict)
	}))
}

// Count is the number of keys with prefix p.
func (k *Keyspace) Count(p string) int64 {
	lo := k.Search(p, false, 0)
	if up, ok := successor(p); ok {
		return k.Search(up, false, lo) - lo
	}
	return k.shape.Keys - lo
}

// Query is one listing request, in the provider-neutral form both wire
// shapes map onto.
type Query struct {
	Prefix    string
	Delimiter string
	// After is the lower bound: keys > After (S3 start-after), or keys >=
	// After with Inclusive (GCS startOffset).
	After     string
	Inclusive bool
	End       string // keys < End (GCS endOffset); "": no bound
	Max       int    // items per page (keys plus prefixes), 1..1000; 0: 1000
	Token     string // a previous page's Next; "": the first page
	// RollUpBelow: keys past After roll up into a common prefix even when
	// the prefix is at or below After (GCS). Otherwise such a prefix is
	// skipped with every key under it (S3's StartAfter, as measured live).
	RollUpBelow bool
}

// Entry is one page item: a key (with its index, for metadata) or a
// common prefix.
type Entry struct {
	Name   string
	Index  int64 // the key's index; -1 for a prefix
	Prefix bool
}

// Page is one answer.
type Page struct {
	Entries []Entry
	Keys    int // entries that are keys
	Next    string
}

// ErrToken is a malformed continuation token.
var ErrToken = errors.New("scalefake: bad continuation token")

// MaxPage is the largest page, as both services cap it.
const MaxPage = 1000

// First is the first key at or past q's start (its prefix, bound and
// continuation), whether or not it is in q's scope, and false past the
// last key: the key a page starting there is about.
func (k *Keyspace) First(q Query) (string, bool, error) {
	i, err := k.start(q)
	if err != nil || i >= k.shape.Keys {
		return "", false, err
	}
	return k.Key(i), true, nil
}

// start is the index q's first page item is at or past.
func (k *Keyspace) start(q Query) (int64, error) {
	i := k.Search(q.Prefix, false, 0)
	if q.After != "" {
		i = max(i, k.Search(q.After, !q.Inclusive, 0))
	}
	if q.Token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(q.Token)
		if err != nil || len(raw) == 0 || (raw[0] != 'k' && raw[0] != 'p') {
			return 0, ErrToken
		}
		last := string(raw[1:])
		if raw[0] == 'k' {
			i = max(i, k.Search(last, true, 0))
		} else if up, ok := successor(last); ok {
			i = max(i, k.Search(up, false, 0))
		} else {
			i = k.shape.Keys
		}
	}
	return i, nil
}

// List answers q.
func (k *Keyspace) List(q Query) (Page, error) {
	limit := q.Max
	if limit <= 0 || limit > MaxPage {
		limit = MaxPage
	}
	i, err := k.start(q)
	if err != nil {
		return Page{}, err
	}
	var p Page
	buf, prefix, delim := make([]byte, 0, k.maxLen), []byte(q.Prefix), []byte(q.Delimiter)
	var last []byte
	lastPrefix := false
	for i < k.shape.Keys {
		buf = k.AppendKey(buf[:0], i)
		if !bytes.HasPrefix(buf, prefix) || (q.End != "" && string(buf) >= q.End) {
			break
		}
		name, isPrefix, next := buf, false, i+1
		if q.Delimiter != "" {
			if d := bytes.Index(buf[len(prefix):], delim); d >= 0 {
				name, isPrefix = buf[:len(q.Prefix)+d+len(q.Delimiter)], true
				next = k.shape.Keys
				if up, ok := successor(string(name)); ok {
					next = k.Search(up, false, i+1)
				}
				if !q.RollUpBelow && q.After != "" && string(name) <= q.After {
					i = next
					continue
				}
			}
		}
		if len(p.Entries) == limit {
			kind := byte('k')
			if lastPrefix {
				kind = 'p'
			}
			p.Next = base64.RawURLEncoding.EncodeToString(append([]byte{kind}, last...))
			break
		}
		e := Entry{Name: string(name), Index: i, Prefix: isPrefix}
		if isPrefix {
			e.Index = -1
		} else {
			p.Keys++
		}
		p.Entries = append(p.Entries, e)
		last, lastPrefix = []byte(e.Name), isPrefix
		i = next
	}
	return p, nil
}

// successor is the least string above every string with prefix p (false:
// none, p is all 0xff).
func successor(p string) (string, bool) {
	b := []byte(p)
	for len(b) > 0 && b[len(b)-1] == 0xff {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "", false
	}
	b[len(b)-1]++
	return string(b), true
}

func width36(v uint64) int {
	w := 1
	for v >= 36 {
		v /= 36
		w++
	}
	return w
}

func appendBase36(b []byte, v uint64, w int) []byte {
	n := len(b)
	for range w {
		b = append(b, '0')
	}
	for p := len(b) - 1; p >= n && v > 0; p-- {
		b[p] = digits36[v%36]
		v /= 36
	}
	return b
}

// splitmix is SplitMix64's finalizer: a deterministic hash of x.
func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func hash2(seed, x uint64) uint64 { return splitmix(seed ^ splitmix(x)) }

// normal is a standard normal draw from (seed, x) (Box-Muller).
func normal(seed, x uint64) float64 {
	u1 := (float64(hash2(seed, x<<1)>>11) + 0.5) / (1 << 53)
	u2 := float64(hash2(seed, x<<1|1)>>11) / (1 << 53)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

const fillerChars = "abcdefghijklmnopqrstuvwxyz0123456789-_"

func filler(seed, salt uint64, n int) string { return string(appendFiller(nil, seed, salt, n)) }

func appendFiller(b []byte, seed, salt uint64, n int) []byte {
	h := hash2(seed, salt)
	for p := range n {
		if p%8 == 0 {
			h = splitmix(h)
		}
		b = append(b, fillerChars[(h>>(8*(p%8)))%uint64(len(fillerChars))])
	}
	return b
}

// Meta is a key's synthetic metadata, from the seed and its index.
type Meta struct {
	Size  int64
	Mtime int64  // Unix seconds
	ETag  string // 32 hex digits, no quotes
}

// Meta is key i's metadata.
func (k *Keyspace) Meta(i int64) Meta {
	h := hash2(k.shape.Seed, uint64(i))
	g := splitmix(h)
	const base, span = 1577836800, 5 * 365 * 86400 // 2020-01-01, five years
	return Meta{Size: int64(h >> 40), Mtime: base + int64(g%span), ETag: fmt.Sprintf("%016x%016x", h, g)}
}

// String describes the shape for logs and reports.
func (s Shape) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "keys=%d", s.Keys)
	if len(s.Fanout) == 0 {
		b.WriteString(" flat")
	} else {
		f := make([]string, len(s.Fanout))
		for i, x := range s.Fanout {
			f[i] = fmt.Sprint(x)
		}
		fmt.Fprintf(&b, " fanout=%s", strings.Join(f, "x"))
	}
	fmt.Fprintf(&b, " skew=%g shared-prefix=%d segment-pad=%d key-len=%d seed=%d", s.Skew, s.SharedPrefix, s.SegmentPad, s.KeyLen, s.Seed)
	return b.String()
}
