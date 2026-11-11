// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

// This file is the scanner as it was at 8c8f8708, before the parsing-cost
// changes, kept verbatim but for its identifiers (prefixed "old") as the
// oracle of the differential tests in xml_diff_test.go: the scanner must
// accept and refuse the same documents and return the same pages and
// errors.

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

type oldParser struct {
	b        []byte
	i        int
	arena    []byte
	tmp      []byte
	objs     []objSpan
	prefixes []span
	versions bool // a ListVersionsResult: entries also carry VersionId and IsLatest

	truncated bool
	token     span
	encoding  span

	// ListObjectVersions only.
	nextKey, nextVID, prefix, keyMarker span
}

const (
	oldTEnd = iota + 1
	oldTStart
)

// reset starts a parse of body, keeping the scratch buffers.
func (p *oldParser) reset(body []byte, versions bool) {
	*p = oldParser{b: body, tmp: p.tmp[:0], arena: p.arena[:0], objs: p.objs[:0], prefixes: p.prefixes[:0], versions: versions}
}

// oldParsers keeps oldParsers' scratch buffers between pages.
var oldParsers sync.Pool // *oldParser

// oldParseList parses a ListObjectsV2 response body (a ListBucketResult) with
// the scanner. A body that does not parse is objhttp.ErrSyntax.
func oldParseList(body []byte) (*Page, error) { return oldPooled(body, (*oldParser).parseList) }

// oldParseVersions parses a ListObjectVersions response body (a
// ListVersionsResult) with the scanner, then checks the page
// (VersionPage.Validate).
func oldParseVersions(body []byte) (*VersionPage, error) {
	return oldPooled(body, (*oldParser).parseVersions)
}

// oldPooled runs parse on body with a oldPooled oldParser.
func oldPooled[T any](body []byte, parse func(*oldParser, []byte) (*T, error)) (*T, error) {
	p, _ := oldParsers.Get().(*oldParser)
	if p == nil {
		p = new(oldParser)
	}
	defer oldParsers.Put(p)
	return parse(p, body)
}

// oldParseList parses a ListBucketResult document.
func (p *oldParser) parseList(body []byte) (*Page, error) {
	p.reset(body, false)
	name, empty, err := p.root()
	if err != nil {
		return nil, err
	}
	if string(oldLocal(name)) != "ListBucketResult" {
		return nil, syntax("root element <%s>, want <ListBucketResult>", name)
	}
	if !empty {
		if err := p.listBody(name); err != nil {
			return nil, err
		}
	}
	return p.finish()
}

// oldParseVersions parses a ListVersionsResult document: its <Version> and
// <DeleteMarker> entries in one slice, in wire order. A truncated page
// without a NextKeyMarker is an invalid page: nothing says where the
// listing continues.
func (p *oldParser) parseVersions(body []byte) (*VersionPage, error) {
	p.reset(body, true)
	name, empty, err := p.root()
	if err != nil {
		return nil, err
	}
	if string(oldLocal(name)) != "ListVersionsResult" {
		return nil, syntax("root element <%s>, want <ListVersionsResult>", name)
	}
	if !empty {
		if err := p.versionsBody(name); err != nil {
			return nil, err
		}
	}
	pg, err := p.finishVersions()
	if err != nil {
		return nil, err
	}
	if err := pg.Validate(); err != nil {
		return nil, err
	}
	return pg, nil
}

// root skips the prolog (anything before the first start tag) and returns
// the root element's name.
func (p *oldParser) root() (name []byte, empty bool, err error) {
	k, name, empty, err := p.next()
	switch {
	case err != nil:
		return nil, false, err
	case k != oldTStart:
		return nil, false, syntax("end tag </%s> before the root element", name)
	}
	return name, empty, nil
}

func (p *oldParser) listBody(self []byte) error {
	for {
		k, name, empty, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(self, name)
		}
		switch string(oldLocal(name)) {
		case "Contents":
			if err := p.entry(name, empty, false); err != nil {
				return err
			}
		case "CommonPrefixes", "IsTruncated", "EncodingType":
			if err := p.common(name, empty); err != nil {
				return err
			}
		case "NextContinuationToken":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			p.token = p.store(t)
		case "KeyCount":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			if _, err := oldParseInt(t); err != nil {
				return err
			}
		default:
			if err := p.skip(name, empty); err != nil {
				return err
			}
		}
	}
}

func (p *oldParser) versionsBody(self []byte) error {
	for {
		k, name, empty, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(self, name)
		}
		var s *span
		switch string(oldLocal(name)) {
		case "Version", "DeleteMarker":
			if err := p.entry(name, empty, string(oldLocal(name)) == "DeleteMarker"); err != nil {
				return err
			}
			continue
		case "CommonPrefixes", "IsTruncated", "EncodingType":
			if err := p.common(name, empty); err != nil {
				return err
			}
			continue
		case "NextKeyMarker":
			s = &p.nextKey
		case "NextVersionIdMarker":
			s = &p.nextVID
		case "Prefix":
			s = &p.prefix
		case "KeyMarker":
			s = &p.keyMarker
		default:
			if err := p.skip(name, empty); err != nil {
				return err
			}
			continue
		}
		t, err := p.scalar(name, empty)
		if err != nil {
			return err
		}
		*s = p.store(t)
	}
}

// entry appends one <Contents>, <Version> or <DeleteMarker> entry.
func (p *oldParser) entry(name []byte, empty, deleteMarker bool) error {
	p.objs = append(p.objs, objSpan{deleteMarker: deleteMarker})
	if empty {
		return nil
	}
	return p.contents(name, len(p.objs)-1)
}

// common reads an element both listings share: CommonPrefixes,
// IsTruncated or EncodingType.
func (p *oldParser) common(name []byte, empty bool) error {
	if string(oldLocal(name)) == "CommonPrefixes" {
		p.prefixes = append(p.prefixes, span{})
		if empty {
			return nil
		}
		return p.commonPrefix(name, &p.prefixes[len(p.prefixes)-1])
	}
	t, err := p.scalar(name, empty)
	if err != nil {
		return err
	}
	if string(oldLocal(name)) == "EncodingType" {
		p.encoding = p.store(t)
		return nil
	}
	p.truncated, err = oldParseBool(t)
	return err
}

func (p *oldParser) contents(self []byte, i int) error {
	for {
		k, name, empty, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(self, name)
		}
		o := &p.objs[i]
		field := string(oldLocal(name))
		if !p.versions && (field == "VersionId" || field == "IsLatest") {
			field = "" // not ListObjectsV2's: skipped
		}
		switch field {
		case "VersionId":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			// Kept verbatim (never URL-decoded); "null" repeats on every
			// entry of an unversioned bucket, so it is shared.
			o.vid = p.shared(t, i, func(o *objSpan) span { return o.vid })
		case "IsLatest":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			if o.latest, err = oldParseBool(t); err != nil {
				return err
			}
		case "Key":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			o.key = p.store(t)
		case "ETag":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			o.etag = p.store(t)
		case "Size":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			if o.size, err = oldParseInt(t); err != nil {
				return err
			}
		case "LastModified":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			if o.mtime, err = oldParseTime(t); err != nil {
				return err
			}
		case "StorageClass":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			o.class = p.shared(t, i, func(o *objSpan) span { return o.class })
		case "ChecksumAlgorithm":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			if !o.hasAlgo {
				o.algo, o.hasAlgo = p.shared(t, i, func(o *objSpan) span { return o.algo }), true
			}
		case "ChecksumType":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			o.ctype = p.shared(t, i, func(o *objSpan) span { return o.ctype })
		case "Owner":
			if !empty {
				if err := p.owner(name, i); err != nil {
					return err
				}
			}
		default: // RestoreStatus and anything newer
			if err := p.skip(name, empty); err != nil {
				return err
			}
		}
	}
}

func (p *oldParser) owner(self []byte, i int) error {
	for {
		k, name, empty, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(self, name)
		}
		switch string(oldLocal(name)) {
		case "ID":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			p.objs[i].ownerID = p.shared(t, i, func(o *objSpan) span { return o.ownerID })
		case "DisplayName":
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			p.objs[i].ownerName = p.shared(t, i, func(o *objSpan) span { return o.ownerName })
		default:
			if err := p.skip(name, empty); err != nil {
				return err
			}
		}
	}
}

func (p *oldParser) commonPrefix(self []byte, s *span) error {
	for {
		k, name, empty, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(self, name)
		}
		if string(oldLocal(name)) == "Prefix" {
			t, err := p.scalar(name, empty)
			if err != nil {
				return err
			}
			*s = p.store(t)
		} else if err := p.skip(name, empty); err != nil {
			return err
		}
	}
}

// store copies t into the arena.
func (p *oldParser) store(t []byte) span {
	s := span{uint32(len(p.arena)), uint32(len(t))}
	p.arena = append(p.arena, t...)
	return s
}

// shared stores t, reusing the previous object's copy when it is equal:
// storage class, owner and checksum fields repeat across a page.
func (p *oldParser) shared(t []byte, i int, field func(*objSpan) span) span {
	if i > 0 {
		if s := field(&p.objs[i-1]); int(s.n) == len(t) && bytes.Equal(p.arena[s.off:s.off+s.n], t) {
			return s
		}
	}
	return p.store(t)
}

// decode URL-decodes keys, prefixes and the spans given when the response
// says encoding-type=url, once and in place (decoding only shrinks).
func (p *oldParser) decode(more ...*span) error {
	if p.text(p.encoding) != "url" {
		return nil
	}
	for i := range p.objs {
		if err := p.unescape(&p.objs[i].key); err != nil {
			return err
		}
	}
	for i := range p.prefixes {
		if err := p.unescape(&p.prefixes[i]); err != nil {
			return err
		}
	}
	for _, s := range more {
		if err := p.unescape(s); err != nil {
			return err
		}
	}
	return nil
}

// own makes the page's arena: the oldPooled scratch arena is copied to one
// sized exactly (rows hold it until they are encoded, so slack would be
// retained too). The returned function gives the scratch back to the
// oldParser once the page's strings are taken.
func (p *oldParser) own() (release func()) {
	scratch := p.arena
	p.arena = make([]byte, len(scratch))
	copy(p.arena, scratch)
	return func() { p.arena, p.b = scratch[:0], nil }
}

// object is an entry's ListObjectsV2 fields.
func (p *oldParser) object(s *objSpan) Object {
	return Object{Key: p.text(s.key), ETag: p.text(s.etag), StorageClass: p.text(s.class),
		OwnerID: p.text(s.ownerID), OwnerDisplayName: p.text(s.ownerName),
		ChecksumAlgorithm: p.text(s.algo), ChecksumType: p.text(s.ctype), Size: s.size, Mtime: s.mtime}
}

// prefixStrings is the page's common prefixes, or nil.
func (p *oldParser) prefixStrings() []string {
	if len(p.prefixes) == 0 {
		return nil
	}
	out := make([]string, len(p.prefixes))
	for i, s := range p.prefixes {
		out[i] = p.text(s)
	}
	return out
}

// finish turns a ListBucketResult's spans into its page.
func (p *oldParser) finish() (*Page, error) {
	if err := p.decode(); err != nil {
		return nil, err
	}
	defer p.own()()
	pg := &Page{IsTruncated: p.truncated, NextContinuationToken: p.text(p.token)}
	pg.Contents = make([]Object, len(p.objs))
	for i := range p.objs {
		pg.Contents[i] = p.object(&p.objs[i])
	}
	pg.CommonPrefixes = p.prefixStrings()
	return pg, nil
}

// finishVersions turns a ListVersionsResult's spans into its page. The
// NextKeyMarker is decoded like keys, and the echoed Prefix and KeyMarker
// are decoded only to refuse a malformed escape; version IDs and the
// NextVersionIdMarker are not encoded by S3 and are kept verbatim.
func (p *oldParser) finishVersions() (*VersionPage, error) {
	if err := p.decode(&p.nextKey, &p.prefix, &p.keyMarker); err != nil {
		return nil, err
	}
	defer p.own()()
	pg := &VersionPage{IsTruncated: p.truncated, NextKeyMarker: p.text(p.nextKey), NextVersionIDMarker: p.text(p.nextVID)}
	pg.Versions = make([]Version, len(p.objs))
	for i := range p.objs {
		s := &p.objs[i]
		pg.Versions[i] = Version{Object: p.object(s), VersionID: p.text(s.vid), IsLatest: s.latest,
			DeleteMarker: s.deleteMarker}
	}
	pg.CommonPrefixes = p.prefixStrings()
	return pg, nil
}

// text views an arena span as a string. In finish the arena is the page's
// own copy, never written again and never oldPooled.
func (p *oldParser) text(s span) string {
	if s.n == 0 {
		return ""
	}
	return unsafe.String(&p.arena[s.off], int(s.n))
}

// unescape percent-decodes a span in place with form semantics ('+' is a
// space), as the AWS SDKs decode encoding-type=url responses.
func (p *oldParser) unescape(s *span) error {
	b := p.arena[s.off : s.off+s.n]
	if bytes.IndexByte(b, '%') < 0 && bytes.IndexByte(b, '+') < 0 {
		return nil
	}
	w := 0
	for r := 0; r < len(b); r++ {
		switch c := b[r]; c {
		case '+':
			b[w] = ' '
		case '%':
			if r+2 >= len(b) || !oldIshex(b[r+1]) || !oldIshex(b[r+2]) {
				return syntax("invalid URL escape in %q", b)
			}
			b[w] = oldUnhex(b[r+1])<<4 | oldUnhex(b[r+2])
			r += 2
		default:
			b[w] = c
		}
		w++
	}
	s.n = uint32(w)
	return nil
}

func oldIshex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func oldUnhex(c byte) byte {
	switch {
	case c <= '9':
		return c - '0'
	case c >= 'a':
		return c - 'a' + 10
	}
	return c - 'A' + 10
}

// oldLocal strips a namespace prefix.
func oldLocal(name []byte) []byte {
	if i := bytes.IndexByte(name, ':'); i >= 0 {
		return name[i+1:]
	}
	return name
}

func (p *oldParser) closes(self, end []byte) error {
	if !bytes.Equal(self, end) {
		return syntax("element <%s> closed by </%s>", self, end)
	}
	return nil
}

// next returns the next start or end tag, skipping character data,
// comments, CDATA, processing instructions and directives.
func (p *oldParser) next() (kind int, name []byte, empty bool, err error) {
	b := p.b
	for {
		j := bytes.IndexByte(b[p.i:], '<')
		if j < 0 {
			return 0, nil, false, syntax("unexpected end of document")
		}
		p.i += j
		if err := p.skipMarkup(); err != oldErrNotMarkup {
			if err != nil {
				return 0, nil, false, err
			}
			continue
		}
		return p.tag()
	}
}

var oldErrNotMarkup = errors.New("not markup")

// skipMarkup skips a comment, CDATA section, processing instruction or
// directive at p.i, or returns oldErrNotMarkup when p.i starts a tag.
func (p *oldParser) skipMarkup() error {
	rest := p.b[p.i:]
	var end string
	switch {
	case bytes.HasPrefix(rest, []byte("<!--")):
		end = "-->"
		p.i += 4
	case bytes.HasPrefix(rest, []byte("<![CDATA[")):
		end = "]]>"
		p.i += 9
	case bytes.HasPrefix(rest, []byte("<?")):
		end = "?>"
		p.i += 2
	case bytes.HasPrefix(rest, []byte("<!")):
		return p.skipDirective()
	default:
		return oldErrNotMarkup
	}
	k := bytes.Index(p.b[p.i:], []byte(end))
	if k < 0 {
		return syntax("unterminated %s", end)
	}
	p.i += k + len(end)
	return nil
}

// skipDirective skips <!...> as encoding/xml does: the first character
// after "<!" is taken literally, quotes hide angle brackets, other angle
// brackets nest, and embedded comments are skipped.
func (p *oldParser) skipDirective() error {
	depth, quote := 0, byte(0)
	for k := p.i + 3; k < len(p.b); k++ {
		c := p.b[k]
		switch {
		case quote == 0 && c == '>' && depth == 0:
			p.i = k + 1
			return nil
		case c == quote:
			quote = 0
		case quote != 0:
		case c == '"' || c == '\'':
			quote = c
		case c == '>':
			depth--
		case c == '<':
			if bytes.HasPrefix(p.b[k+1:], []byte("!--")) {
				e := bytes.Index(p.b[k+4:], []byte("-->"))
				if e < 0 {
					return syntax("unterminated comment")
				}
				k += 4 + e + 2
				continue
			}
			depth++
		}
	}
	return syntax("unterminated directive")
}

// tag reads the start or end tag at p.i.
func (p *oldParser) tag() (kind int, name []byte, empty bool, err error) {
	b := p.b
	k := p.i + 1
	kind = oldTStart
	if k < len(b) && b[k] == '/' {
		kind = oldTEnd
		k++
	}
	s := k
	for k < len(b) && !oldIsSpace(b[k]) && b[k] != '>' && b[k] != '/' {
		k++
	}
	name = b[s:k]
	if len(name) == 0 {
		return 0, nil, false, syntax("tag without a name")
	}
	quote := byte(0)
	for ; k < len(b); k++ {
		c := b[k]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '>':
			empty = kind == oldTStart && b[k-1] == '/'
			p.i = k + 1
			return kind, name, empty, nil
		}
	}
	return 0, nil, false, syntax("unterminated tag <%s", name)
}

func oldIsSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// skip skips the rest of element name.
func (p *oldParser) skip(name []byte, empty bool) error {
	if empty {
		return nil
	}
	for {
		k, n, e, err := p.next()
		if err != nil {
			return err
		}
		if k == oldTEnd {
			return p.closes(name, n)
		}
		if err := p.skip(n, e); err != nil {
			return err
		}
	}
}

// scalar returns the character data of element name: oldEntities decoded,
// CDATA included, comments and child elements skipped, \r\n normalised.
// The result is valid until the next call.
func (p *oldParser) scalar(name []byte, empty bool) ([]byte, error) {
	if empty {
		return nil, nil
	}
	b := p.b
	// Fast path: plain text then the end tag.
	if j := bytes.IndexByte(b[p.i:], '<'); j >= 0 {
		t := b[p.i : p.i+j]
		e := p.i + j
		if e+2+len(name) < len(b) && b[e+1] == '/' && bytes.Equal(b[e+2:e+2+len(name)], name) &&
			b[e+2+len(name)] == '>' && bytes.IndexByte(t, '&') < 0 && bytes.IndexByte(t, '\r') < 0 &&
			!bytes.Contains(t, []byte("]]>")) {
			p.i = e + 3 + len(name)
			return t, nil
		}
	}
	p.tmp = p.tmp[:0]
	for {
		j := bytes.IndexByte(b[p.i:], '<')
		if j < 0 {
			return nil, syntax("unexpected end of document in <%s>", name)
		}
		var err error
		if p.tmp, err = oldAppendText(p.tmp, b[p.i:p.i+j]); err != nil {
			return nil, err
		}
		p.i += j
		rest := b[p.i:]
		if bytes.HasPrefix(rest, []byte("<![CDATA[")) {
			k := bytes.Index(rest[9:], []byte("]]>"))
			if k < 0 {
				return nil, syntax("unterminated CDATA")
			}
			p.tmp = oldAppendNormalised(p.tmp, rest[9:9+k])
			p.i += 9 + k + 3
			continue
		}
		if err := p.skipMarkup(); err != oldErrNotMarkup {
			if err != nil {
				return nil, err
			}
			continue
		}
		k, n, e, err := p.tag()
		if err != nil {
			return nil, err
		}
		if k == oldTEnd {
			if err := p.closes(name, n); err != nil {
				return nil, err
			}
			return p.tmp, nil
		}
		if err := p.skip(n, e); err != nil {
			return nil, err
		}
	}
}

// oldAppendText appends character data with oldEntities decoded and line endings
// normalised (encoding/xml's text()).
func oldAppendText(dst, t []byte) ([]byte, error) {
	if bytes.Contains(t, []byte("]]>")) {
		return nil, syntax("unescaped ]]> in character data")
	}
	for len(t) > 0 {
		a := bytes.IndexByte(t, '&')
		if a < 0 {
			return oldAppendNormalised(dst, t), nil
		}
		dst = oldAppendNormalised(dst, t[:a])
		t = t[a:]
		semi := bytes.IndexByte(t, ';')
		if semi < 0 {
			return nil, syntax("invalid character entity")
		}
		ent := t[1:semi]
		switch {
		case len(ent) > 1 && ent[0] == '#':
			digits, base := ent[1:], 10
			if digits[0] == 'x' {
				digits, base = digits[1:], 16
			}
			n, err := strconv.ParseUint(string(digits), base, 64)
			if err != nil || n > unicode.MaxRune {
				return nil, syntax("invalid character entity &%s;", ent)
			}
			dst = utf8.AppendRune(dst, rune(n))
		default:
			r, ok := oldEntities[string(ent)]
			if !ok {
				return nil, syntax("invalid character entity &%s;", ent)
			}
			dst = append(dst, r)
		}
		t = t[semi+1:]
	}
	return dst, nil
}

var oldEntities = map[string]byte{"lt": '<', "gt": '>', "amp": '&', "apos": '\'', "quot": '"'}

// oldAppendNormalised turns \r\n and lone \r into \n.
func oldAppendNormalised(dst, t []byte) []byte {
	for {
		r := bytes.IndexByte(t, '\r')
		if r < 0 {
			return append(dst, t...)
		}
		dst = append(append(dst, t[:r]...), '\n')
		t = t[r+1:]
		if len(t) > 0 && t[0] == '\n' {
			t = t[1:]
		}
	}
}

// oldParseInt parses an integer as encoding/xml does (empty is zero).
func oldParseInt(t []byte) (int64, error) {
	if len(t) == 0 {
		return 0, nil
	}
	if len(t) <= 18 {
		var n int64
		ok := true
		for _, c := range t {
			if c < '0' || c > '9' {
				ok = false
				break
			}
			n = n*10 + int64(c-'0')
		}
		if ok {
			return n, nil
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(t)), 10, 64)
	if err != nil {
		return 0, syntax("integer %q", t)
	}
	return n, nil
}

func oldParseBool(t []byte) (bool, error) {
	switch string(t) {
	case "true":
		return true, nil
	case "false", "":
		return false, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(string(t)))
	if err != nil {
		return false, syntax("boolean %q", t)
	}
	return v, nil
}

// oldParseTime returns a timestamp in Unix microseconds (0 when empty). The
// fast path takes S3's form 2006-01-02T15:04:05[.fff]Z; anything else goes
// through time.Time.UnmarshalText, which encoding/xml would call.
func oldParseTime(t []byte) (int64, error) {
	if len(t) == 0 {
		return 0, nil
	}
	if us, ok := oldFastTime(t); ok {
		return us, nil
	}
	var tm time.Time
	if err := tm.UnmarshalText(t); err != nil {
		return 0, syntax("timestamp %q", t)
	}
	return tm.UnixMicro(), nil
}

// oldFastTime parses the common form in about 35 ns against about 68 ns for
// time.Time.UnmarshalText (BenchmarkFastTime). TestFastTimeMatchesStandard and
// FuzzFastTime pin it to the standard oldParser: it declines what it is unsure of.
func oldFastTime(t []byte) (int64, bool) {
	if len(t) < 20 || t[4] != '-' || t[7] != '-' || t[10] != 'T' || t[13] != ':' || t[16] != ':' || t[len(t)-1] != 'Z' {
		return 0, false
	}
	num := func(s []byte) (int, bool) {
		n := 0
		for _, c := range s {
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}
	y, ok1 := num(t[0:4])
	mo, ok2 := num(t[5:7])
	d, ok3 := num(t[8:10])
	h, ok4 := num(t[11:13])
	mi, ok5 := num(t[14:16])
	s, ok6 := num(t[17:19])
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) || mo < 1 || mo > 12 || d < 1 || d > oldDaysIn(mo, y) || h > 23 || mi > 59 || s > 59 {
		return 0, false
	}
	frac := t[19 : len(t)-1]
	us := 0
	if len(frac) > 0 {
		if frac[0] != '.' || len(frac) < 2 || len(frac) > 10 {
			return 0, false
		}
		f, ok := num(frac[1:])
		if !ok {
			return 0, false
		}
		for k := len(frac) - 1; k < 9; k++ {
			f *= 10
		}
		us = f / 1000
	}
	days := oldCivilDays(y, mo, d)
	return (days*86400+int64(h*3600+mi*60+s))*1e6 + int64(us), true
}

func oldDaysIn(m, y int) int {
	switch m {
	case 2:
		if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}

// oldCivilDays is days since 1970-01-01 (Howard Hinnant's days_from_civil).
func oldCivilDays(y, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := y
	if era < 0 {
		era -= 399
	}
	era /= 400
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int64(era*146097+doe) - 719468
}
