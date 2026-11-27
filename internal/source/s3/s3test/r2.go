// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// R2SortKey is the form Cloudflare R2's ListObjectsV2 orders a key by, as
// measured on 2026-11-27 (a fitted rule, not a documented contract): the
// key's Unicode NFC form followed by ":7e", compared as bytes. So a key
// that is a proper prefix of others sorts among them (a.jpg.orig before
// a.jpg; a marker d/ after d/0), and a decomposed key sorts by its
// composed form. The one place the fake's order is defined.
func R2SortKey(key string) string { return norm.NFC.String(key) + ":7e" }

// R2 is a test-owned model of Cloudflare R2's ListObjectsV2 with the
// semantics measured on 2026-11-27, written from those rules rather than
// from a store's implementation:
//
//   - one object per NFC name: a key and its decomposed spelling are one
//     object, the spelling put last;
//   - keys are listed in R2SortKey order and returned as uploaded;
//   - prefix matches the key's NFC form against the prefix's (prefix e
//     does not return a decomposed é…; prefix é does), and a prefix that
//     is not valid UTF-8 is refused with 403;
//   - delimiter groups on the NFC form; a common prefix is returned once,
//     spelled as its first key in that order spells it (so the decomposed
//     and composed spellings of one folder are one common prefix), at
//     that key's position;
//   - continuation tokens are opaque; start-after S returns the keys
//     whose sort form is above the raw bytes of S, a key equal to S left
//     out of the first page only;
//   - max-keys above 1,000 lists 1,000 (the value asked is echoed);
//   - encoding-type=url encodes keys and prefixes.
//
// The hang measured on a delimited listing whose prefix is the NFC
// spelling of a decomposed folder is not modelled. Serve it with Serve.
type R2 struct {
	// Hook, if set, runs before each listing is answered, with the
	// request: a test holds a request here (a barrier).
	Hook func(r *http.Request)
	// Edit, if set, may rewrite a page before it is sent (malformed
	// answers).
	Edit func(q url.Values, p *R2Page)

	mu     sync.Mutex
	objs   []r2Object // sorted by R2SortKey
	log    []url.Values
	tokens map[string]int // token -> position
}

type r2Object struct {
	key, sort, etag string
	size            int64
	mtime           time.Time
}

// R2Page is one answer before it is encoded.
type R2Page struct {
	Contents       []R2Entry
	CommonPrefixes []string
	Truncated      bool
	Token          string
}

// R2Entry is one listed object.
type R2Entry struct {
	Key   string
	Size  int64
	ETag  string // unquoted
	Mtime time.Time
}

// NewR2 holds keys, put in order (a later spelling of an NFC name
// replaces the earlier); each object's size is its key's length, its ETag
// its key's MD5 and its modification time a second after the previous.
func NewR2(keys []string) *R2 {
	f := &R2{tokens: map[string]int{}}
	byNFC := map[string]int{}
	at := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
	for i, k := range keys {
		sum := md5.Sum([]byte(k))
		o := r2Object{key: k, sort: R2SortKey(k), etag: hex.EncodeToString(sum[:]), size: int64(len(k)), mtime: at.Add(time.Duration(i) * time.Second)}
		n := norm.NFC.String(k)
		if j, ok := byNFC[n]; ok {
			f.objs[j] = o
			continue
		}
		byNFC[n] = len(f.objs)
		f.objs = append(f.objs, o)
	}
	slices.SortFunc(f.objs, func(a, b r2Object) int { return strings.Compare(a.sort, b.sort) })
	return f
}

// Keys are the objects' keys as uploaded, in R2's order.
func (f *R2) Keys() []string {
	out := make([]string, len(f.objs))
	for i := range f.objs {
		out[i] = f.objs[i].key
	}
	return out
}

// Log is every listing request's query so far.
func (f *R2) Log() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.log)
}

func (f *R2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Method != http.MethodGet || q.Get("list-type") != "2" || strings.Trim(r.URL.Path, "/") != Name {
		r2Error(w, http.StatusNotImplemented, "NotImplemented", "the R2 fake serves ListObjectsV2 of bucket "+Name)
		return
	}
	f.mu.Lock()
	f.log = append(f.log, q)
	f.mu.Unlock()
	if f.Hook != nil {
		f.Hook(r)
	}
	prefix := q.Get("prefix")
	if !utf8.ValidString(prefix) {
		r2Error(w, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
		return
	}
	max := 1000
	if s := q.Get("max-keys"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			r2Error(w, http.StatusBadRequest, "InvalidArgument", "max-keys")
			return
		}
		max = min(n, 1000)
	}
	p, err := f.list(prefix, q.Get("delimiter"), q.Get("start-after"), q.Get("continuation-token"), max)
	if err != nil {
		r2Error(w, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if f.Edit != nil {
		f.Edit(q, p)
	}
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, f.encode(q, p))
}

// list answers one request from the objects in R2's order.
func (f *R2) list(prefix, delim, startAfter, token string, max int) (*R2Page, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nfcPrefix := norm.NFC.String(prefix)
	var cands []int
	for i := range f.objs {
		if strings.HasPrefix(norm.NFC.String(f.objs[i].key), nfcPrefix) {
			cands = append(cands, i)
		}
	}
	at, exclude := 0, ""
	switch {
	case token != "":
		pos, ok := f.tokens[token]
		if !ok {
			return nil, fmt.Errorf("the continuation token is not valid")
		}
		at = pos
	case startAfter != "":
		at, _ = slices.BinarySearchFunc(cands, startAfter, func(i int, s string) int {
			if f.objs[i].sort > s {
				return 1
			}
			return -1
		})
		exclude = startAfter
	}
	p := &R2Page{}
	n := 0
	for at < len(cands) {
		o := &f.objs[cands[at]]
		if exclude != "" && o.key == exclude {
			at++
			continue
		}
		if n == max {
			break
		}
		nfc := norm.NFC.String(o.key)
		if j := strings.Index(nfc[len(nfcPrefix):], delim); delim != "" && j >= 0 {
			group := nfc[:len(nfcPrefix)+j+len(delim)]
			p.CommonPrefixes = append(p.CommonPrefixes, rawPrefix(o.key, strings.Count(group, delim), delim))
			for at < len(cands) && strings.HasPrefix(norm.NFC.String(f.objs[cands[at]].key), group) {
				at++
			}
		} else {
			p.Contents = append(p.Contents, R2Entry{Key: o.key, Size: o.size, ETag: o.etag, Mtime: o.mtime})
			at++
		}
		n++
	}
	if at < len(cands) {
		p.Truncated = true
		p.Token = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("r2|%d|%d", len(f.tokens), at)))
		f.tokens[p.Token] = at
	}
	return p, nil
}

// rawPrefix is key up to and including its n-th delimiter: a group's
// spelling as that key was uploaded (NFC moves no delimiter).
func rawPrefix(key string, n int, delim string) string {
	end := 0
	for range n {
		end += strings.Index(key[end:], delim) + len(delim)
	}
	return key[:end]
}

// encode writes p as a ListBucketResult, keys and prefixes URL-encoded
// when the request asked.
func (f *R2) encode(q url.Values, p *R2Page) string {
	enc := func(s string) string {
		if q.Get("encoding-type") == "url" {
			s = url.QueryEscape(s)
		}
		return html.EscapeString(s)
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount>", Name, enc(q.Get("prefix")), len(p.Contents)+len(p.CommonPrefixes))
	if m := q.Get("max-keys"); m != "" {
		fmt.Fprintf(&b, "<MaxKeys>%s</MaxKeys>", html.EscapeString(m))
	} else {
		b.WriteString("<MaxKeys>1000</MaxKeys>")
	}
	if d := q.Get("delimiter"); d != "" {
		fmt.Fprintf(&b, "<Delimiter>%s</Delimiter>", enc(d))
	}
	if q.Get("encoding-type") == "url" {
		b.WriteString("<EncodingType>url</EncodingType>")
	}
	fmt.Fprintf(&b, "<IsTruncated>%v</IsTruncated>", p.Truncated)
	if t := q.Get("continuation-token"); t != "" {
		fmt.Fprintf(&b, "<ContinuationToken>%s</ContinuationToken>", html.EscapeString(t))
	}
	if s := q.Get("start-after"); s != "" {
		fmt.Fprintf(&b, "<StartAfter>%s</StartAfter>", enc(s))
	}
	if p.Token != "" {
		fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", html.EscapeString(p.Token))
	}
	for _, c := range p.Contents {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>&quot;%s&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass>",
			enc(c.Key), c.Mtime.Format("2006-01-02T15:04:05.000Z"), html.EscapeString(c.ETag), c.Size)
		if q.Get("fetch-owner") == "true" {
			b.WriteString("<Owner><ID>r2-account</ID><DisplayName>r2-account</DisplayName></Owner>")
		}
		b.WriteString("</Contents>")
	}
	for _, c := range p.CommonPrefixes {
		fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", enc(c))
	}
	b.WriteString("</ListBucketResult>")
	return b.String()
}

func r2Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, html.EscapeString(msg))
}
