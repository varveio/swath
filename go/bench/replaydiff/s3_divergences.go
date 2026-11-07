// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/s3"
)

// The Go S3 route answers as live S3 does in request shapes where the
// Java server answers otherwise (docs/replay.md, "The S3 route"; each was
// probed on live S3 on 2026-11-07): an empty start-after is echoed; a
// parameter with a malformed escape is dropped; the first of duplicated
// parameters wins; '+' is a space; an empty, altered or Java-form
// continuation token is refused (tokens carry a checksum, so their bytes
// differ from the Java server's); a flat walk's token sent with a
// delimiter relists the prefix it ends in; encoding-type is checked;
// marker is refused; encoding-type=url writes a space as '+' and leaves
// '*'; a NUL in the prefix or start-after is refused; key parameters are
// read as UTF-8 with replacement.
//
// Against the Java server every S3 request therefore goes through
// compareS3 instead of a byte comparison of the two answers to the same
// request. It works out what the Go server must answer from the Java
// server's own answers: either a refusal the Java server does not make
// (status, code and message, pinned from the live probe), or the Java
// server's answer to the equivalent request (the query as the Go route
// reads it: malformed parameters dropped, first values, '+' as a space,
// key parameters as UTF-8, the token in the Java server's form) with the
// probed echo and encoding rules applied. The Go answer must equal that
// expectation, continuation token values masked; a difference fails the
// run. When the Java server's answer to the request as sent differs from
// the Go answer, the request is a documented divergence: it is listed
// (documented_divergences) with both answers and the reasons, and counts
// as passed or failed on the Go side alone. Page tokens are compared by
// where they lead: the driver hands the Go server the Java token it is
// given translated to the Go form, and lists the requests whose answers
// differ only in token bytes (token_byte_differences).

// s3DivergenceRef reports whether d compares the S3 route with the Java
// server, where the divergences apply.
func (d *driver) s3DivergenceRef(r request) bool {
	return d.ref != nil && d.ref.name == "java" && (d.o.protocol == "" || d.o.protocol == "s3") && r.Compare == ""
}

// s3Refusal is an S3 answer the Go route gives where the Java server does
// not refuse: live S3's status, code and message.
type s3Refusal struct {
	status        int
	code, message string
}

var (
	refuseToken    = &s3Refusal{400, "InvalidArgument", "The continuation token provided is incorrect"}
	refuseMarker   = &s3Refusal{400, "InvalidArgument", "Marker unsupported with REST.GET.BUCKET in list-type=2"}
	refuseEncoding = &s3Refusal{400, "InvalidArgument", "Invalid Encoding Method specified in Request"}
	refuseNUL      = &s3Refusal{400, "InvalidArgument", "Value must be a sequence of Unicode characters and cannot include Null."}
)

// s3Param is one query parameter as the Go route reads it.
type s3Param struct {
	name  string
	value []byte
	raw   string // the part as sent
}

// s3Plan is how one request is judged.
type s3Plan struct {
	java    string     // the query the Java server's expectation answer is asked with
	goQuery string     // the query the Go server is sent
	refusal *s3Refusal // the Go answer, unless the Java server refuses the list-type or max-keys first
	fixes   []s3Fix
	reasons []string
}

// s3Fix is one probed rule applied to a Java page; reason names it when
// it changes the page.
type s3Fix struct {
	reason string
	apply  func([]byte) []byte
}

// formDecode decodes a query name or value as live S3 does: '+' is a
// space, %XX a byte; ok is false for a '%' not followed by two hex digits.
// It is the driver's own reading, independent of the server's.
func formDecode(s string) ([]byte, bool) {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '+':
			out = append(out, ' ')
		case '%':
			if i+2 >= len(s) || !isHex(s[i+1]) || !isHex(s[i+2]) {
				return nil, false
			}
			out = append(out, unhexByte(s[i+1])<<4|unhexByte(s[i+2]))
			i += 2
		default:
			out = append(out, c)
		}
	}
	return out, true
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func unhexByte(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// s3Params reads a raw ASCII query: parameters with a malformed escape
// dropped, the first of duplicates kept.
func s3Params(raw string) (params []s3Param, reasons []string) {
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		name, value, _ := strings.Cut(part, "=")
		n, okName := formDecode(name)
		v, okValue := formDecode(value)
		if !okName || !okValue {
			reasons = append(reasons, "malformed escape drops "+name)
			continue
		}
		if strings.Contains(part, "+") {
			reasons = append(reasons, "'+' is a space in "+string(n))
		}
		if seen[string(n)] {
			reasons = append(reasons, "first "+string(n)+" wins")
			continue
		}
		seen[string(n)] = true
		params = append(params, s3Param{name: string(n), value: v, raw: part})
	}
	return params, reasons
}

// utf8Replaced is b as UTF-8 with each malformed sequence one U+FFFD, the
// grouping live S3 showed: a lead byte with its valid continuations is one
// replacement, an encoded surrogate (ED A0..BF xx) one for all three
// bytes.
func utf8Replaced(b []byte) []byte {
	if utf8.Valid(b) {
		return b
	}
	var out []byte
	for i := 0; i < len(b); {
		r, n := utf8.DecodeRune(b[i:])
		if r != utf8.RuneError || n > 1 {
			out = append(out, b[i:i+n]...)
			i += n
			continue
		}
		// Consume the lead byte and the continuation bytes its form allows.
		need := 0
		switch c := b[i]; {
		case c >= 0xc2 && c <= 0xdf:
			need = 1
		case c >= 0xe0 && c <= 0xef:
			need = 2
		case c >= 0xf0 && c <= 0xf4:
			need = 3
		}
		j := i + 1
		for k := 0; k < need && j < len(b) && b[j]&0xc0 == 0x80; k++ {
			if k == 0 && (b[i] == 0xe0 && b[j] < 0xa0 || b[i] == 0xf0 && b[j] < 0x90 || b[i] == 0xf4 && b[j] > 0x8f) {
				break
			}
			j++
		}
		out = append(out, "�"...)
		i = j
	}
	return out
}

// javaToken decodes the Java server's token forms: "v2:" +
// base64url(flag || boundary) and "v1:" + base64url(boundary), padded or
// not.
func javaToken(tok string) (boundary []byte, inclusive, ok bool) {
	dec := func(s string) ([]byte, error) {
		if strings.Contains(s, "=") {
			return base64.URLEncoding.DecodeString(s)
		}
		return base64.RawURLEncoding.DecodeString(s)
	}
	switch {
	case strings.HasPrefix(tok, "v2:"):
		p, err := dec(tok[3:])
		if err != nil || len(p) == 0 || p[0] > 1 {
			return nil, false, false
		}
		return p[1:], p[0] == 1, true
	case strings.HasPrefix(tok, "v1:"):
		p, err := dec(tok[3:])
		return p, false, err == nil
	}
	return nil, false, false
}

// javaFormToken is the Java server's token for a resume boundary.
func javaFormToken(boundary []byte, inclusive bool) string {
	p := append([]byte{0}, boundary...)
	if inclusive {
		p[0] = 1
	}
	return "v2:" + base64.RawURLEncoding.EncodeToString(p)
}

// planS3 judges query. driverToken says the continuation token was taken
// from a Java answer by the driver (a token walk or a dependent case), so
// the Go server is handed the same boundary in its own form; a token
// written into the matrix is sent to the Go server as it is.
func planS3(query string, driverToken bool) s3Plan {
	plan := s3Plan{java: query, goQuery: query}
	for i := 0; i < len(query); i++ {
		if query[i] >= 0x80 {
			// The route reads raw non-ASCII bytes as '?'; the driver's
			// reading does not model that, and no rewritten request
			// carries one.
			return plan
		}
	}
	params, reasons := s3Params(query)
	plan.reasons = reasons
	get := func(name string) (*s3Param, bool) {
		for i := range params {
			if params[i].name == name {
				return &params[i], true
			}
		}
		return nil, false
	}
	rewrite := len(reasons) > 0
	tok, hasTok := get("continuation-token")
	goTok, javaTok := "", ""
	if hasTok {
		goTok, javaTok = string(tok.value), string(tok.value)
		if driverToken {
			if b, inc, ok := javaToken(javaTok); ok {
				goTok = s3.EncodeToken(b, inc)
			}
		} else if t, ok, _ := s3.DecodeToken(goTok); ok {
			javaTok = javaFormToken(t.Boundary, t.Inclusive)
			plan.reasons = append(plan.reasons, "checksummed token: the Java form is refused, the Go form served")
			rewrite = true
		}
		if goTok != string(tok.value) {
			plan.goQuery = replacePart(query, tok.raw, "continuation-token="+s3.PercentEncode([]byte(goTok)))
		}
	}
	enc, hasEnc := get("encoding-type")
	prefix, _ := get("prefix")
	startAfter, hasStartAfter := get("start-after")
	switch {
	case hasParam(params, "marker"):
		plan.refusal = refuseMarker
		plan.reasons = append(plan.reasons, "marker refused")
	case hasEnc && !strings.EqualFold(string(enc.value), "url"):
		plan.refusal = refuseEncoding
		plan.reasons = append(plan.reasons, "encoding-type checked")
	case hasTok:
		if _, _, err := s3.DecodeToken(goTok); err != nil {
			plan.refusal = refuseToken
			plan.reasons = append(plan.reasons, "empty, altered or Java-form token refused")
		}
	}
	if plan.refusal == nil {
		switch {
		case prefix != nil && bytes.IndexByte(prefix.value, 0) >= 0, !hasTok && hasStartAfter && bytes.IndexByte(startAfter.value, 0) >= 0:
			plan.refusal = refuseNUL
			plan.reasons = append(plan.reasons, "NUL refused")
		}
	}
	// The Java server's equivalent request.
	var parts []string
	for _, p := range params {
		v := p.value
		switch p.name {
		case "prefix", "delimiter", "start-after":
			if r := utf8Replaced(v); !bytes.Equal(r, v) {
				plan.reasons = append(plan.reasons, p.name+" read as UTF-8")
				rewrite, v = true, r
			}
		case "continuation-token":
			v = []byte(javaTok)
		case "encoding-type":
			if string(v) != "url" && strings.EqualFold(string(v), "url") {
				rewrite, v = true, []byte("url")
			}
		}
		parts = append(parts, p.name+"="+s3.PercentEncode(v))
	}
	if rewrite {
		plan.java = strings.Join(parts, "&")
	}
	if hasStartAfter && len(startAfter.value) == 0 && !hasTok {
		plan.fixes = append(plan.fixes, s3Fix{"empty start-after echoed", func(b []byte) []byte {
			return bytes.Replace(b, []byte("</Prefix>"), []byte("</Prefix><StartAfter></StartAfter>"), 1)
		}})
	}
	if hasEnc && strings.EqualFold(string(enc.value), "url") {
		plan.fixes = append(plan.fixes, s3Fix{"encoding-type=url writes ' ' as '+' and '*' as is", reencodeURL})
		if string(enc.value) != "url" {
			echo := "<EncodingType>" + string(enc.value) + "</EncodingType>"
			plan.fixes = append(plan.fixes, s3Fix{"encoding-type echoed as sent", func(b []byte) []byte {
				return bytes.Replace(b, []byte("<EncodingType>url</EncodingType>"), []byte(echo), 1)
			}})
		}
	}
	return plan
}

func hasParam(params []s3Param, name string) bool {
	return slices.ContainsFunc(params, func(p s3Param) bool { return p.name == name })
}

// replacePart replaces the first '&'-separated part equal to old.
func replacePart(query, old, new string) string {
	parts := strings.Split(query, "&")
	if i := slices.Index(parts, old); i >= 0 {
		parts[i] = new
	}
	return strings.Join(parts, "&")
}

var encodedElement = regexp.MustCompile(`<(Key|Prefix|StartAfter|Delimiter)>([^<]*)</`)

// reencodeURL rewrites the Java server's encoding-type=url values (every
// byte but A-Z a-z 0-9 - _ . / escaped) in live S3's table.
func reencodeURL(b []byte) []byte {
	return encodedElement.ReplaceAllFunc(b, func(m []byte) []byte {
		sub := encodedElement.FindSubmatch(m)
		out := append([]byte("<"), sub[1]...)
		out = append(out, '>')
		out = output.AppendPercentEncoded(out, s3.PercentDecode(string(sub[2])))
		return append(out, "</"...)
	})
}

var s3TokenValue = regexp.MustCompile(`<(Next)?ContinuationToken>[^<]*</`)

func maskTokens(b []byte) []byte {
	return s3TokenValue.ReplaceAll(b, []byte("<${1}ContinuationToken>T</"))
}

// s3Pin is a live answer a named divergence case must give: a refusal, or
// a page holding a probed snippet.
type s3Pin struct {
	refusal  *s3Refusal
	contains string
	// java, when set, replaces the expectation request (a shape the
	// equivalence above cannot express).
	java string
	// mutate alters both servers' tokens before they are sent.
	mutate func(string) string
}

var (
	s3PinsMu sync.Mutex
	s3Pins   = map[string]*s3Pin{}
)

// s3DivergenceCases are the documented divergences as cases of their own,
// each pinned from the live probe, built over the fixture's keys.
func s3DivergenceCases(keys [][]byte) []request {
	if len(keys) == 0 {
		return nil
	}
	dir := firstSegment(keys)
	var out []request
	add := func(name, query, from string, pin *s3Pin) {
		out = append(out, request{Name: name, Method: "GET", Query: "list-type=2&" + query, TokenFrom: from})
		s3PinsMu.Lock()
		s3Pins[name] = pin
		s3PinsMu.Unlock()
	}
	p := s3.PercentEncode([]byte(dir))
	add("s3 live 01 empty start-after", "prefix="+p+"&start-after=&max-keys=2", "", &s3Pin{contains: "<StartAfter></StartAfter>"})
	add("s3 live 02 malformed escape in prefix", "prefix="+p+"a%ZZ&max-keys=2", "", &s3Pin{contains: "<Prefix></Prefix>"})
	add("s3 live 03 first max-keys wins", "prefix="+p+"&max-keys=1&max-keys=2", "", &s3Pin{contains: "<MaxKeys>1</MaxKeys>"})
	add("s3 live 04 max-keys=+2", "prefix="+p+"&max-keys=+2", "", &s3Pin{refusal: &s3Refusal{400, "InvalidArgument", "max-keys must be an integer"}})
	add("s3 live 05 empty continuation-token", "prefix="+p+"&max-keys=2&continuation-token=", "", &s3Pin{refusal: refuseToken})
	add("s3 live 06 flipped token", "max-keys=3", "token page 1", &s3Pin{refusal: refuseToken, mutate: flipLast})
	add("s3 live 06 truncated token", "max-keys=3", "token page 1", &s3Pin{refusal: refuseToken, mutate: func(t string) string { return t[:max(1, len(t)-4)] }})
	if seed, first, ok := flatSeed(keys); ok {
		s := s3.PercentEncode([]byte(seed))
		add("s3 live 07 flat source", "prefix="+s+"&max-keys=1", "", nil)
		add("s3 live 07 flat token with a delimiter", "prefix="+s+"&delimiter=%2F&max-keys=2", "s3 live 07 flat source", &s3Pin{
			contains: "<CommonPrefixes><Prefix>" + xmlText(first) + "</Prefix></CommonPrefixes>",
			java:     "list-type=2&prefix=" + s + "&delimiter=%2F&max-keys=2&continuation-token=" + s3.PercentEncode([]byte(javaFormToken([]byte(seed), true))),
		})
	}
	add("s3 live 08 encoding-type=bogus", "prefix="+p+"&max-keys=2&encoding-type=bogus", "", &s3Pin{refusal: refuseEncoding})
	add("s3 live 08 encoding-type=URL", "prefix="+p+"&max-keys=2&encoding-type=URL", "", &s3Pin{contains: "<EncodingType>URL</EncodingType>"})
	add("s3 live 09 marker", "prefix="+p+"&max-keys=2&marker="+p, "", &s3Pin{refusal: refuseMarker})
	add("s3 live 10 url encoding table", "start-after=a%20b%2Ac&max-keys=1&encoding-type=url", "", &s3Pin{contains: "<StartAfter>a+b*c</StartAfter>"})
	add("s3 live 11 NUL in start-after", "prefix="+p+"&start-after="+p+"%00&max-keys=2", "", &s3Pin{refusal: refuseNUL})
	add("s3 live 12 %FF in start-after", "start-after=%FF&max-keys=2&encoding-type=url", "", &s3Pin{contains: "<StartAfter>%EF%BF%BD</StartAfter>"})
	return out
}

func flipLast(t string) string {
	if strings.HasSuffix(t, "A") {
		return t[:len(t)-1] + "B"
	}
	return t[:len(t)-1] + "A"
}

func xmlText(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;").Replace(s)
}

// flatSeed is a prefix whose "/" listing starts with a common prefix of
// two or more keys followed by another entry, so a flat page of one key
// ends inside that common prefix; first is that common prefix.
func flatSeed(keys [][]byte) (seed, first string, ok bool) {
	for _, p := range append([]string{""}, directoryPrefixes(keys)...) {
		var under [][]byte
		for _, k := range keys {
			if bytes.HasPrefix(k, []byte(p)) {
				under = append(under, k)
			}
		}
		if len(under) < 3 {
			continue
		}
		rest := under[0][len(p):]
		i := bytes.IndexByte(rest, '/')
		if i < 0 {
			continue
		}
		cp := p + string(rest[:i+1])
		if bytes.HasPrefix(under[1], []byte(cp)) && !bytes.HasPrefix(under[len(under)-1], []byte(cp)) {
			return p, cp, true
		}
	}
	return "", "", false
}

// compareS3 sends r under the divergence rules (the comment at the top).
// It returns the Java server's answer to r as sent, which the walks and
// the dependent cases take their tokens from.
func (d *driver) compareS3(r request) (*response, diff, error) {
	if r.Wire != nil {
		// A byte stream of several requests is not rewritten: both servers
		// get it as it is, and the answers are compared with the tokens
		// masked.
		a, err := d.ref.do(d.o.bucket, r)
		if err != nil {
			return nil, diff{}, fmt.Errorf("%s: %w", d.ref.name, err)
		}
		b, err := d.got.do(d.o.bucket, r)
		if err != nil {
			return a, diff{}, fmt.Errorf("%s: %w", d.got.name, err)
		}
		d.count(2)
		df := (bytesComparator{}).compare(masked(a), masked(b))
		if df.OK && !bytes.Equal(a.Body, b.Body) {
			d.noteTokenBytes(r.Name)
		}
		return a, df, nil
	}
	query, setQuery := requestQuery(r)
	plan := planS3(query, r.TokenFrom != "")
	s3PinsMu.Lock()
	pin := s3Pins[r.Name]
	s3PinsMu.Unlock()
	asSentQuery := query
	if pin != nil && pin.mutate != nil {
		params, _ := s3Params(query)
		for _, p := range params {
			if p.name == "continuation-token" {
				javaTok := string(p.value)
				goTok := javaTok
				if b, inc, ok := javaToken(javaTok); ok {
					goTok = s3.EncodeToken(b, inc)
				}
				asSentQuery = replacePart(query, p.raw, "continuation-token="+s3.PercentEncode([]byte(pin.mutate(javaTok))))
				plan.goQuery = replacePart(query, p.raw, "continuation-token="+s3.PercentEncode([]byte(pin.mutate(goTok))))
				plan.java = asSentQuery
				plan.refusal = refuseToken
				plan.reasons = append(plan.reasons, "altered token refused")
			}
		}
	}
	if pin != nil && pin.java != "" {
		plan.java = pin.java
		plan.reasons = append(plan.reasons, "a flat token relists the prefix it ends in")
	}
	exp, err := d.ref.do(d.o.bucket, setQuery(plan.java))
	if err != nil {
		return nil, diff{}, fmt.Errorf("%s: %w", d.ref.name, err)
	}
	got, err := d.got.do(d.o.bucket, setQuery(plan.goQuery))
	if err != nil {
		return exp, diff{}, fmt.Errorf("%s: %w", d.got.name, err)
	}
	d.count(2)
	asSent := exp
	if asSentQuery != plan.java {
		if asSent, err = d.ref.do(d.o.bucket, setQuery(asSentQuery)); err != nil {
			return nil, diff{}, fmt.Errorf("%s: %w", d.ref.name, err)
		}
		d.count(1)
	}
	want := expectedS3(exp, plan, resourceOf(r, d.o.bucket), &plan.reasons)
	df := (bytesComparator{}).compare(masked(want), masked(got))
	if pin != nil && df.OK {
		if msg := pin.check(got); msg != "" {
			df.OK, df.Detail = false, "the live pin does not hold: "+msg
		}
	}
	if df.OK {
		same := (bytesComparator{}).compare(masked(asSent), masked(got)).OK
		switch {
		case !same:
			d.noteDivergence(r.Name, asSent, got, plan.reasons)
		case !bytes.Equal(asSent.Body, got.Body):
			d.noteTokenBytes(r.Name)
		}
	}
	return asSent, df, nil
}

// expectedS3 is the Go answer plan expects, given the Java server's
// answer to the equivalent request: the Java server's own refusal of the
// bucket, method, list-type or max-keys (checked first by both), else the
// planned refusal, else the Java page with the probed rules applied.
func expectedS3(exp *response, plan s3Plan, resource string, reasons *[]string) *response {
	javaFirst := exp.Status == 400 && (bytes.Contains(exp.Body, []byte("<Message>only ListObjectsV2 is supported<")) ||
		bytes.Contains(exp.Body, []byte("<Message>max-keys must be")))
	if javaFirst || exp.Status == 404 || exp.Status == 405 {
		return exp
	}
	if plan.refusal != nil {
		body := s3.AppendError(nil, &s3.Error{Status: plan.refusal.status, Code: plan.refusal.code, Message: plan.refusal.message}, resource)
		return &response{Status: plan.refusal.status, Header: exp.Header, Body: body}
	}
	if exp.Status != 200 {
		return exp
	}
	body := bytes.Clone(exp.Body)
	for _, f := range plan.fixes {
		if next := f.apply(body); !bytes.Equal(next, body) {
			body = next
			*reasons = append(*reasons, f.reason)
		}
	}
	return &response{Status: exp.Status, Header: exp.Header, Body: body}
}

func masked(r *response) *response {
	return &response{Status: r.Status, Header: r.Header, Body: maskTokens(r.Body)}
}

func (p *s3Pin) check(got *response) string {
	if p.refusal != nil {
		want := "<Code>" + p.refusal.code + "</Code><Message>" + xmlText(p.refusal.message) + "</Message>"
		if got.Status != p.refusal.status || !bytes.Contains(got.Body, []byte(want)) {
			return fmt.Sprintf("want %d %s, got %d %.200s", p.refusal.status, want, got.Status, got.Body)
		}
		return ""
	}
	if got.Status != 200 || !bytes.Contains(got.Body, []byte(p.contains)) {
		return fmt.Sprintf("want 200 with %s, got %d %.300s", p.contains, got.Status, got.Body)
	}
	return ""
}

// requestQuery is r's query and a function that returns r with another
// query, for a URL request and for a raw target alike.
func requestQuery(r request) (string, func(string) request) {
	if r.Raw != "" {
		i := strings.IndexByte(r.Raw, '?')
		if i < 0 {
			return "", func(string) request { return r }
		}
		return r.Raw[i+1:], func(q string) request {
			c := r
			c.Raw = r.Raw[:i+1] + q
			return c
		}
	}
	return r.Query, func(q string) request {
		c := r
		c.Query = q
		return c
	}
}

// resourceOf is the path an S3 error names: the request path as sent.
func resourceOf(r request, bucket string) string {
	if r.Raw != "" {
		p, _, _ := strings.Cut(strings.ReplaceAll(r.Raw, "{bucket}", bucket), "?")
		return p
	}
	if r.Path == "" {
		return "/" + bucket
	}
	return strings.ReplaceAll(r.Path, "{bucket}", bucket)
}

// noteDivergence lists a documented divergence with both answers.
func (d *driver) noteDivergence(name string, java, got *response, reasons []string) {
	slices.Sort(reasons)
	reasons = slices.Compact(reasons)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rep.Divergences = append(d.rep.Divergences, fmt.Sprintf("%s: java %s, go %s (as live S3: %s)",
		name, answerSummary(java), answerSummary(got), strings.Join(reasons, "; ")))
}

// noteTokenBytes lists a request whose answers agree apart from token
// bytes, once per name.
func (d *driver) noteTokenBytes(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tokenNames == nil {
		d.tokenNames = map[string]bool{}
	}
	if !d.tokenNames[name] {
		d.tokenNames[name] = true
		d.rep.TokenBytes = append(d.rep.TokenBytes, name)
	}
}

var errorCode = regexp.MustCompile(`<Code>([^<]*)</Code>`)

func answerSummary(r *response) string {
	if m := errorCode.FindSubmatch(r.Body); m != nil && r.Status != 200 {
		return fmt.Sprintf("%d %s", r.Status, m[1])
	}
	return fmt.Sprint(r.Status)
}

// sortedDivergences is the report's divergence list in a stable order.
func sortedDivergences(lines []string) []string {
	set := map[string]bool{}
	for _, l := range lines {
		set[l] = true
	}
	return slices.Sorted(maps.Keys(set))
}
