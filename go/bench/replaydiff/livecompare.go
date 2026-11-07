// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The live comparators hold the Go server's answer to the provider's
// under the conformance field policy the native evidence is judged by
// (the conformance suite's ProviderExchangeComparator with its S3 and GCS
// profile comparators, and scripts/provider-conformance/evidence.py's
// normalizations): the same status; on both sides only classified
// headers, the media type, a Content-Length that matches the body, a
// padded GMT Date; then the page field by field or the error's code
// (S3) or first reason (GCS). Request ids, dates and continuation tokens
// are the only values set aside: a token is compared by presence and by
// where it leads (each side follows its own), and the owner's values (the
// replay's are synthetic) by their element sequence alone.

// profileFailure is a comparison's first failure, in the suite's wording.
type profileFailure struct{ msg string }

func pfail(format string, args ...any) { panic(&profileFailure{fmt.Sprintf(format, args...)}) }

// profileCheck runs check, turning its first failure into a diff.
func profileCheck(ref, got *response, check func()) (d diff) {
	d = diff{Status: [2]int{ref.Status, got.Status}, Offset: -1, OK: true}
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(*profileFailure)
			if !ok {
				panic(r)
			}
			d.OK, d.Detail = false, f.msg
		}
	}()
	check()
	return d
}

func pequal(label string, expected, actual any) {
	if fmt.Sprint(expected) != fmt.Sprint(actual) {
		pfail("%s differs: expected=%v actual=%v", label, expected, actual)
	}
}

var (
	s3Classified = set("content-type", "date", "x-amz-request-id", "x-amz-id-2", "x-amz-bucket-region",
		"content-length", "server", "transfer-encoding", "connection", "x-swath-replay-error")
	gcsClassified = set("content-type", "date", "x-goog-request-id", "content-length", "server", "cache-control",
		"vary", "transfer-encoding", "x-swath-replay-error", "x-guploader-uploadid", "x-goog-gcs-base-ts", "expires",
		"alt-svc", "pragma")
	nonemptyClassified = []string{"server", "cache-control", "transfer-encoding", "x-swath-replay-error",
		"x-guploader-uploadid", "x-goog-gcs-base-ts", "alt-svc", "pragma"}
	paddedGMT = regexp.MustCompile(`^[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$`)
)

func set(names ...string) map[string]bool {
	m := map[string]bool{}
	for _, n := range names {
		m[n] = true
	}
	return m
}

// lowerHeaders is a response's headers by lower-case name.
func lowerHeaders(r *response) map[string][]string {
	h := map[string][]string{}
	for k, v := range r.Header {
		h[strings.ToLower(k)] = append(h[strings.ToLower(k)], v...)
	}
	return h
}

func single(h map[string][]string, name string) (string, bool) {
	v, ok := h[name]
	if !ok {
		return "", false
	}
	if len(v) != 1 {
		pfail("duplicate HTTP header %s", name)
	}
	return v[0], true
}

// checkClassified is the suite's checkHeaders for one side.
func checkClassified(side string, r *response, allowed map[string]bool, mediaType string) {
	h := lowerHeaders(r)
	for _, name := range slices.Sorted(maps.Keys(h)) {
		if !allowed[name] {
			pfail("%s: unclassified HTTP header %s", side, name)
		}
	}
	ct, ok := single(h, "content-type")
	if !ok || strings.TrimSpace(strings.SplitN(strings.ToLower(ct), ";", 2)[0]) != mediaType {
		pfail("%s: HTTP Content-Type differs from %s", side, mediaType)
	}
	if l, ok := single(h, "content-length"); ok {
		n, err := strconv.ParseInt(l, 10, 64)
		if err != nil {
			pfail("%s: HTTP Content-Length is malformed", side)
		}
		if n != int64(len(r.Body)) {
			pfail("%s: HTTP Content-Length differs from body", side)
		}
	}
	for _, name := range []string{"date", "expires"} {
		if v, ok := single(h, name); ok {
			if !paddedGMT.MatchString(v) {
				pfail("%s: HTTP %s spelling is not padded GMT", side, name)
			}
			if _, err := time.Parse(time.RFC1123, v); err != nil {
				pfail("%s: HTTP %s is not an RFC 1123 date", side, name)
			}
		}
	}
	for _, name := range append([]string{"x-goog-request-id", "x-amz-request-id", "x-amz-id-2"}, nonemptyClassified...) {
		if v, ok := single(h, name); ok && v == "" {
			pfail("%s: %s must be nonempty", side, name)
		}
	}
	for _, v := range h["vary"] {
		if v == "" {
			pfail("%s: vary must be nonempty", side)
		}
	}
}

// --- S3 ------------------------------------------------------------------

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

var (
	s3TopLevel = set("Name", "Prefix", "Delimiter", "StartAfter", "ContinuationToken", "NextContinuationToken", "KeyCount",
		"MaxKeys", "EncodingType", "IsTruncated", "Marker", "NextMarker", "Contents", "CommonPrefixes")
	s3ExactScalars = []string{"Prefix", "Delimiter", "StartAfter", "EncodingType", "KeyCount", "MaxKeys", "IsTruncated", "Marker"}
	s3Contents     = set("Key", "LastModified", "ETag", "ChecksumAlgorithm", "ChecksumType", "Size", "StorageClass", "Owner", "RestoreStatus")
)

// s3Live compares a live S3 answer with the Go server's. refToken and
// gotToken are the continuation tokens each side's request carried
// (decoded; "" for none): each page must echo its own.
func s3Live(ref, got *response, bucket, refToken, gotToken string) diff {
	return profileCheck(ref, got, func() {
		pequal("S3 HTTP status", ref.Status, got.Status)
		checkClassified("native", ref, s3Classified, "application/xml")
		checkClassified("replay", got, s3Classified, "application/xml")
		if ref.Status == 200 {
			s3AssertPage(ref.Body, got.Body, bucket, refToken, gotToken)
		} else {
			pequal("S3 error code", s3ErrorCode(ref.Body), s3ErrorCode(got.Body))
			pequal("S3 error document", s3ErrorElements(ref.Body), s3ErrorElements(got.Body))
		}
	})
}

type xnode struct {
	name, ns, text string
	children       []*xnode
}

type s3Page struct {
	sequence []string
	scalars  map[string]string
	contents [][]s3Field
	prefixes []string
}

type s3Field struct {
	name, text string
	children   []string
}

func (p *s3Page) scalar(n string) (string, bool) {
	v, ok := p.scalars[n]
	return v, ok
}

func optional(v string, ok bool) string {
	if !ok {
		return "null"
	}
	return v
}

func s3AssertPage(nativeBody, replayBody []byte, bucket, nativeToken, replayToken string) {
	n, r := parseS3Page(nativeBody), parseS3Page(replayBody)
	checkS3Page(n, "native", bucket, nativeToken)
	checkS3Page(r, "replay", bucket, replayToken)
	pequal("top-level element sequence", n.sequence, r.sequence)
	scalars := s3ExactScalars
	if _, v1 := n.scalar("Marker"); v1 {
		// ListObjects (V1): NextMarker is a key, not an opaque token.
		scalars = append(slices.Clone(scalars), "NextMarker")
	}
	for _, s := range scalars {
		pequal(s, optional(n.scalar(s)), optional(r.scalar(s)))
	}
	_, a := n.scalar("NextContinuationToken")
	_, b := r.scalar("NextContinuationToken")
	pequal("NextContinuationToken presence", a, b)
	s3CompareContents(n.contents, r.contents)
	pequal("CommonPrefixes", n.prefixes, r.prefixes)
	// The values above are compared decoded; the key-carrying elements
	// are also compared as written, so a control character written raw
	// where S3 writes a reference (&#x1;) differs.
	pequal("key text as written", s3KeyText(nativeBody), s3KeyText(replayBody))
}

var s3KeyElement = regexp.MustCompile(`<(Key|Prefix|StartAfter|Delimiter)>([^<]*)</`)

func s3KeyText(body []byte) []string {
	var out []string
	for _, m := range s3KeyElement.FindAllSubmatch(body, -1) {
		out = append(out, string(m[1])+"="+string(m[2]))
	}
	return out
}

var s3ErrorElement = regexp.MustCompile(`<([A-Za-z]+)>([^<]*)</[A-Za-z]+>`)

// s3ErrorElements is an S3 error document's elements in order, each
// name=text as written; RequestId and HostId, which S3 varies per
// request, are compared by presence only.
func s3ErrorElements(body []byte) []string {
	var out []string
	for _, m := range s3ErrorElement.FindAllSubmatch(body, -1) {
		name, text := string(m[1]), string(m[2])
		if name == "RequestId" || name == "HostId" {
			text = "(volatile)"
		}
		out = append(out, name+"="+text)
	}
	return out
}

func checkS3Page(p *s3Page, side, bucket, requestToken string) {
	pequal(side+" Name", bucket, optional(p.scalar("Name")))
	truncated, _ := p.scalar("IsTruncated")
	if truncated != "true" && truncated != "false" {
		pfail("%s IsTruncated is not a boolean", side)
	}
	if _, v1 := p.scalar("Marker"); !v1 {
		_, next := p.scalar("NextContinuationToken")
		if next != (truncated == "true") {
			pfail("%s IsTruncated and NextContinuationToken disagree", side)
		}
	}
	want := "null"
	if requestToken != "" {
		want = requestToken
	}
	pequal(side+" ContinuationToken echo", want, optional(p.scalar("ContinuationToken")))
}

func s3CompareContents(native, replay [][]s3Field) {
	pequal("Contents count", len(native), len(replay))
	for i := range native {
		nf := slices.DeleteFunc(slices.Clone(native[i]), func(f s3Field) bool { return f.name == "RestoreStatus" })
		rf := replay[i]
		for _, f := range rf {
			if f.name == "RestoreStatus" {
				pfail("replay contains profile-omitted RestoreStatus")
			}
		}
		names := func(fs []s3Field) []string {
			var out []string
			for _, f := range fs {
				out = append(out, f.name)
			}
			return out
		}
		pequal(fmt.Sprintf("Contents %d field sequence", i), names(nf), names(rf))
		for j := range nf {
			e, a := nf[j], rf[j]
			label := fmt.Sprintf("Contents %d %s", i, e.name)
			switch e.name {
			case "LastModified":
				pequal(label, instantOf(e.text, "LastModified"), instantOf(a.text, "LastModified"))
			case "Owner":
				if !slices.Equal(e.children, []string{"ID"}) && !slices.Equal(e.children, []string{"ID", "DisplayName"}) {
					pfail("%s children are not an S3 Owner: %v", label, e.children)
				}
				pequal(label+" replay children", e.children, a.children)
			default:
				pequal(label, e.text, a.text)
			}
		}
	}
}

func instantOf(v, name string) string {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		pfail("%s must be an RFC 3339 instant", name)
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseS3Page(body []byte) *s3Page {
	root := parseLenientXML(body)
	if root.name != "ListBucketResult" {
		pfail("S3 listing root is %s", root.name)
	}
	if root.ns != s3Namespace {
		pfail("S3 listing namespace differs")
	}
	p := &s3Page{scalars: map[string]string{}}
	for _, c := range root.children {
		if !s3TopLevel[c.name] {
			pfail("unknown S3 listing element %s", c.name)
		}
		p.sequence = append(p.sequence, c.name)
		switch c.name {
		case "Contents":
			var fields []s3Field
			for _, f := range c.children {
				if !s3Contents[f.name] {
					pfail("unknown S3 Contents element %s", f.name)
				}
				var kids []string
				for _, k := range f.children {
					if f.name != "Owner" && f.name != "RestoreStatus" {
						pfail("%s must be a text element", f.name)
					}
					if f.name == "Owner" && k.name != "ID" && k.name != "DisplayName" {
						pfail("unknown S3 Owner element %s", k.name)
					}
					kids = append(kids, k.name)
				}
				fields = append(fields, s3Field{f.name, f.text, kids})
			}
			p.contents = append(p.contents, fields)
		case "CommonPrefixes":
			if len(c.children) != 1 || c.children[0].name != "Prefix" {
				pfail("CommonPrefixes must hold exactly one Prefix")
			}
			p.prefixes = append(p.prefixes, c.children[0].text)
		default:
			if len(c.children) > 0 {
				pfail("%s must be a text element", c.name)
			}
			if _, dup := p.scalars[c.name]; dup {
				pfail("duplicate S3 listing element %s", c.name)
			}
			p.scalars[c.name] = c.text
		}
	}
	return p
}

func s3ErrorCode(body []byte) string {
	root := parseLenientXML(body)
	if root.name != "Error" {
		pfail("S3 error body has root %s", root.name)
	}
	code := ""
	seen := false
	for _, c := range root.children {
		if c.name == "Code" {
			if seen {
				pfail("S3 error has duplicate Code")
			}
			code, seen = c.text, true
		}
	}
	if code == "" {
		pfail("S3 error lacks Code")
	}
	return code
}

// parseLenientXML reads an S3 body the way the suite does: S3 returns
// keys XML 1.0 cannot carry (U+0001) when encoding-type is absent, so a
// small tokenizer reads tag names, the root's namespace declaration, the
// five entities and numeric references, and keeps text exactly.
func parseLenientXML(body []byte) *xnode {
	if !utf8.Valid(body) {
		pfail("S3 body is not valid UTF-8")
	}
	s := strings.TrimPrefix(string(body), "\ufeff")
	pos := 0
	skip := func() {
		for pos < len(s) && strings.IndexByte(" \t\r\n", s[pos]) >= 0 {
			pos++
		}
	}
	skip()
	if strings.HasPrefix(s[pos:], "<?xml") {
		end := strings.Index(s[pos:], "?>")
		if end < 0 {
			pfail("S3 XML declaration is incomplete")
		}
		pos += end + 2
	}
	skip()
	var element func(root bool) *xnode
	element = func(root bool) *xnode {
		if !strings.HasPrefix(s[pos:], "<") || strings.HasPrefix(s[pos:], "</") {
			pfail("expected an S3 element start tag")
		}
		end := strings.IndexByte(s[pos:], '>')
		if end < 0 {
			pfail("unterminated S3 start tag")
		}
		tag := s[pos+1 : pos+end]
		pos += end + 1
		empty := strings.HasSuffix(tag, "/")
		tag = strings.TrimSuffix(tag, "/")
		parts := strings.Fields(tag)
		if len(parts) == 0 || !validXMLName(parts[0]) {
			pfail("unsupported S3 element name %s", tag)
		}
		n := &xnode{name: parts[0]}
		if len(parts) > 1 {
			attr := strings.Join(parts[1:], " ")
			v, ok := strings.CutPrefix(attr, `xmlns="`)
			if !root || !ok || !strings.HasSuffix(v, `"`) || len(parts) != 2 {
				pfail("unexpected S3 attribute on %s", n.name)
			}
			n.ns = strings.TrimSuffix(v, `"`)
		}
		if empty {
			return n
		}
		var raw strings.Builder
		for {
			next := strings.IndexByte(s[pos:], '<')
			if next < 0 {
				pfail("unterminated S3 element %s", n.name)
			}
			raw.WriteString(s[pos : pos+next])
			pos += next
			if strings.HasPrefix(s[pos:], "</") {
				end := strings.IndexByte(s[pos:], '>')
				if end < 0 || strings.TrimSpace(s[pos+2:pos+end]) != n.name {
					pfail("mismatched S3 end tag for %s", n.name)
				}
				pos += end + 1
				break
			}
			n.children = append(n.children, element(false))
		}
		text := decodeEntities(raw.String())
		if len(n.children) > 0 && strings.TrimSpace(text) != "" {
			pfail("mixed text in S3 element %s", n.name)
		}
		if len(n.children) == 0 {
			n.text = text
		}
		return n
	}
	root := element(true)
	skip()
	if pos != len(s) {
		pfail("trailing content after the S3 root element")
	}
	return root
}

func validXMLName(n string) bool {
	for i, c := range n {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return n != ""
}

func decodeEntities(raw string) string {
	var out strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] != '&' {
			out.WriteByte(raw[i])
			continue
		}
		end := strings.IndexByte(raw[i:], ';')
		if end < 0 {
			pfail("unterminated S3 entity")
		}
		ent := raw[i+1 : i+end]
		switch ent {
		case "lt":
			out.WriteByte('<')
		case "gt":
			out.WriteByte('>')
		case "amp":
			out.WriteByte('&')
		case "quot":
			out.WriteByte('"')
		case "apos":
			out.WriteByte('\'')
		default:
			var cp int64 = -1
			if h, ok := strings.CutPrefix(ent, "#x"); ok {
				cp, _ = strconv.ParseInt(h, 16, 32)
			} else if d, ok := strings.CutPrefix(ent, "#"); ok {
				cp, _ = strconv.ParseInt(d, 10, 32)
			}
			if cp < 0 || cp > utf8.MaxRune || cp >= 0xD800 && cp <= 0xDFFF {
				pfail("unknown S3 entity %s", ent)
			}
			out.WriteRune(rune(cp))
		}
		i += end
	}
	return out.String()
}

// --- GCS -----------------------------------------------------------------

// gcsMetadata is how the replay's item metadata is held to the
// provider's: native when the fixture is a swath/gcs-objects/v1 dataset
// (every stored field exact), synthetic otherwise (the wire contract's
// fixed values, the provider's checked for shape only).
type gcsMetadata int

const (
	gcsSynthetic gcsMetadata = iota
	gcsNative
)

var (
	gcsPageFields   = set("kind", "items", "prefixes", "nextPageToken")
	gcsReplayFields = set("kind", "bucket", "name", "size", "updated", "etag", "generation", "metageneration",
		"storageClass", "contentType", "acl")
	gcsNativeOnly = set("id", "selfLink", "mediaLink", "timeCreated", "md5Hash", "crc32c", "owner", "metadata",
		"timeStorageClassUpdated", "componentCount", "cacheControl", "contentEncoding", "contentDisposition",
		"contentLanguage", "kmsKeyName", "customerEncryption", "temporaryHold", "eventBasedHold",
		"retentionExpirationTime", "customTime", "softDeleteTime", "hardDeleteTime", "timeFinalized")
	gcsSixDigits = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$`)
	gcsDecimal   = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
)

// gcsLive compares a live objects.list answer with the Go server's.
func gcsLive(ref, got *response, bucket string, full bool, meta gcsMetadata) diff {
	return profileCheck(ref, got, func() {
		pequal("GCS HTTP status", ref.Status, got.Status)
		checkClassified("native", ref, gcsClassified, "application/json")
		checkClassified("replay", got, gcsClassified, "application/json")
		if ref.Status == 200 {
			gcsAssertPage(ref.Body, got.Body, bucket, full, meta)
		} else {
			pequal("GCS error reason", gcsErrorReason(ref), gcsErrorReason(got))
		}
	})
}

// jsonObject decodes an object keeping its field order and refusing
// duplicate names and trailing data.
type jsonObject struct {
	keys   []string
	fields map[string]json.RawMessage
}

func readObject(b []byte, what string) *jsonObject {
	dec := json.NewDecoder(bytes.NewReader(b))
	o, err := decodeObject(dec)
	if err != nil {
		pfail("%s: %v", what, err)
	}
	// Only whitespace may follow the object: another value is a token
	// and anything else a syntax error, and both fail.
	if t, err := dec.Token(); err != io.EOF {
		pfail("%s: trailing data after the object (%v %v)", what, t, err)
	}
	return o
}

func decodeObject(dec *json.Decoder) (*jsonObject, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if t != json.Delim('{') {
		return nil, fmt.Errorf("not an object")
	}
	o := &jsonObject{fields: map[string]json.RawMessage{}}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name := k.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		if _, dup := o.fields[name]; dup {
			return nil, fmt.Errorf("duplicate JSON field %q", name)
		}
		o.keys = append(o.keys, name)
		o.fields[name] = v
	}
	_, err = dec.Token()
	return o, err
}

func (o *jsonObject) has(n string) bool { _, ok := o.fields[n]; return ok }

func (o *jsonObject) text(n string) string {
	var s string
	if err := json.Unmarshal(o.fields[n], &s); err != nil || !o.has(n) {
		pfail("%s must be text", n)
	}
	return s
}

func (o *jsonObject) decimal(n string) string {
	v := o.text(n)
	if !gcsDecimal.MatchString(v) {
		pfail("%s must be a decimal string", n)
	}
	return v
}

func (o *jsonObject) optionalText(n string) string {
	if !o.has(n) {
		return "null"
	}
	return o.text(n)
}

func (o *jsonObject) onlyFields(allowed map[string]bool, label string) {
	for _, k := range o.keys {
		if !allowed[k] {
			pfail("%s has unknown field %s", label, k)
		}
	}
}

func (o *jsonObject) array(n string) []json.RawMessage {
	if !o.has(n) {
		return nil
	}
	var a []json.RawMessage
	if err := json.Unmarshal(o.fields[n], &a); err != nil {
		pfail("GCS %s must be an array", n)
	}
	return a
}

type gcsPageView struct {
	items    []*jsonObject
	prefixes []string
	token    *string
}

func readGCSPage(body []byte, replay bool) gcsPageView {
	label := "native page"
	if replay {
		label = "replay page"
	}
	p := readObject(body, label)
	p.onlyFields(gcsPageFields, label)
	pequal("page kind", "storage#objects", p.text("kind"))
	var v gcsPageView
	for _, raw := range p.array("items") {
		v.items = append(v.items, readObject(raw, "GCS item"))
	}
	if replay && p.has("items") && len(v.items) == 0 {
		pfail("replay must omit empty items array")
	}
	seen := map[string]bool{}
	pre := p.array("prefixes")
	if replay && p.has("prefixes") && len(pre) == 0 {
		pfail("replay must omit empty prefixes array")
	}
	for _, raw := range pre {
		var s string
		if json.Unmarshal(raw, &s) != nil || seen[s] {
			pfail("invalid or duplicate GCS prefix")
		}
		seen[s] = true
		v.prefixes = append(v.prefixes, s)
	}
	if p.has("nextPageToken") {
		t := p.text("nextPageToken")
		if t == "" {
			pfail("invalid GCS nextPageToken")
		}
		v.token = &t
	}
	return v
}

func gcsAssertPage(nativeBody, replayBody []byte, bucket string, full bool, meta gcsMetadata) {
	n, r := readGCSPage(nativeBody, false), readGCSPage(replayBody, true)
	pequal("item count", len(n.items), len(r.items))
	replayFields := gcsReplayFields
	if meta == gcsNative {
		replayFields = maps.Clone(gcsReplayFields)
		replayFields["md5Hash"], replayFields["crc32c"], replayFields["timeCreated"] = true, true, true
	}
	for i := range n.items {
		ni, ri := n.items[i], r.items[i]
		all := maps.Clone(gcsReplayFields)
		maps.Copy(all, gcsNativeOnly)
		ni.onlyFields(all, "native item")
		ri.onlyFields(replayFields, "replay item")
		is := strconv.Itoa(i)
		pequal("item kind "+is, "storage#object", ni.text("kind"))
		pequal("replay item kind "+is, "storage#object", ri.text("kind"))
		pequal("native bucket "+is, bucket, ni.text("bucket"))
		pequal("replay bucket "+is, bucket, ri.text("bucket"))
		pequal("name "+is, ni.text("name"), ri.text("name"))
		pequal("size "+is, ni.decimal("size"), ri.decimal("size"))
		if !gcsSixDigits.MatchString(ri.text("updated")) {
			pfail("replay updated must use six UTC fractional digits")
		}
		pequal("updated "+is, instantOf(ni.text("updated"), "updated"), instantOf(ri.text("updated"), "updated"))
		if ni.text("etag") == "" || ri.text("etag") == "" {
			pfail("etag must be nonempty")
		}
		ni.decimal("generation")
		ni.decimal("metageneration")
		if meta == gcsNative {
			pequal("etag "+is, ni.text("etag"), ri.text("etag"))
			pequal("generation "+is, ni.decimal("generation"), ri.decimal("generation"))
			pequal("metageneration "+is, ni.decimal("metageneration"), ri.decimal("metageneration"))
			for _, f := range []string{"storageClass", "contentType", "md5Hash", "crc32c"} {
				pequal(f+" "+is, ni.optionalText(f), ri.optionalText(f))
			}
			// timeCreated is listed where the recording stored it (a part
			// written before the lister recorded it holds none).
			if ri.has("timeCreated") {
				if !gcsSixDigits.MatchString(ri.text("timeCreated")) {
					pfail("replay timeCreated must use six UTC fractional digits")
				}
				pequal("timeCreated "+is, instantOf(ni.text("timeCreated"), "timeCreated"), instantOf(ri.text("timeCreated"), "timeCreated"))
			}
		} else {
			pequal("generation "+is, "1", ri.decimal("generation"))
			pequal("metageneration "+is, "1", ri.decimal("metageneration"))
			pequal("storageClass "+is, "STANDARD", ri.text("storageClass"))
			pequal("contentType "+is, "application/octet-stream", ri.text("contentType"))
			ni.optionalText("storageClass")
			ni.optionalText("contentType")
			if ri.has("crc32c") {
				pfail("replay contains profile-omitted native metadata")
			}
		}
		if full {
			if acl := ri.array("acl"); acl == nil || len(acl) != 0 {
				pfail("replay full ACL must be an empty array")
			}
		} else if ri.has("acl") {
			pfail("replay noAcl projection must omit acl")
		}
		if meta != gcsNative && ri.has("timeCreated") || ri.has("owner") {
			pfail("replay contains profile-omitted native metadata")
		}
	}
	pequal("prefixes", n.prefixes, r.prefixes)
	pequal("terminal pagination", n.token == nil, r.token == nil)
}

func gcsErrorReason(r *response) string {
	root := readObject(r.Body, "GCS error")
	if len(root.keys) != 1 || !root.has("error") {
		pfail("unknown GCS error envelope field")
	}
	er := readObject(root.fields["error"], "GCS error")
	for _, k := range er.keys {
		if k != "code" && k != "message" && k != "errors" {
			pfail("unknown GCS error field %s", k)
		}
	}
	var code int
	if json.Unmarshal(er.fields["code"], &code) != nil || code != r.Status {
		pfail("invalid GCS error envelope")
	}
	er.text("message")
	errs := er.array("errors")
	if len(errs) == 0 {
		pfail("GCS error reason missing")
	}
	var reason string
	for i, raw := range errs {
		d := readObject(raw, "GCS error detail")
		d.onlyFields(set("domain", "reason", "message", "location", "locationType"), "GCS error detail")
		if i == 0 {
			reason = d.text("reason")
		}
	}
	return reason
}

// gcsFullProjection reports whether a query asks for projection=full.
func gcsFullProjection(query string) bool {
	for _, part := range strings.Split(query, "&") {
		if part == "projection=full" {
			return true
		}
	}
	return false
}

// --- Documented differences ----------------------------------------------

// judgement is one request and the two answers the live comparators
// held to each other, with what is known of how the request was formed:
// what a documented difference is matched against. The live modes judge
// the provider's answer as it arrives; the capture mode judges the one a
// recording holds, by the same rules.
type judgement struct {
	protocol, bucket string
	caseID           string // the matrix case, when known
	query            string // the query the provider was sent
	mutation         string // the alteration of an issued token the request carries, if any
	refTok, gotTok   string // the tokens each side was sent (decoded)
	ref, got         *response
	df               diff
}

// documentedDifference reports why a failed comparison is a difference
// the replay is known to keep, or "" when it is not one. Each rule
// matches the request's shape and both answers' shapes, never the case
// name alone, so the same difference in another request is documented
// too and any other answer to the same request still fails:
//
//   - a mismatch the provider evidence pins for the case (pins, from
//     -live-pins), message for message;
//   - S3 ListObjects V1 (list-type absent or not 2): the provider lists,
//     the replay refuses list-type (it serves ListObjectsV2 only);
//   - S3 fetch-owner=true where the provider writes Owner and the replay
//     writes none (a recording made without owners), the pages otherwise
//     agreeing;
//   - S3, an issued token with a decoded byte flipped (s3-07x): the
//     provider serves its own token so altered as if unaltered, the
//     replay refuses every altered token;
//   - GCS softDeleted=true (gcs-11h): a provider bucket that keeps
//     soft-deleted objects lists them, the replay refuses the parameter.
func documentedDifference(j judgement, pins map[string]string) string {
	if j.df.OK {
		return ""
	}
	if p := pins[j.caseID]; p != "" && p == j.df.Detail {
		return "the provider evidence pins this mismatch for " + j.caseID + ": " + p
	}
	if j.protocol == "gcs" {
		if v, ok := firstParam(j.query, "softDeleted"); ok && v == "true" && j.ref.Status == 200 &&
			j.got.Status == 400 && gcsReasonIs(j.got, "invalid") {
			return "softDeleted=true: the provider lists the soft-deleted objects its bucket keeps; the replay holds none and refuses the parameter"
		}
		return ""
	}
	if j.ref.Status != 200 || !bytes.Contains(j.ref.Body, []byte("<ListBucketResult")) {
		return ""
	}
	if lt, ok := firstParam(j.query, "list-type"); (!ok || lt != "2") && bytes.Contains(j.ref.Body, []byte("<Marker>")) &&
		s3Refused(j.got, "list-type") {
		return "ListObjects V1: the provider lists; the replay serves ListObjectsV2 only and refuses list-type"
	}
	if v, ok := firstParam(j.query, "fetch-owner"); ok && v == "true" && j.got.Status == 200 &&
		bytes.Contains(j.ref.Body, []byte("<Owner>")) && !bytes.Contains(j.got.Body, []byte("<Owner>")) {
		body := dropOwners(j.ref.Body)
		h := j.ref.Header.Clone()
		if h.Get("Content-Length") != "" {
			h.Set("Content-Length", strconv.Itoa(len(body)))
		}
		if s3Live(&response{Status: j.ref.Status, Header: h, Body: body}, j.got, j.bucket, j.refTok, j.gotTok).OK {
			return "fetch-owner=true over a recording made without owners: the provider writes Owner, the replay has none to write"
		}
	}
	if j.mutation == "flip-decoded" && s3Refused(j.got, "continuation-token") {
		return "an issued token with a decoded byte flipped: the provider serves its own token so altered as if unaltered; the replay refuses every altered token (its token carries a checksum)"
	}
	return ""
}

// firstParam is the first value of the query parameter name as the
// provider reads a raw query (s3Params: malformed escapes dropped, '+' a
// space).
func firstParam(query, name string) (string, bool) {
	params, _ := s3Params(query)
	for _, p := range params {
		if p.name == name {
			return string(p.value), true
		}
	}
	return "", false
}

var s3ArgumentName = regexp.MustCompile(`<ArgumentName>([^<]*)</ArgumentName>`)

// s3Refused reports whether r is S3's 400 InvalidArgument naming argument.
func s3Refused(r *response, argument string) bool {
	c, a := errorCode.FindSubmatch(r.Body), s3ArgumentName.FindSubmatch(r.Body)
	return r.Status == 400 && c != nil && string(c[1]) == "InvalidArgument" && a != nil && string(a[1]) == argument
}

// gcsReasonIs reports whether r is a GCS error whose first reason is
// reason.
func gcsReasonIs(r *response, reason string) bool {
	var got string
	return profileCheck(r, r, func() { got = gcsErrorReason(r) }).OK && got == reason
}
