// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// The evidence comparison: a port of the conformance tree's
// ProviderExchangeComparator (Azure parts), AzureProfileComparator and
// AzureNamespaceMapping, independent of the route's code. Every failure
// message is spelled as the reference comparator spells it, because the
// archive's pins (replay-expectations.json) are those messages.

// mismatch is a comparison failure; its text is the outcome after
// "mismatch: ".
type mismatch string

func (m mismatch) Error() string { return string(m) }

func fail(format string, args ...any) { panic(mismatch(fmt.Sprintf(format, args...))) }

func require(ok bool, msg string) {
	if !ok {
		panic(mismatch(msg))
	}
}

// show is a value as the reference comparator prints it: nil as null.
func show(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case *string:
		if x == nil {
			return "null"
		}
		return *x
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprint(v)
}

func equal(label string, expected, actual any) {
	if show(expected) != show(actual) || isNil(expected) != isNil(actual) {
		fail("%s differs: expected=%s actual=%s", label, show(expected), show(actual))
	}
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	p, ok := v.(*string)
	return ok && p == nil
}

func sp(s string) *string { return &s }

// catch runs f and returns its mismatch, if any.
func catch(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m, ok := r.(mismatch)
			if !ok {
				panic(r)
			}
			err = m
		}
	}()
	f()
	return nil
}

// exchange is one HTTP answer: lower-case header names, each name once.
type exchange struct {
	status  int
	headers map[string][]string
	body    []byte
}

func newExchange(status int, headers [][2]string, body []byte) exchange {
	h := map[string][]string{}
	seenCase := map[string]string{}
	for _, kv := range headers {
		lower := strings.ToLower(kv[0])
		if first, ok := seenCase[lower]; ok && first != kv[0] {
			fail("duplicate HTTP header name %s", kv[0])
		}
		seenCase[lower] = kv[0]
		h[lower] = append(h[lower], kv[1])
	}
	return exchange{status, h, body}
}

func (e exchange) single(name string) *string {
	v, ok := e.headers[name]
	if !ok {
		return nil
	}
	if len(v) != 1 {
		fail("duplicate HTTP header %s", name)
	}
	return &v[0]
}

var azureHeaders = map[string]bool{"content-type": true, "date": true, "x-ms-request-id": true, "x-ms-version": true,
	"x-ms-error-code": true, "x-ms-client-request-id": true, "content-length": true, "server": true, "cache-control": true,
	"vary": true, "transfer-encoding": true, "x-swath-replay-error": true}

var nonemptyHeaders = []string{"server", "cache-control", "transfer-encoding", "x-swath-replay-error", "x-guploader-uploadid",
	"x-goog-gcs-base-ts", "alt-svc", "pragma"}

var httpDate = regexp.MustCompile(`^[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$`)

func checkHeaders(e exchange, contentType string, head bool) {
	names := make([]string, 0, len(e.headers))
	for n := range e.headers {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		if !azureHeaders[n] {
			fail("unclassified HTTP header %s", n)
		}
	}
	ct := e.single("content-type")
	if ct == nil && !head || ct != nil && strings.TrimSpace(strings.SplitN(strings.ToLower(*ct), ";", 2)[0]) != contentType {
		fail("HTTP Content-Type differs from %s", contentType)
	}
	if l := e.single("content-length"); l != nil {
		n, err := strconv.ParseInt(*l, 10, 64)
		if err != nil {
			fail("HTTP Content-Length is malformed")
		}
		if n < 0 || !head && n != int64(len(e.body)) {
			fail("HTTP Content-Length differs from body")
		}
	}
	for _, n := range []string{"date", "expires"} {
		if d := e.single(n); d != nil && !httpDate.MatchString(*d) {
			fail("HTTP %s spelling is not padded GMT", n)
		}
	}
	for _, id := range []string{"x-goog-request-id", "x-ms-request-id", "x-amz-request-id", "x-amz-id-2"} {
		if _, ok := e.headers[id]; ok && *e.single(id) == "" {
			fail("%s must be nonempty", id)
		}
	}
	for _, n := range nonemptyHeaders {
		if _, ok := e.headers[n]; ok && *e.single(n) == "" {
			fail("%s must be nonempty", n)
		}
	}
	for _, v := range e.headers["vary"] {
		if v == "" {
			fail("vary must be nonempty")
		}
	}
}

func requireSuccessHeaders(e exchange) {
	if e.status != 200 {
		fail("walk page has non-200 HTTP status")
	}
	checkHeaders(e, "application/xml", false)
}

// xmlPreamble is the body up to the end of the XML declaration, BOM
// included.
func xmlPreamble(body []byte) string {
	text := strings.ToValidUTF8(string(body), "\ufffd")
	start := 0
	if strings.HasPrefix(text, "\ufeff") {
		start = len("\ufeff")
	}
	if !strings.HasPrefix(text[start:], "<?xml") {
		fail("Azure XML declaration is missing")
	}
	end := strings.Index(text[start:], "?>")
	if end < 0 {
		fail("Azure XML declaration is incomplete")
	}
	return text[:start+end+2]
}

// assertAzure compares one exchange.
func assertAzure(native, replay exchange, nativeEndpoint, replayEndpoint, nativeContainer, replayContainer string,
	version, clientID, nativeMarker, replayMarker *string, method string) {
	equal("Azure HTTP status", native.status, replay.status)
	head := method == "HEAD"
	checkHeaders(native, "application/xml", head)
	checkHeaders(replay, "application/xml", head)
	if native.status < 400 || native.single("x-ms-version") != nil {
		equal("native x-ms-version", version, native.single("x-ms-version"))
	}
	equal("replay x-ms-version", version, replay.single("x-ms-version"))
	if clientID != nil {
		equal("native x-ms-client-request-id", clientID, native.single("x-ms-client-request-id"))
		equal("replay x-ms-client-request-id", clientID, replay.single("x-ms-client-request-id"))
	}
	if head {
		if native.status < 400 || native.status > 599 || len(native.body) != 0 || len(replay.body) != 0 {
			fail("Azure HEAD error profile requires empty non-success bodies")
		}
		nc, rc := native.single("x-ms-error-code"), replay.single("x-ms-error-code")
		if nc == nil || *nc == "" || rc == nil || *rc == "" {
			fail("Azure HEAD error code header is missing")
		}
		equal("Azure HEAD error code", nc, rc)
		return
	}
	equal("Azure BOM/prolog", xmlPreamble(native.body), xmlPreamble(replay.body))
	if native.status == 200 {
		n, r := parsePage(native.body, false), parsePage(replay.body, true)
		compareParsed(n, r, nativeEndpoint, replayEndpoint, nativeContainer, replayContainer, nativeMarker, replayMarker, true)
		return
	}
	nc, rc := azureError(native.body, true), azureError(replay.body, false)
	equal("Azure error code", nc, rc)
	equal("native x-ms-error-code", nc, native.single("x-ms-error-code"))
	equal("replay x-ms-error-code", rc, replay.single("x-ms-error-code"))
}

var nativeErrorDetails = map[string]bool{"QueryParameterName": true, "QueryParameterValue": true, "HeaderName": true,
	"HeaderValue": true, "Reason": true, "RequestId": true, "Time": true, "AuthenticationErrorDetail": true,
	"MinimumAllowed": true, "MaximumAllowed": true}

func azureError(body []byte, native bool) string {
	root, err := parseXML(body)
	if err != nil {
		fail("invalid Azure error XML")
	}
	if root.name != "Error" {
		fail("invalid Azure error root")
	}
	var code, message *string
	seen := map[string]bool{}
	for _, c := range root.children {
		el := c.elem
		if el == nil {
			continue
		}
		switch {
		case el.name == "Code" && code == nil:
			code = sp(el.textContent())
		case el.name == "Message" && message == nil:
			message = sp(el.textContent())
		case native && nativeErrorDetails[el.name] && !seen[el.name] && len(el.attrs) == 0 && !el.hasElementChild():
			seen[el.name] = true
		default:
			fail("unknown or duplicate Azure error element")
		}
	}
	if code == nil || *code == "" || message == nil {
		fail("invalid Azure error envelope")
	}
	return *code
}

// capturedPage is one page of a walk with the request's own scope.
type capturedPage struct {
	marker, prefix, delimiter, maxResults *string
	exchange                              exchange
}

func assertAzureWalk(native, replay []capturedPage, nativeEndpoint, replayEndpoint, nativeContainer, replayContainer string,
	version *string, maxPages, maxEntries int) {
	var nativePreamble, replayPreamble *string
	for _, side := range []struct {
		label    string
		pages    []capturedPage
		preamble **string
	}{{"native", native, &nativePreamble}, {"replay", replay, &replayPreamble}} {
		for _, p := range side.pages {
			requireSuccessHeaders(p.exchange)
			equal(side.label+" x-ms-version", version, p.exchange.single("x-ms-version"))
			pre := xmlPreamble(p.exchange.body)
			if *side.preamble == nil {
				*side.preamble = &pre
			} else {
				equal(side.label+" XML preamble across pages", *side.preamble, &pre)
			}
		}
	}
	equal("Azure BOM/prolog", nativePreamble, replayPreamble)
	n := aggregate(native, false, maxPages, maxEntries)
	r := aggregate(replay, true, maxPages, maxEntries)
	compareParsed(n, r, nativeEndpoint, replayEndpoint, nativeContainer, replayContainer, nil, nil, false)
}

type entry struct {
	kind, name string
	encoded    *string
	properties map[string]string
}

type page struct {
	endpoint, container string
	echoes              map[string]*string
	entries             []entry
	nextMarker          string
}

func aggregate(pages []capturedPage, replay bool, maxPages, maxEntries int) page {
	if len(pages) == 0 || len(pages) > maxPages || maxEntries < 0 {
		fail("Azure walk page bound violated")
	}
	var entries []entry
	emitted, seen := map[string]bool{}, map[string]bool{}
	var expected *string
	var first *page
	var scopePrefix, scopeDelimiter *string
	for i, c := range pages {
		equal(fmt.Sprintf("Azure request marker at page %d", i), expected, c.marker)
		p := parsePage(c.exchange.body, replay)
		equal(fmt.Sprintf("Azure Marker echo at page %d", i), c.marker, p.echoes["Marker"])
		equal(fmt.Sprintf("Azure Prefix echo at page %d", i), c.prefix, p.echoes["Prefix"])
		equal(fmt.Sprintf("Azure Delimiter echo at page %d", i), c.delimiter, p.echoes["Delimiter"])
		equal(fmt.Sprintf("Azure MaxResults echo at page %d", i), c.maxResults, p.echoes["MaxResults"])
		if first == nil {
			first = &p
			scopePrefix, scopeDelimiter = c.prefix, c.delimiter
		} else {
			equal("Azure endpoint across pages", first.endpoint, p.endpoint)
			equal("Azure container across pages", first.container, p.container)
			equal("Azure Prefix scope across pages", scopePrefix, c.prefix)
			equal("Azure Delimiter scope across pages", scopeDelimiter, c.delimiter)
		}
		for _, e := range p.entries {
			if e.kind == "BlobPrefix" {
				if emitted[e.name] {
					fail("cross-page repeated Azure BlobPrefix is unsupported by replay profile")
				}
				emitted[e.name] = true
			}
		}
		entries = append(entries, p.entries...)
		if len(entries) > maxEntries {
			fail("Azure walk entry bound violated")
		}
		expected = nil
		if p.nextMarker != "" {
			expected = sp(p.nextMarker)
		}
		if i+1 < len(pages) {
			if expected == nil || seen[*expected] {
				fail("Azure walk stopped early or repeated marker")
			}
			seen[*expected] = true
		}
	}
	if expected != nil {
		fail("Azure walk ended with nonterminal marker")
	}
	return page{endpoint: first.endpoint, container: first.container, echoes: map[string]*string{}, entries: entries}
}

var replayProperties = []string{"Last-Modified", "Etag", "Content-Length", "Content-Type", "BlobType", "AccessTier",
	"AccessTierInferred", "LeaseStatus", "LeaseState"}

var nativeOnlyProperties = map[string]bool{"Creation-Time": true, "Content-Encoding": true, "Content-Language": true,
	"Content-MD5": true, "Cache-Control": true, "ServerEncrypted": true, "Content-Disposition": true, "Content-CRC64": true,
	"AccessTierChangeTime": true, "EncryptionScope": true, "EncryptionContext": true, "TagCount": true}

func compareParsed(n, r page, nativeEndpoint, replayEndpoint, nativeContainer, replayContainer string,
	nativeMarker, replayMarker *string, checkEcho bool) {
	equal("native endpoint", nativeEndpoint, n.endpoint)
	equal("replay endpoint", replayEndpoint, r.endpoint)
	equal("native container", nativeContainer, n.container)
	equal("replay container", replayContainer, r.container)
	if checkEcho {
		equal("native marker echo", nativeMarker, n.echoes["Marker"])
		equal("replay marker echo", replayMarker, r.echoes["Marker"])
		for _, f := range []string{"Prefix", "MaxResults", "Delimiter"} {
			equal(f+" echo", n.echoes[f], r.echoes[f])
		}
	}
	equal("entry count", len(n.entries), len(r.entries))
	for i := range n.entries {
		e, a := n.entries[i], r.entries[i]
		equal(fmt.Sprintf("entry kind %d", i), e.kind, a.kind)
		equal(fmt.Sprintf("entry Name text %d", i), e.name, a.name)
		equal(fmt.Sprintf("entry Name Encoded attribute %d", i), e.encoded, a.encoded)
		if a.kind != "Blob" {
			continue
		}
		equal(fmt.Sprintf("Content-Length %d", i), decimalField(e.properties, "Content-Length"), decimalField(a.properties, "Content-Length"))
		equal(fmt.Sprintf("Last-Modified %d", i), lastModified(e.properties), lastModified(a.properties))
		for _, kv := range [][2]string{{"Etag", "0x000000000000001"}, {"Content-Type", "application/octet-stream"},
			{"BlobType", "BlockBlob"}, {"AccessTier", "Hot"}, {"AccessTierInferred", "true"}, {"LeaseStatus", "unlocked"},
			{"LeaseState", "available"}} {
			equal("synthetic "+kv[0], kv[1], optional(a.properties, kv[0]))
		}
		if !slices.Contains([]string{"BlockBlob", "PageBlob", "AppendBlob"}, e.properties["BlobType"]) {
			fail("native BlobType has invalid value")
		}
		if !slices.Contains([]string{"locked", "unlocked"}, e.properties["LeaseStatus"]) {
			fail("native LeaseStatus has invalid value")
		}
		if v, ok := e.properties["AccessTierInferred"]; ok && v != "true" && v != "false" {
			fail("native AccessTierInferred has invalid value")
		}
		for _, s := range replayProperties {
			if v, ok := e.properties[s]; ok && v == "" {
				fail("native %s has wrong empty value", s)
			}
		}
	}
	equal("terminal marker", n.nextMarker == "", r.nextMarker == "")
}

func optional(m map[string]string, k string) *string {
	if v, ok := m[k]; ok {
		return &v
	}
	return nil
}

var decimalText = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

func decimalField(m map[string]string, name string) string {
	v, ok := m[name]
	if !ok || !decimalText.MatchString(v) {
		fail("%s must be decimal", name)
	}
	return v
}

func lastModified(m map[string]string) time.Time {
	v, ok := m["Last-Modified"]
	if !ok {
		fail("Last-Modified missing")
	}
	if !httpDate.MatchString(v) {
		fail("Last-Modified must use padded GMT wire format")
	}
	t, err := time.Parse(time.RFC1123, v)
	if err != nil {
		fail("Last-Modified must use padded GMT wire format")
	}
	return t.UTC()
}

var echoOrder = []string{"Prefix", "Marker", "MaxResults", "Delimiter"}

func parsePage(body []byte, replay bool) page {
	root, err := parseXML(body)
	if err != nil {
		fail("invalid Azure XML")
	}
	if root.name != "EnumerationResults" {
		fail("expected EnumerationResults")
	}
	root.onlyAttributes("ServiceEndpoint", "ContainerName")
	endpoint := root.attribute("ServiceEndpoint")
	if !strings.HasSuffix(endpoint, "/") {
		fail("ServiceEndpoint must have trailing slash")
	}
	p := page{endpoint: endpoint, container: root.attribute("ContainerName"), echoes: map[string]*string{}}
	children := root.elements()
	cursor := 0
	for _, name := range echoOrder {
		if cursor < len(children) && children[cursor].name == name {
			p.echoes[name] = sp(children[cursor].scalar(name))
			cursor++
		}
	}
	if cursor >= len(children) || children[cursor].name != "Blobs" {
		fail("Azure Blobs missing or element order changed")
	}
	blobs := children[cursor]
	cursor++
	if cursor >= len(children) || children[cursor].name != "NextMarker" || cursor+1 != len(children) {
		fail("Azure NextMarker missing or element order changed")
	}
	p.nextMarker = children[cursor].scalar("NextMarker")
	for _, child := range blobs.elements() {
		kind := child.name
		if kind != "Blob" && kind != "BlobPrefix" {
			fail("unknown Azure entry element")
		}
		fields := child.elements()
		if len(fields) == 0 || fields[0].name != "Name" {
			fail("Azure Name missing or out of order")
		}
		name := fields[0]
		name.onlyAttributes("Encoded")
		var encoded *string
		if v, ok := name.attr("Encoded"); ok {
			encoded = &v
			if v != "true" {
				fail("invalid Encoded attribute")
			}
		}
		e := entry{kind: kind, name: name.scalar("Name"), encoded: encoded, properties: map[string]string{}}
		if kind == "BlobPrefix" {
			if len(fields) != 1 {
				fail("flat BlobPrefix has unexpected field")
			}
		} else {
			if len(fields) < 2 || fields[1].name != "Properties" {
				fail("Blob Properties missing or out of order")
			}
			orMetadata := len(fields) == 3 && fields[2].name == "OrMetadata" && len(fields[2].elements()) == 0 &&
				fields[2].textContent() == "" && len(fields[2].attrs) == 0
			if len(fields) > 3 || len(fields) == 3 && (replay || !orMetadata) {
				fail("Blob has an unexpected field after Properties")
			}
			e.properties = properties(fields[1], replay)
		}
		p.entries = append(p.entries, e)
	}
	return p
}

func properties(parent *xnode, replay bool) map[string]string {
	fields := map[string]string{}
	previous := -1
	for _, c := range parent.elements() {
		if !slices.Contains(replayProperties, c.name) && (replay || !nativeOnlyProperties[c.name]) {
			fail("unknown Azure Properties field %s", c.name)
		}
		if _, dup := fields[c.name]; dup {
			fail("duplicate Azure Properties field")
		}
		fields[c.name] = c.scalar(c.name)
		if i := slices.Index(replayProperties, c.name); i >= 0 {
			if i <= previous {
				fail("Azure Properties element order changed")
			}
			previous = i
		}
	}
	if replay {
		if len(fields) != len(replayProperties) {
			fail("Azure replay Properties field set differs")
		}
		for _, n := range replayProperties {
			if _, ok := fields[n]; !ok {
				fail("Azure replay Properties field set differs")
			}
		}
	}
	return fields
}

// xnode is a parsed element: its children are elements, text, CDATA,
// comments and processing instructions, in document order, kept apart
// because the reference comparator's policy tells them apart (a comment
// inside a scalar is an unexpected child; text between elements must be
// blank, CDATA there is ignored).
type xnode struct {
	name     string
	attrs    []xml.Attr
	children []xchild
}

type childKind int

const (
	textNode childKind = iota
	cdataNode
	commentNode
	piNode
)

type xchild struct {
	elem *xnode
	kind childKind
	text string
}

func qname(n xml.Name) string {
	if n.Space == "" {
		return n.Local
	}
	return n.Space + ":" + n.Local
}

// xmlSpace is the XML grammar's white space, the only text allowed
// outside the root element.
func xmlSpace(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return false
		}
	}
	return true
}

// parseXML parses a document as the reference comparator's DOM parser
// does, namespace-unaware: a DOCTYPE, a duplicate attribute, a misplaced
// XML declaration, mismatched tags, a second root or text outside the root
// are refused. A UTF-8 byte order mark is allowed before the declaration.
func parseXML(body []byte) (*xnode, error) {
	body = bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))
	d := xml.NewDecoder(bytes.NewReader(body))
	var stack []*xnode
	var root *xnode
	for first := true; ; first = false {
		offset := d.InputOffset()
		tok, err := d.RawToken()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			seen := map[string]bool{}
			for _, a := range t.Attr {
				if seen[qname(a.Name)] {
					return nil, errors.New("duplicate attribute")
				}
				seen[qname(a.Name)] = true
			}
			n := &xnode{name: qname(t.Name), attrs: t.Attr}
			if len(stack) == 0 {
				if root != nil {
					return nil, errors.New("second root")
				}
				root = n
			} else {
				top := stack[len(stack)-1]
				top.children = append(top.children, xchild{elem: n})
			}
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) == 0 || stack[len(stack)-1].name != qname(t.Name) {
				return nil, errors.New("mismatched end tag")
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			cdata := bytes.HasPrefix(body[offset:], []byte("<![CDATA["))
			if len(stack) == 0 {
				if cdata || !xmlSpace(t) {
					return nil, errors.New("text outside the root")
				}
				continue
			}
			kind := textNode
			if cdata {
				kind = cdataNode
			}
			top := stack[len(stack)-1]
			top.children = append(top.children, xchild{kind: kind, text: string(t)})
		case xml.Comment:
			if len(stack) > 0 {
				top := stack[len(stack)-1]
				top.children = append(top.children, xchild{kind: commentNode, text: string(t)})
			}
		case xml.ProcInst:
			if strings.EqualFold(t.Target, "xml") {
				if !first || offset != 0 {
					return nil, errors.New("misplaced XML declaration")
				}
				continue
			}
			if len(stack) > 0 {
				top := stack[len(stack)-1]
				top.children = append(top.children, xchild{kind: piNode, text: string(t.Inst)})
			}
		case xml.Directive:
			return nil, errors.New("doctype refused")
		}
	}
	if root == nil || len(stack) != 0 {
		return nil, errors.New("incomplete document")
	}
	return root, nil
}

func (n *xnode) attr(name string) (string, bool) {
	for _, a := range n.attrs {
		if qname(a.Name) == name {
			return a.Value, true
		}
	}
	return "", false
}

func (n *xnode) attribute(name string) string {
	v, ok := n.attr(name)
	if !ok {
		fail("missing Azure %s attribute", name)
	}
	return v
}

func (n *xnode) onlyAttributes(allowed ...string) {
	for _, a := range n.attrs {
		if !slices.Contains(allowed, qname(a.Name)) {
			fail("unknown Azure XML attribute")
		}
	}
}

// javaBlank is the reference's blank test: every code point a Java
// whitespace character (the Unicode space, line and paragraph separators
// except the no-break spaces, plus TAB, LF, VT, FF, CR and U+001C..U+001F).
func javaBlank(s string) bool {
	for _, r := range s {
		switch {
		case r >= '\t' && r <= '\r', r >= 0x1c && r <= 0x1f:
		case r == 0xa0 || r == 0x2007 || r == 0x202f:
			return false
		case unicode.In(r, unicode.Zs, unicode.Zl, unicode.Zp):
		default:
			return false
		}
	}
	return true
}

func (n *xnode) elements() []*xnode {
	var out []*xnode
	for _, c := range n.children {
		switch {
		case c.elem != nil:
			out = append(out, c.elem)
		case c.kind == textNode && !javaBlank(c.text):
			fail("unexpected Azure XML text")
		}
	}
	return out
}

func (n *xnode) scalar(label string) string {
	if len(n.attrs) > 0 && label != "Name" {
		fail("%s has unexpected attribute", label)
	}
	var b strings.Builder
	for _, c := range n.children {
		if c.elem != nil || c.kind != textNode && c.kind != cdataNode {
			fail("%s has unexpected child", label)
		}
		b.WriteString(c.text)
	}
	return b.String()
}

func (n *xnode) textContent() string {
	var b strings.Builder
	for _, c := range n.children {
		switch {
		case c.elem != nil:
			b.WriteString(c.elem.textContent())
		case c.kind == textNode || c.kind == cdataNode:
			b.WriteString(c.text)
		}
	}
	return b.String()
}

func (n *xnode) hasElementChild() bool {
	for _, c := range n.children {
		if c.elem != nil {
			return true
		}
	}
	return false
}
