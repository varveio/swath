// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// The capture mode (-capture FILE) replays a real client's request
// stream: the exchanges bench/faultproxy -record wrote while a client
// (the swath lister) listed a live bucket through it. Every captured
// request is sent, in order, to the Go server serving the recording of
// the same bucket, and its answer is compared with the provider's
// captured one under the live comparators' field policy. A captured
// token is never sent: it is mapped by effect to the token the Go server
// issued at the same point (the answer to the request that issued it),
// as the evidence replays align walks.

// The capture is read and replayed one exchange at a time: what stays
// resident is one exchange, the token map and the report's failures. The
// recorder writes an exchange before the client receives its answer
// (bench/faultproxy record.go), so the answer that issued a token always
// precedes every request that carries it; a token no earlier exchange
// issued is a failure, not a skip.

// A recording of the live matrix (-live-endpoint through the recording
// proxy with -live-record) holds requests a client's walks never send:
// tokens the driver wrote itself or altered from an issued one. Its
// driver annotates each request (recordNote; the proxy writes the
// annotations with the exchange and never forwards them), and the
// replay forms the Go server's request from them: a token the case wrote
// is sent as it was, an altered one is the Go server's token for the
// unaltered one with the same alteration. The answers are judged as the
// live mode judges them (s3Live, gcsLive, documentedDifference), with
// the case's pins (-live-pins).

// The annotation headers the live driver sends through a recording
// proxy (-live-record). Values are query-escaped: a case name or token
// may hold any byte.
const (
	recordCaseHeader  = "X-Record-Case"
	recordTokenHeader = "X-Record-Token"
	recordFromHeader  = "X-Record-Token-From"
	// tokenVerbatim: the request's token is the case's own (a constant,
	// an invented value), issued by no answer; the replay sends it as is.
	tokenVerbatim = "verbatim"
)

// recordNote is what the live driver tells a recording proxy about one
// request: the case it belongs to; how its token was formed: "" for a
// token an earlier answer issued (or none), tokenVerbatim, or
// tokenNote's mutation and the unaltered token it was applied to; and
// from, the case whose walk issued the token, when the request is not
// that walk's own continuation.
type recordNote struct{ caseID, token, from string }

func (n recordNote) headers() [][2]string {
	h := [][2]string{{recordCaseHeader, url.QueryEscape(n.caseID)}}
	if n.token != "" {
		h = append(h, [2]string{recordTokenHeader, n.token})
	}
	if n.from != "" {
		h = append(h, [2]string{recordFromHeader, url.QueryEscape(n.from)})
	}
	return h
}

// tokenNote is the X-Record-Token value of a token made by applying
// mutation (mutateToken) to the issued token source.
func tokenNote(mutation, source string) string {
	return mutation + " " + url.QueryEscape(source)
}

// readRecordNote reads an exchange's annotations.
func readRecordNote(notes []capturedHeader) (recordNote, error) {
	var n recordNote
	for _, h := range notes {
		switch http.CanonicalHeaderKey(h.Name) {
		case recordCaseHeader, recordFromHeader:
			id, err := url.QueryUnescape(h.Value)
			if err != nil {
				return n, fmt.Errorf("annotation %s: %w", h.Name, err)
			}
			if http.CanonicalHeaderKey(h.Name) == recordCaseHeader {
				n.caseID = id
			} else {
				n.from = id
			}
		case recordTokenHeader:
			n.token = h.Value
		}
	}
	return n, nil
}

// noteMutation splits a tokenNote value.
func noteMutation(note string) (mutation, source string, err error) {
	mutation, esc, ok := strings.Cut(note, " ")
	if !ok || mutation != "flip" && mutation != "truncate" && mutation != "flip-decoded" {
		return "", "", fmt.Errorf("annotation %s %q: want verbatim, or flip, truncate or flip-decoded and the token", recordTokenHeader, note)
	}
	if source, err = url.QueryUnescape(esc); err != nil {
		return "", "", fmt.Errorf("annotation %s: %w", recordTokenHeader, err)
	}
	return mutation, source, nil
}

// captureLimits bound what one exchange may hold. A body over a limit is
// reported as a failure of its exchange, never cut and compared.
type captureLimits struct {
	maxBody    int64 // a captured body as recorded (before any content coding is undone)
	maxDecoded int64 // a body once its content coding is undone
	maxTokens  int   // the tokens remembered for the walks still to be replayed
	maxDetails int   // the failures, and apart from them the skips, the report details
}

// maxLine is the longest capture line accepted: the body JSON-escaped
// (at most six bytes for one: \u00XX) and room for the rest of the line.
func (l captureLimits) maxLine() int {
	return int(min(6*l.maxBody+1<<20, 1<<40))
}

// capturedExchange is one line of a faultproxy -record capture.
type capturedExchange struct {
	Seq           uint64           `json:"seq"`
	CapturedAt    string           `json:"captured_at"`
	LimitExceeded string           `json:"limit_exceeded"`
	Annotations   []capturedHeader `json:"annotations"`
	Request       struct {
		Method  string           `json:"method"`
		Path    string           `json:"path"`
		Query   string           `json:"query"`
		Headers []capturedHeader `json:"headers"`
	} `json:"request"`
	Response struct {
		Status     int              `json:"status"`
		Headers    []capturedHeader `json:"headers"`
		Body       *string          `json:"body"`
		BodyBase64 *string          `json:"body_base64"`
	} `json:"response"`
}

type capturedHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// captureItem is one line of a capture: its exchange, or why it could
// not be read.
type captureItem struct {
	line int
	x    *capturedExchange
	err  error
}

// captureLines reads r line by line. A line over maxLine bytes is read
// to its end, reported, and the next line read; bufio.Scanner stops at
// such a line for good, and json.Decoder bounds no value's size.
func captureLines(r io.Reader, maxLine int) iter.Seq[captureItem] {
	return func(yield func(captureItem) bool) {
		br := bufio.NewReaderSize(r, 1<<16)
		var buf []byte
		for n := 1; ; n++ {
			var long bool
			var err error
			buf, long, err = readLine(br, buf[:0], maxLine)
			if err != nil {
				if err != io.EOF {
					yield(captureItem{line: n, err: err})
				}
				return
			}
			var it captureItem
			it.line = n
			switch {
			case long:
				it.err = fmt.Errorf("line %d is over %d bytes (the line limit -capture-max-body sets)", n, maxLine)
			case len(bytes.TrimSpace(buf)) == 0:
				continue
			default:
				var x capturedExchange
				if err := json.Unmarshal(buf, &x); err != nil {
					it.err = fmt.Errorf("line %d: %w", n, err)
				} else {
					it.x = &x
				}
			}
			if !yield(it) {
				return
			}
			if cap(buf) > 1<<20 {
				buf = nil // a long line's buffer is not kept
			}
		}
	}
}

// readLine appends the next line of br (without its '\n') to buf. A line
// over max bytes is consumed and reported as long, its bytes dropped.
// io.EOF is returned only when no line was left.
func readLine(br *bufio.Reader, buf []byte, max int) ([]byte, bool, error) {
	long, read := false, false
	for {
		frag, err := br.ReadSlice('\n')
		read = read || len(frag) > 0
		if !long {
			if len(buf)+len(frag) > max+1 {
				long, buf = true, buf[:0]
			} else {
				buf = append(buf, frag...)
			}
		}
		switch {
		case err == bufio.ErrBufferFull:
			continue
		case err == io.EOF && read:
			return buf, long, nil
		case err != nil:
			return buf, long, err
		}
		return bytes.TrimSuffix(buf, []byte("\n")), long, nil
	}
}

// exchangeItems yields xs as a capture's lines (tests).
func exchangeItems(xs []capturedExchange) iter.Seq[captureItem] {
	return func(yield func(captureItem) bool) {
		for i := range xs {
			if !yield(captureItem{line: i + 1, x: &xs[i]}) {
				return
			}
		}
	}
}

// capturedResponse rebuilds the provider's answer. A body the provider
// sent with a content coding (GCS compresses when the client accepts
// gzip) is decoded and the coding's headers dropped: the comparison is
// of the answer, and the Go server is asked without Accept-Encoding. A
// body over a limit is an error naming it.
func capturedResponse(x *capturedExchange, lim captureLimits) (*response, error) {
	r := &response{Status: x.Response.Status, Header: http.Header{}}
	for _, h := range x.Response.Headers {
		r.Header[http.CanonicalHeaderKey(h.Name)] = append(r.Header[http.CanonicalHeaderKey(h.Name)], h.Value)
	}
	switch {
	case x.Response.Body != nil:
		if n := int64(len(*x.Response.Body)); n > lim.maxBody {
			return nil, fmt.Errorf("the captured body is %d bytes, over the limit of %d (-capture-max-body)", n, lim.maxBody)
		}
		r.Body = []byte(*x.Response.Body)
	case x.Response.BodyBase64 != nil:
		// The limit is of the decoded bytes. DecodedLen is an upper
		// bound that counts padding as data (a one-byte body is four
		// characters), so the body is decoded through a reader that
		// stops one byte past the limit and its actual length checked.
		b, err := io.ReadAll(io.LimitReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(*x.Response.BodyBase64)), lim.maxBody+1))
		if err != nil {
			return nil, err
		}
		if int64(len(b)) > lim.maxBody {
			return nil, fmt.Errorf("the captured body is over %d bytes (-capture-max-body)", lim.maxBody)
		}
		r.Body = b
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(r.Body))
		if err != nil {
			return nil, err
		}
		if r.Body, err = io.ReadAll(io.LimitReader(zr, lim.maxDecoded+1)); err != nil {
			return nil, err
		}
		if int64(len(r.Body)) > lim.maxDecoded {
			return nil, fmt.Errorf("the captured body decodes to over %d bytes (-capture-max-decoded)", lim.maxDecoded)
		}
		r.Header.Del("Content-Encoding")
		r.Header.Del("Content-Length")
	}
	return r, nil
}

// forwardedHeaders are the client's request headers sent on to the Go
// server: all but the connection's own, credentials and the content
// coding the client negotiated.
func forwardedHeaders(hs []capturedHeader) [][2]string {
	var out [][2]string
	for _, h := range hs {
		switch strings.ToLower(h.Name) {
		case "host", "authorization", "accept-encoding", "content-length", "connection", "x-forwarded-for", "x-amz-security-token":
			continue
		}
		out = append(out, [2]string{http.CanonicalHeaderKey(h.Name), h.Value})
	}
	return out
}

// queryToken is the token a raw query carries (decoded) under name, and
// the query with it removed.
func queryToken(query, name string) (token string, rest string, ok bool) {
	var kept []string
	for _, part := range strings.Split(query, "&") {
		n, v, _ := strings.Cut(part, "=")
		if n == name && !ok {
			t, err := url.QueryUnescape(strings.ReplaceAll(v, "+", "%2B"))
			if err != nil {
				t = v
			}
			token, ok = t, true
			continue
		}
		kept = append(kept, part)
	}
	return token, strings.Join(kept, "&"), ok
}

// captureProtocol is the protocol a captured request speaks.
func captureProtocol(path, otherwise string) string {
	if strings.HasPrefix(path, "/storage/v1/b/") {
		return "gcs"
	}
	return otherwise
}

func runCapture(o options) error {
	if o.goBin == "" || o.fixture == "" {
		return fmt.Errorf("-go and -fixture are required with -capture")
	}
	lim := o.captureLimits
	f, err := os.Open(o.capture)
	if err != nil {
		return err
	}
	defer f.Close()
	// The first exchange decides the protocol the Go server is started
	// with (a capture is one client's run); the rest stream after it.
	next, stop := iter.Pull(captureLines(f, lim.maxLine()))
	defer stop()
	first, ok := next()
	if !ok {
		return fmt.Errorf("%s: no exchange", o.capture)
	}
	protocol := o.protocol
	if first.x != nil {
		protocol = captureProtocol(first.x.Request.Path, o.protocol)
	}
	items := func(yield func(captureItem) bool) {
		if !yield(first) {
			return
		}
		for it, ok := next(); ok; it, ok = next() {
			if !yield(it) {
				return
			}
		}
	}
	goFixture := o.fixture
	if o.goFixture != "" {
		goFixture = o.goFixture
	}
	args := serveArgs(goFixture, o.bucket, o.goArgs)
	if protocol == "gcs" {
		args = append(args, "--protocols", "gcs")
	}
	goSrv, err := startServer(context.Background(), "go", o.goBin, args, filepath.Join(o.logDir, "replaydiff-go.log"))
	if err != nil {
		return err
	}
	defer goSrv.stop()
	got := &target{name: "go", base: goSrv.endpoint, client: liveClient(1)}
	rep := &report{Reference: "capture " + o.capture, Fixture: o.fixture, GoFixture: o.goFixture, Bucket: o.bucket, Started: time.Now()}
	d := &driver{o: o, got: got, rep: rep, countPasses: true}
	meta := gcsSynthetic
	if protocol == "gcs" {
		if meta, err = gcsMetadataOf(got, o); err != nil {
			return err
		}
	}
	if o.pins != "" {
		if d.pins, err = readPins(o.pins); err != nil {
			return err
		}
	}
	replayCapture(d, protocol, meta, items, lim)
	rep.Seconds = time.Since(rep.Started).Seconds()
	return d.write()
}

// maxDetail is the longest request path or query a report entry
// quotes: a captured request is bounded only by the line limit, and a
// report of -capture-max-details entries should not hold that many.
const maxDetail = 1 << 10

// clip is s cut to maxDetail bytes, saying how long it was.
func clip(s string) string {
	if len(s) <= maxDetail {
		return s
	}
	return fmt.Sprintf("%s...(%d bytes)", s[:maxDetail], len(s))
}

// capturedToken is what replay knows of a token a captured answer
// issued: the Go server's token for the same point, or the exchange
// whose replay failed to give one; and the issuing request's query
// without its token, and its case (annotated recordings).
type capturedToken struct {
	got           string
	failedAt      uint64
	query, caseID string
}

// tokenMap maps each captured token to the Go server's token for the
// same point. A provider may issue the same token from requests of
// different scope (a GCS token names the object it ends at, whatever the
// query asked), where the Go server's tokens differ (each carries its
// scope and is refused with another), so a token keeps one entry per
// issuing request: its query without the token and its case. n counts
// the entries (-capture-max-tokens).
type tokenMap struct {
	m map[string][]capturedToken
	n int
	// pageParam is the page-size parameter (maxResults, max-keys): a
	// walk may continue with another page size, which both token
	// contracts accept within the same scope.
	pageParam string
}

// grows reports whether setting tok for a request of query and caseID
// adds an entry.
func (tm *tokenMap) grows(tok, query, caseID string) bool {
	for _, t := range tm.m[tok] {
		if t.query == query && t.caseID == caseID {
			return false
		}
	}
	return true
}

func (tm *tokenMap) set(tok string, t capturedToken) {
	list := tm.m[tok]
	for i := range list {
		if list[i].query == t.query && list[i].caseID == t.caseID {
			list[i] = t
			return
		}
	}
	tm.m[tok] = append(list, t)
	tm.n++
}

// errNotIssued: no earlier captured answer issued the token at all.
var errNotIssued = errors.New("not issued")

// lookup is the entry for tok that a request of query, in case caseID,
// sends. A request annotated with its source walk (from) sends that
// walk's token: its entry, or none (the annotation names a walk that
// issued no such token), whatever other walks issued the same provider
// token. Only a request with no source sends its own walk's token: the
// entry an earlier request of the same query issued, else one of the
// same query but its page size (a client may change it mid-walk), the
// request's own case's where several did. Entries that would send
// different Go tokens are never chosen between: the request fails as
// ambiguous.
func (tm *tokenMap) lookup(tok, query, caseID, from string) (capturedToken, error) {
	list := tm.m[tok]
	if len(list) == 0 {
		return capturedToken{}, errNotIssued
	}
	var match []capturedToken
	var what string
	if from != "" {
		what = "the annotated source " + from
		for _, t := range list {
			if t.caseID == from {
				match = append(match, t)
			}
		}
	} else {
		what = "an earlier request of the same query"
		match = issuers(list, query, caseID, func(q string) string { return q })
		if len(match) == 0 && tm.pageParam != "" {
			what = "an earlier request of the same query but its page size"
			scope := func(q string) string { return withoutParam(q, tm.pageParam) }
			match = issuers(list, scope(query), caseID, scope)
		}
	}
	switch {
	case len(match) == 0:
		return capturedToken{}, fmt.Errorf("its token was issued by an earlier captured answer, but not to %s", what)
	case !agree(match):
		return capturedToken{}, fmt.Errorf("its token was issued to %d earlier requests of %s whose Go tokens differ: the source is ambiguous", len(match), what)
	}
	return match[0], nil
}

// issuers are the entries whose query, as key reads it, is query; where
// they disagree, those of caseID when it has any.
func issuers(list []capturedToken, query, caseID string, key func(string) string) []capturedToken {
	var match []capturedToken
	for _, t := range list {
		if key(t.query) == query {
			match = append(match, t)
		}
	}
	if !agree(match) {
		own := slices.DeleteFunc(slices.Clone(match), func(t capturedToken) bool { return t.caseID != caseID })
		if len(own) > 0 {
			match = own
		}
	}
	return match
}

// withoutParam is a raw query with every name parameter removed.
func withoutParam(query, name string) string {
	parts := strings.Split(query, "&")
	parts = slices.DeleteFunc(parts, func(p string) bool { n, _, _ := strings.Cut(p, "="); return n == name })
	return strings.Join(parts, "&")
}

// agree reports whether the entries all send the same Go token (or all
// failed at the same exchange).
func agree(list []capturedToken) bool {
	for _, t := range list[min(1, len(list)):] {
		if t.got != list[0].got || t.failedAt != list[0].failedAt {
			return false
		}
	}
	return true
}

// replayCapture sends each captured request to d.got in order and
// compares the answers.
func replayCapture(d *driver, protocol string, meta gcsMetadata, items iter.Seq[captureItem], lim captureLimits) {
	tokenParam, next, pageParam := "continuation-token", nextToken, "max-keys"
	if protocol == "gcs" {
		tokenParam, next, pageParam = "pageToken", gcsNextToken, "maxResults"
	}
	d.maxDetails = lim.maxDetails
	tokens := &tokenMap{m: map[string][]capturedToken{}, pageParam: pageParam} // a captured token -> the Go server's for the same point
	unread := 0                                                                // exchanges whose captured answer could not be read
	for it := range items {
		if it.err != nil {
			unread++
			d.record(result{Kind: "capture", Name: fmt.Sprintf("capture line %d", it.line), Error: it.err.Error()})
			continue
		}
		x := it.x
		note, noteErr := readRecordNote(x.Annotations)
		name := fmt.Sprintf("capture %d %s %s?%s", x.Seq, x.Request.Method, clip(x.Request.Path), clip(x.Request.Query))
		if note.caseID != "" {
			name = fmt.Sprintf("capture %d [%s] %s %s?%s", x.Seq, clip(note.caseID), x.Request.Method, clip(x.Request.Path), clip(x.Request.Query))
		}
		if noteErr != nil {
			d.record(result{Kind: "capture", Name: name, Error: noteErr.Error()})
			continue
		}
		if x.LimitExceeded != "" {
			unread++
			d.record(result{Kind: "capture", Name: name, Error: "the recording's body limit was exceeded: " + x.LimitExceeded})
			continue
		}
		if x.Response.Status == 0 {
			d.record(result{Kind: "capture", Name: name, Skipped: "the proxy reset the connection: no answer was captured"})
			continue
		}
		if p := captureProtocol(x.Request.Path, "s3"); p != protocol && (p == "gcs" || protocol == "gcs") {
			d.record(result{Kind: "capture", Name: name, Error: fmt.Sprintf("a %s request in a %s capture", p, protocol)})
			continue
		}
		ref, err := capturedResponse(x, lim)
		if err != nil {
			unread++
			d.record(result{Kind: "capture", Name: name, Error: "captured answer: " + err.Error()})
			continue
		}
		issued := "" // the token the captured answer issued
		if ref.Status == 200 {
			issued = next(ref.Body)
		}
		query := x.Request.Query
		refTok, rest, hasTok := queryToken(query, tokenParam)
		if issued != "" && tokens.grows(issued, rest, note.caseID) && tokens.n >= lim.maxTokens {
			d.record(result{Kind: "capture", Name: name, Error: fmt.Sprintf(
				"more than %d tokens issued (-capture-max-tokens): the replay stops here", lim.maxTokens)})
			return
		}
		// failed marks the token this exchange issued as failed at seq:
		// the rest of its walk is skipped with that reason.
		failed := func(seq uint64) {
			if issued != "" {
				tokens.set(issued, capturedToken{failedAt: seq, query: rest, caseID: note.caseID})
			}
		}
		gotTok, mutation, source := "", "", refTok
		origin := originNone // how the request's token was formed, as verified here
		switch {
		case note.token == tokenVerbatim:
			// A token the case wrote itself is one no answer issued: one
			// an earlier answer did issue would be sent to the Go server
			// as the provider's, and judged as a token case's refusal.
			if _, issuedEarlier := tokens.m[refTok]; issuedEarlier && refTok != "" {
				failed(x.Seq)
				d.record(result{Kind: "capture", Name: name, Error: "annotation does not match the captured token: " +
					"the token annotated as the case's own was issued by an earlier captured answer"})
				continue
			}
			gotTok = refTok
			if refTok != "" {
				origin = originVerbatim
			}
		case note.token != "":
			var err error
			if mutation, source, err = noteMutation(note.token); err != nil {
				failed(x.Seq)
				d.record(result{Kind: "capture", Name: name, Error: err.Error()})
				continue
			}
			// The annotation names the alteration and the token it was
			// applied to; the request carries the result. Unless the two
			// agree exactly, the Go token would be altered on the
			// annotation's word alone and its refusal judged against an
			// answer to some other token.
			if m, err := mutateToken(source, mutation); err != nil || !hasTok || m != refTok {
				why := fmt.Sprintf("%s of the annotated token is not the token the request carried", mutation)
				if err != nil {
					why = fmt.Sprintf("%s of the annotated token: %v", mutation, err)
				}
				failed(x.Seq)
				d.record(result{Kind: "capture", Name: name, Error: "annotation does not match the captured token: " + why})
				continue
			}
		}
		if hasTok && refTok != "" && note.token != tokenVerbatim {
			t, err := tokens.lookup(source, rest, note.caseID, note.from)
			switch {
			case err != nil:
				msg := err.Error()
				if errors.Is(err, errNotIssued) {
					msg = "its token was not issued by an earlier captured answer: the capture is out of order or does not start at the walk's first page"
				}
				if unread > 0 {
					msg += fmt.Sprintf(" (%d earlier captured answers could not be read)", unread)
				}
				failed(x.Seq)
				d.record(result{Kind: "capture", Name: name, Error: msg})
				continue
			case t.failedAt != 0:
				failed(t.failedAt)
				d.record(result{Kind: "capture", Name: name, Skipped: fmt.Sprintf("its token was issued by capture %d, whose replay failed", t.failedAt)})
				continue
			}
			gotTok, origin = t.got, originIssued
			if mutation != "" {
				origin = originMutated
				var err error
				if gotTok, err = mutateToken(gotTok, mutation); err != nil {
					failed(x.Seq)
					d.record(result{Kind: "capture", Name: name, Skipped: "go token: " + err.Error()})
					continue
				}
			}
			query = rest + "&" + tokenParam + "=" + campaignEscape(gotTok, "")
			query = strings.TrimPrefix(query, "&")
		}
		path := x.Request.Path
		res := result{Kind: "capture", Name: name, Method: x.Request.Method, Target: clip(path) + "?" + clip(query)}
		b, err := d.got.do(d.o.bucket, request{Method: x.Request.Method, Path: path, Query: query, Header: forwardedHeaders(x.Request.Headers)})
		d.count(1)
		if err != nil {
			failed(x.Seq)
			res.Error = err.Error()
			d.record(res)
			continue
		}
		var df diff
		if protocol == "gcs" {
			df = gcsLive(ref, b, d.o.bucket, gcsFullProjection(query), meta)
		} else {
			df = s3Live(ref, b, d.o.bucket, refTok, gotTok)
		}
		if t := next(b.Body); issued != "" && t != "" {
			tokens.set(issued, capturedToken{got: t, query: rest, caseID: note.caseID})
		} else {
			failed(x.Seq) // the Go server issued no token where the provider did
		}
		if why := documentedDifference(judgement{protocol: protocol, bucket: d.o.bucket, caseID: note.caseID,
			query: rest, mutation: mutation, refTok: refTok, gotTok: gotTok, origin: origin, ref: ref, got: b, df: df}, d.pins); why != "" {
			d.noteDocumented(name, why, df)
			continue
		}
		res.Pass = df.OK
		if !df.OK {
			df.RefExcerpt, df.GotExcerpt = fmt.Sprintf("%d %.300q", ref.Status, ref.Body), fmt.Sprintf("%d %.300q", b.Status, b.Body)
			res.diff = &df
		}
		d.record(res)
	}
}
