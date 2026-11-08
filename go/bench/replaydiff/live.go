// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/varveio/swath/go/internal/replay/s3"
)

// The live modes (-live-endpoint): the reference is the provider itself,
// anonymous. The Go server serves a recording of the same bucket; the
// requests are the provider conformance matrix (providermatrix.go) bound
// to names under -live-prefix, then token walks of the scope and its
// directories, each side following its own tokens; the answers are
// compared by the conformance field policy (livecompare.go).

// tamperedToken is the campaign's malformed constant token.
const tamperedToken = "CONFORMANCE-TAMPERED-TOKEN"

// liveRun is one live comparison.
type liveRun struct {
	d        *driver
	protocol string // s3 or gcs
	scope    string
	roles    *fixtureRoles
	meta     gcsMetadata
	mu       sync.Mutex
	// tokens holds each walk case's next tokens per page, per side.
	tokens map[string][2][]string
	// pins are the provider evidence's pinned mismatches by case id.
	pins map[string]pin
}

func liveClient(parallel int) *http.Client {
	return &http.Client{Timeout: 5 * time.Minute, Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment, MaxIdleConnsPerHost: parallel * 2, ForceAttemptHTTP2: true,
		// The campaign's probes sent no Accept-Encoding: bodies and
		// Content-Length are compared as the provider sends them.
		DisableCompression: true,
	}}
}

func runLive(o options) error {
	if o.goBin == "" || o.fixture == "" {
		return fmt.Errorf("-go and -fixture are required with -live-endpoint")
	}
	if o.protocol != "s3" && o.protocol != "gcs" {
		return fmt.Errorf("-live-endpoint compares -protocol s3 or gcs")
	}
	client := liveClient(o.parallel)
	goFixture := o.fixture
	if o.goFixture != "" {
		goFixture = o.goFixture
	}
	args := serveArgs(goFixture, o.bucket, o.goArgs)
	if o.protocol == "gcs" {
		args = append(args, "--protocols", "gcs")
	}
	goSrv, err := startServer(context.Background(), "go", o.goBin, args, filepath.Join(o.logDir, "replaydiff-go.log"))
	if err != nil {
		return err
	}
	defer goSrv.stop()
	got := &target{name: "go", base: goSrv.endpoint, client: client}
	base := strings.TrimSuffix(o.live, "/")
	u, err := url.Parse(base)
	if err != nil {
		return err
	}
	ref := &target{name: "live", base: base, client: client, recorder: o.liveRecord,
		vhost: o.protocol == "s3" && strings.HasPrefix(u.Host, o.bucket+".")}

	keys, newest, err := listScope(got, o.protocol, o.bucket, o.livePrefix)
	if err != nil {
		return fmt.Errorf("listing the fixture under %q: %w", o.livePrefix, err)
	}
	if len(keys) == 0 {
		return fmt.Errorf("the fixture holds no name under %q", o.livePrefix)
	}
	if o.stableBefore != "" {
		t, err := time.Parse(time.RFC3339, o.stableBefore)
		if err != nil {
			return fmt.Errorf("-live-stable-before: %w", err)
		}
		if !newest.Before(t) {
			return fmt.Errorf("the newest name under %q was modified %s, not before %s: the live bucket may have changed there",
				o.livePrefix, newest.Format(time.RFC3339), o.stableBefore)
		}
	}
	rep := &report{Reference: "live " + base, Fixture: o.fixture, GoFixture: o.goFixture, Bucket: o.bucket, Seed: o.seed,
		Started: time.Now(), Keys: len(keys), Scope: o.livePrefix, Newest: newest.UTC().Format(time.RFC3339)}
	lr := &liveRun{d: &driver{o: o, ref: ref, got: got, rep: rep}, protocol: o.protocol, scope: o.livePrefix,
		roles: deriveRoles(keys, o.livePrefix), tokens: map[string][2][]string{}}
	if o.protocol == "gcs" {
		if lr.meta, err = gcsMetadataOf(got, o); err != nil {
			return err
		}
	}
	if o.pins != "" {
		if lr.pins, err = readPins(o.pins); err != nil {
			return err
		}
	}
	lr.run(keys)
	rep.Seconds = time.Since(rep.Started).Seconds()
	return lr.d.write()
}

// gcsMetadataOf is the -gcs-metadata choice, or with auto, native when
// the fixture's first item carries a stored etag.
func gcsMetadataOf(got *target, o options) (gcsMetadata, error) {
	switch o.gcsMeta {
	case "native":
		return gcsNative, nil
	case "synthetic":
		return gcsSynthetic, nil
	case "", "auto":
	default:
		return 0, fmt.Errorf("-gcs-metadata is auto, native or synthetic")
	}
	resp, err := got.do(o.bucket, request{Method: "GET", Path: gcsPath, Query: "maxResults=1&prefix=" + campaignEscape(o.livePrefix, "/")})
	if err != nil {
		return 0, err
	}
	var page struct {
		Items []struct {
			ETag string `json:"etag"`
		} `json:"items"`
	}
	if json.Unmarshal(resp.Body, &page) == nil && len(page.Items) > 0 && page.Items[0].ETag != "swath-gcs-synthetic-v1" {
		return gcsNative, nil
	}
	return gcsSynthetic, nil
}

// listScope lists the Go server's names under prefix, in rising byte
// order, and the newest modification time among them.
func listScope(t *target, protocol, bucket, prefix string) ([]string, time.Time, error) {
	var keys []string
	var newest time.Time
	note := func(lm string) {
		if tm, err := time.Parse(time.RFC3339Nano, lm); err == nil && tm.After(newest) {
			newest = tm
		}
	}
	token := ""
	for {
		var r request
		if protocol == "gcs" {
			r = request{Method: "GET", Path: gcsPath, Query: "maxResults=1000&prefix=" + campaignEscape(prefix, "/")}
			if token != "" {
				r.Query += "&pageToken=" + campaignEscape(token, "")
			}
		} else {
			r = request{Method: "GET", Query: "list-type=2&max-keys=1000&encoding-type=url&prefix=" + s3.PercentEncode([]byte(prefix))}
			if token != "" {
				r.Query += "&continuation-token=" + s3.PercentEncode([]byte(token))
			}
		}
		resp, err := t.do(bucket, r)
		if err != nil {
			return nil, newest, err
		}
		if resp.Status != 200 {
			return nil, newest, fmt.Errorf("status %d: %.300s", resp.Status, resp.Body)
		}
		if protocol == "gcs" {
			var p struct {
				Items []struct {
					Name    string `json:"name"`
					Updated string `json:"updated"`
				} `json:"items"`
				NextPageToken string `json:"nextPageToken"`
			}
			if err := json.Unmarshal(resp.Body, &p); err != nil {
				return nil, newest, err
			}
			for _, it := range p.Items {
				keys = append(keys, it.Name)
				note(it.Updated)
			}
			token = p.NextPageToken
		} else {
			var p struct {
				NextContinuationToken string
				Contents              []struct{ Key, LastModified string }
			}
			if err := xml.Unmarshal(resp.Body, &p); err != nil {
				return nil, newest, err
			}
			for _, c := range p.Contents {
				keys = append(keys, string(listedKey(c.Key)))
				note(c.LastModified)
			}
			token = p.NextContinuationToken
		}
		if token == "" {
			// The roles and walks search these names by binary search
			// (under): a listing that is not in strictly rising byte
			// order, or names decoded wrongly, would bind them to the
			// wrong names, so it is refused here.
			for i := 1; i < len(keys); i++ {
				if keys[i-1] >= keys[i] {
					return nil, newest, fmt.Errorf("the listing is not in rising byte order: %q then %q", keys[i-1], keys[i])
				}
			}
			return keys, newest, nil
		}
	}
}

// liveCase is one request sequence to send: a matrix case or a walk.
type liveCase struct {
	id, kind, query string
	header          [][2]string
	src             *providerCase
}

func (lr *liveRun) cases() []providerCase {
	if lr.protocol == "gcs" {
		return gcsProviderCases()
	}
	return s3ProviderCases()
}

// scoped adds the scope as the prefix of a query that names none, so a
// question asked of the whole corpus is asked of the recorded scope.
func (lr *liveRun) scoped(query string) string {
	if lr.scope == "" {
		return query
	}
	for _, part := range strings.Split(query, "&") {
		if name, _, _ := strings.Cut(part, "="); name == "prefix" {
			return query
		}
	}
	return query + "&prefix=" + campaignEscape(lr.scope, "/")
}

func (lr *liveRun) run(keys []string) {
	var exchanges, walks, tokens []liveCase
	for _, c := range lr.cases() {
		q, why := lr.roles.instantiate(&c)
		if why != "" {
			lr.d.rep.Coverage = append(lr.d.rep.Coverage, c.ID+": "+why)
			continue
		}
		lc := liveCase{id: c.ID, kind: c.Kind, query: lr.scoped(q), header: c.Header, src: &c}
		line := c.ID + ": sent " + lc.query
		if note := lr.roles.reducedShape(&c); note != "" {
			line += " [reduced shape: " + note + "]"
		}
		lr.d.rep.Coverage = append(lr.d.rep.Coverage, line)
		switch c.Kind {
		case "exchange":
			exchanges = append(exchanges, lc)
		case "walk":
			walks = append(walks, lc)
		case "token":
			tokens = append(tokens, lc)
		}
	}
	lr.parallel(exchanges, lr.exchange)
	lr.parallel(walks, lr.walk)
	lr.parallel(tokens, lr.token)
	lr.parallel(lr.scopeWalks(keys), lr.walk)
	if lr.d.o.liveScripted {
		lr.parallel(lr.scripted(keys), lr.exchange)
	}
}

func (lr *liveRun) parallel(cases []liveCase, do func(liveCase)) {
	work := make(chan liveCase)
	var wg sync.WaitGroup
	for range lr.d.o.parallel {
		wg.Go(func() {
			for c := range work {
				do(c)
			}
		})
	}
	for _, c := range cases {
		work <- c
	}
	close(work)
	wg.Wait()
}

func (lr *liveRun) request(query string, header [][2]string) request {
	r := request{Method: "GET", Query: query, Header: header}
	if lr.protocol == "gcs" {
		r.Path = gcsPath
		r.Header = append([][2]string{{"Accept", "application/json"}}, header...)
	}
	return r
}

func (lr *liveRun) tokenParam() string {
	if lr.protocol == "gcs" {
		return "pageToken"
	}
	return "continuation-token"
}

func (lr *liveRun) withToken(query, token string) string {
	if token == "" {
		return query
	}
	return query + "&" + lr.tokenParam() + "=" + campaignEscape(token, "")
}

func (lr *liveRun) nextToken(body []byte) string {
	if lr.protocol == "gcs" {
		return gcsNextToken(body)
	}
	return nextToken(body)
}

// send asks both sides, each with its own token, and compares. note
// says how the token was formed, for a recording proxy (its case is
// c's).
func (lr *liveRun) send(c liveCase, refTok, gotTok string, note recordNote) (a, b *response, df diff, err error) {
	d := lr.d
	query := c.query
	ra := lr.request(lr.withToken(query, refTok), c.header)
	if d.ref.recorder {
		note.caseID = c.id
		ra.Header = append(slices.Clone(ra.Header), note.headers()...)
	}
	if a, err = d.ref.do(d.o.bucket, ra); err != nil {
		return nil, nil, df, fmt.Errorf("live: %w", err)
	}
	if b, err = d.got.do(d.o.bucket, lr.request(lr.withToken(query, gotTok), c.header)); err != nil {
		return a, nil, df, fmt.Errorf("go: %w", err)
	}
	d.count(2)
	if lr.protocol == "gcs" {
		df = gcsLive(a, b, d.o.bucket, gcsFullProjection(query), lr.meta)
	} else {
		df = s3Live(a, b, d.o.bucket, refTok, gotTok)
	}
	if !df.OK && a.Status == b.Status && a.Status != 200 {
		df.RefExcerpt, df.GotExcerpt = fmt.Sprintf("%.300q", a.Body), fmt.Sprintf("%.300q", b.Body)
	} else if !df.OK {
		df.RefExcerpt, df.GotExcerpt = fmt.Sprintf("%d %.300q", a.Status, a.Body), fmt.Sprintf("%d %.300q", b.Status, b.Body)
	}
	return a, b, df, nil
}

func (lr *liveRun) result(c liveCase) result {
	kind := "provider"
	if c.src == nil {
		kind = c.kind
	}
	return result{Kind: kind, Name: c.id, Method: "GET", Target: c.query}
}

func (lr *liveRun) exchange(c liveCase) {
	res := lr.result(c)
	var note recordNote
	if t, _, ok := queryToken(c.query, lr.tokenParam()); ok && t != "" {
		note.token = tokenVerbatim // a token the case wrote itself, no answer issued
	}
	a, b, df, err := lr.send(c, "", "", note)
	origin := originNone
	if note.token == tokenVerbatim {
		origin = originVerbatim
	}
	lr.finish(&res, lr.judgement(c, "", "", "", origin, a, b, df), err)
}

// judgement is what the documented differences are matched against.
func (lr *liveRun) judgement(c liveCase, mutation, refTok, gotTok string, origin tokenOrigin, a, b *response, df diff) judgement {
	return judgement{protocol: lr.protocol, bucket: lr.d.o.bucket, caseID: c.id, query: c.query, mutation: mutation, origin: origin,
		refTok: refTok, gotTok: gotTok, ref: a, got: b, df: df}
}

// finish records a case's verdict: a documented difference
// (documentedDifference) is listed apart, outside the verdict counts.
func (lr *liveRun) finish(res *result, j judgement, err error) {
	df := j.df
	if err == nil {
		if why := documentedDifference(j, lr.pins); why != "" {
			lr.d.noteDocumented(res.Name, why, df)
			return
		}
	}
	if err != nil {
		res.Error = err.Error()
	} else {
		res.Pass = df.OK
		if !df.OK {
			res.diff = &df
		}
	}
	lr.d.record(*res)
}

// readPins reads a replay-expectations.json (case id to "match" or to
// {"mismatch": message}) and binds each pinned mismatch to the evidence
// beside it (index.json and the capture it names): the case must be one
// of the provider's matrix whose template, rendered over the evidence's
// run prefix, is the captured request's query, and the pin then applies
// only to requests of that template's shape answered as the captured
// provider answered (pin.admits). A pin that cannot be bound so is an
// error: it would document by its label alone.
func readPins(path string) (map[string]pin, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	pins := map[string]pin{}
	var ids []string
	mismatches := map[string]string{}
	for id, v := range raw {
		var m struct{ Mismatch string }
		if json.Unmarshal(v, &m) == nil && m.Mismatch != "" {
			mismatches[id] = m.Mismatch
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return pins, nil
	}
	dir := filepath.Dir(path)
	var index struct {
		Provider  string `json:"provider"`
		RunPrefix string `json:"run_prefix"`
		Captures  map[string]struct {
			File string `json:"file"`
			Kind string `json:"kind"`
		} `json:"captures"`
	}
	if err := readEvidenceJSON(filepath.Join(dir, "index.json"), &index); err != nil {
		return nil, fmt.Errorf("the pins' evidence index: %w", err)
	}
	var cases []providerCase
	switch index.Provider {
	case "gcs":
		cases = gcsProviderCases()
	case "s3":
		cases = s3ProviderCases()
	default:
		return nil, fmt.Errorf("the pins' evidence index names provider %q, not gcs or s3", index.Provider)
	}
	tokenParam := tokenParamOf(index.Provider)
	for _, id := range slices.Sorted(slices.Values(ids)) {
		i := slices.IndexFunc(cases, func(c providerCase) bool { return c.ID == id })
		if i < 0 {
			return nil, fmt.Errorf("pin %s: not a case of the %s matrix", id, index.Provider)
		}
		c := cases[i]
		ce, ok := index.Captures[id]
		if !ok {
			return nil, fmt.Errorf("pin %s: the evidence holds no capture of it", id)
		}
		if ce.Kind != "exchange" {
			return nil, fmt.Errorf("pin %s: its capture is a %s; a pin binds one captured exchange", id, ce.Kind)
		}
		var doc struct {
			Request struct {
				Query string `json:"query"`
			} `json:"request"`
			Response struct {
				Status     int    `json:"status"`
				BodyBase64 string `json:"body_base64"`
			} `json:"response"`
		}
		if err := readEvidenceJSON(filepath.Join(dir, filepath.FromSlash(ce.File)), &doc); err != nil {
			return nil, fmt.Errorf("pin %s: %w", id, err)
		}
		want, missing := c.render(corpusBinding(index.RunPrefix))
		_, want, _ = queryToken(want, tokenParam)
		capTok, captured, _ := queryToken(doc.Request.Query, tokenParam)
		if len(missing) > 0 || captured != want {
			return nil, fmt.Errorf("pin %s: the evidence captured %q, which the case's template %q does not render over %q",
				id, doc.Request.Query, c.Tmpl, index.RunPrefix)
		}
		p := pin{mismatch: mismatches[id], shape: templateShape(c.Tmpl, tokenParam), refStatus: doc.Response.Status, mutation: c.Mutation}
		switch {
		case c.Mutation == "tampered":
			p.token, p.mutation = pinTampered, ""
		case c.TokenFrom != "":
			p.token = pinSourced
		case c.Kind == "walk":
			p.token = pinAnyToken
		}
		if p.token == pinSourced && capTok == "" || p.token == pinTampered && capTok != tamperedToken {
			return nil, fmt.Errorf("pin %s: the evidence captured %q, without the token the case sends", id, doc.Request.Query)
		}
		if p.refStatus != 200 {
			body, err := base64.StdEncoding.DecodeString(doc.Response.BodyBase64)
			if err != nil {
				return nil, fmt.Errorf("pin %s: the captured body: %w", id, err)
			}
			if p.refReason = errorReason(index.Provider, &response{Status: p.refStatus, Body: body}); p.refReason == "" {
				return nil, fmt.Errorf("pin %s: the captured %d is not a %s error document", id, p.refStatus, index.Provider)
			}
		}
		pins[id] = p
	}
	return pins, nil
}

// readEvidenceJSON decodes an evidence file; a .gz file is stored
// compressed.
func readEvidenceJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		r = zr
	}
	if err := json.NewDecoder(r).Decode(v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// walk follows each side's own tokens to the end, comparing every page.
// A page whose difference is a documented one ends the walk, which is
// then listed as documented, as the page is in a replay of its
// recording.
func (lr *liveRun) walk(c liveCase) {
	res := lr.result(c)
	res.Pass = true
	var refTokens, gotTokens []string
	refTok, gotTok := "", ""
	limit := lr.d.o.walkLimit
	documented := ""
	for {
		a, b, df, err := lr.send(c, refTok, gotTok, recordNote{})
		res.Pages++
		if err == nil && !df.OK {
			origin := originIssued
			if refTok == "" {
				origin = originNone
			}
			if why := documentedDifference(lr.judgement(c, "", refTok, gotTok, origin, a, b, df), lr.pins); why != "" {
				documented = why
				lr.d.noteDocumented(res.Name, fmt.Sprintf("page %d: %s", res.Pages, why), df)
				break
			}
		}
		if err != nil || !df.OK {
			res.Pass, res.FailAt = false, res.Pages
			if err != nil {
				res.Error = err.Error()
			} else {
				res.diff = &df
			}
			break
		}
		refTok, gotTok = lr.nextToken(a.Body), lr.nextToken(b.Body)
		if refTok == "" || a.Status != 200 {
			break
		}
		refTokens, gotTokens = append(refTokens, refTok), append(gotTokens, gotTok)
		if limit > 0 && res.Pages >= limit {
			res.Partial = true
			break
		}
	}
	lr.mu.Lock()
	lr.tokens[c.id] = [2][]string{refTokens, gotTokens}
	lr.mu.Unlock()
	if documented == "" {
		lr.d.record(res)
	}
}

// token sends a case with a token its source walk issued at page Page,
// each side its own, altered by the case's mutation.
func (lr *liveRun) token(c liveCase) {
	res := lr.result(c)
	src := c.src
	var toks [2]string
	note, mutation := recordNote{from: src.TokenFrom}, src.Mutation
	if src.Mutation == "tampered" {
		toks = [2]string{tamperedToken, tamperedToken}
		note, mutation = recordNote{token: tokenVerbatim}, ""
	} else {
		lr.mu.Lock()
		walked, ok := lr.tokens[src.TokenFrom]
		lr.mu.Unlock()
		if !ok || len(walked[0]) <= src.Page || len(walked[1]) <= src.Page {
			lr.d.record(result{Kind: res.Kind, Name: c.id, Skipped: fmt.Sprintf("%s issued no token at page %d on both sides", src.TokenFrom, src.Page)})
			return
		}
		for i := range 2 {
			t, err := mutateToken(walked[i][src.Page], src.Mutation)
			if err != nil {
				lr.d.record(result{Kind: res.Kind, Name: c.id, Skipped: []string{"live", "go"}[i] + " token: " + err.Error()})
				return
			}
			toks[i] = t
		}
		if mutation != "" {
			note.token = tokenNote(mutation, walked[0][src.Page])
		}
	}
	a, b, df, err := lr.send(c, toks[0], toks[1], note)
	origin := originIssued
	switch {
	case src.Mutation == "tampered":
		origin = originVerbatim
	case mutation != "":
		origin = originMutated
	}
	lr.finish(&res, lr.judgement(c, mutation, toks[0], toks[1], origin, a, b, df), err)
}

// mutateToken is the campaign's token alteration (matrix.py
// mutate_token): flip the last character, cut four, or flip the lowest
// bit of the last decoded byte of a base64 token. A token that is not
// base64 has no encoding-preserving flip. The decoding is not strict
// about the unused bits of the last character or the padding, as the
// campaign's is not.
func mutateToken(token, mutation string) (string, error) {
	switch mutation {
	case "":
		return token, nil
	case "flip":
		last := "A"
		if strings.HasSuffix(token, "A") {
			last = "B"
		}
		return token[:len(token)-1] + last, nil
	case "truncate":
		if len(token) > 4 {
			return token[:len(token)-4], nil
		}
		return token[:1], nil
	case "flip-decoded":
		enc := base64.StdEncoding
		if strings.ContainsAny(token, "-_") {
			enc = base64.URLEncoding
		}
		// Padding is not checked (live S3 tokens carry a stray '='), as
		// the campaign's decoder does not check it.
		raw, err := enc.WithPadding(base64.NoPadding).DecodeString(strings.TrimRight(token, "="))
		if err != nil || strings.ContainsAny(token, "\r\n") {
			return "", fmt.Errorf("token is not base64; no encoding-preserving mutation exists")
		}
		if len(raw) == 0 {
			return "", fmt.Errorf("token decodes to no bytes")
		}
		raw[len(raw)-1] ^= 1
		out := enc.EncodeToString(raw)
		if !strings.HasSuffix(token, "=") {
			out = strings.TrimRight(out, "=")
		}
		return out, nil
	}
	return "", fmt.Errorf("unknown token mutation %s", mutation)
}

// scopeWalks are the token walks of the scope and of its directories
// (up to -max-top), flat and with "/", at each -walks page size whose
// walk takes at most -live-walk-pages pages.
func (lr *liveRun) scopeWalks(keys []string) []liveCase {
	dirs := scanDirectories(keys, lr.scope)
	prefixes := []string{lr.scope}
	for _, sub := range dirs[lr.scope].subdirs {
		if len(prefixes) > lr.d.o.maxTop {
			break
		}
		prefixes = append(prefixes, sub)
	}
	var out []liveCase
	for _, size := range parseSizes(lr.d.o.walkSizes) {
		for _, p := range prefixes {
			n := dirs[p].total
			if (n+size-1)/size > lr.d.o.liveWalkPages {
				continue
			}
			e := campaignEscape(p, "/")
			m := fmt.Sprint(size)
			var flat, delim string
			if lr.protocol == "gcs" {
				flat, delim = "prefix="+e+"&maxResults="+m, "prefix="+e+"&delimiter=/&maxResults="+m
			} else {
				flat = "list-type=2&prefix=" + e + "&max-keys=" + m + "&encoding-type=url"
				delim = "list-type=2&prefix=" + e + "&delimiter=/&max-keys=" + m
			}
			out = append(out, liveCase{id: "walk prefix=" + p + " size=" + m, kind: "walk", query: flat},
				liveCase{id: "walk prefix=" + p + " delimiter=/ size=" + m, kind: "walk", query: delim})
		}
	}
	return out
}

// scripted is the driver's own matrix (the requests marked Live on S3;
// those on the GCS route that need no token and no other path), scoped.
func (lr *liveRun) scripted(keys []string) []liveCase {
	var bkeys [][]byte
	for _, k := range keys {
		bkeys = append(bkeys, []byte(k))
	}
	var out []liveCase
	rng := newRand(lr.d.o.seed)
	if lr.protocol == "gcs" {
		for _, r := range gcsMatrix(bkeys, rng, lr.d.o.maxPrefixes) {
			if r.Path == "" && r.TokenFrom == "" && r.Method == "GET" && !strings.Contains(r.Query, "pageToken") {
				out = append(out, liveCase{id: "scripted " + r.Name, kind: "case", query: lr.scoped(r.Query)})
			}
		}
		return out
	}
	for _, r := range s3Matrix(bkeys, rng, lr.d.o.maxPrefixes) {
		if r.Live && r.Method == "GET" && r.Path == "" && r.Raw == "" {
			out = append(out, liveCase{id: "scripted " + r.Name, kind: "case", query: lr.scoped(r.Query)})
		}
	}
	return out
}

func newRand(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 0x5eed)) }
