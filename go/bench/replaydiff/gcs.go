// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// The GCS JSON API route (-protocol gcs): both servers serve the fixture
// with --protocols gcs and answer GET /storage/v1/b/{bucket}/o.

const gcsPath = "/storage/v1/b/{bucket}/o"

// runGCS is run for the GCS route: the scripted matrix, token walks that
// follow each server's own tokens, and a seeded random walk.
func runGCS(o options) error {
	ctx := context.Background()
	if o.goBin == "" || o.java == "" || o.fixture == "" {
		return fmt.Errorf("-go, -java and -fixture are required")
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: o.parallel * 2}, Timeout: 5 * time.Minute}
	java, err := startServer(ctx, "java", o.java, gcsServeArgs(o.fixture, o.bucket, o.javaArgs), filepath.Join(o.logDir, "replaydiff-java.log"))
	if err != nil {
		return err
	}
	defer java.stop()
	ref := &target{name: "java", base: java.endpoint, client: client}
	keys, err := gcsListAll(ref, o.bucket)
	if err != nil {
		return fmt.Errorf("listing the reference: %w", err)
	}
	goFixture := o.fixture
	if o.goFixture != "" {
		goFixture = o.goFixture
	}
	goSrv, err := startServer(ctx, "go", o.goBin, gcsServeArgs(goFixture, o.bucket, o.goArgs), filepath.Join(o.logDir, "replaydiff-go.log"))
	if err != nil {
		return err
	}
	defer goSrv.stop()
	got := &target{name: "go", base: goSrv.endpoint, client: client}
	rep := &report{Reference: ref.name, Fixture: o.fixture, GoFixture: o.goFixture, Bucket: o.bucket, Seed: o.seed, Started: time.Now(), Keys: len(keys)}
	d := &driver{o: o, ref: ref, got: got, comp: gcsComparator{}, rep: rep}
	d.runGCSAll(keys, rand.New(rand.NewPCG(o.seed, 0x5eed)))
	rep.Seconds = time.Since(rep.Started).Seconds()
	return d.write()
}

// runGCSAll runs the matrix, the walks and the random requests.
func (d *driver) runGCSAll(keys [][]byte, rng *rand.Rand) {
	d.runGCSCases("case", gcsMatrix(keys, rng, d.o.maxPrefixes))
	d.runGCSWalks(gcsWalks(keys, parseSizes(d.o.walkSizes), d.o.maxTop))
	prefixes := directoryPrefixes(keys)
	random := make([]request, d.o.random)
	for i := range random {
		random[i] = gcsRandomRequest(keys, rng, prefixes)
		random[i].Name = fmt.Sprintf("random %d", i)
	}
	d.runGCSCases("random", random)
}

func parseSizes(s string) []int {
	var sizes []int
	for _, f := range strings.Split(s, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && n > 0 {
			sizes = append(sizes, n)
		}
	}
	return sizes
}

func gcsServeArgs(fixture, bucket, extra string) []string {
	return append(serveArgs(fixture, bucket, extra), "--protocols", "gcs")
}

// gcsQuery builds a query from name/value pairs, values encoded with
// every byte but RFC 3986's unreserved characters escaped, as the GCS
// client libraries send them.
func gcsQuery(params ...string) string {
	var b strings.Builder
	for i := 0; i+1 < len(params); i += 2 {
		if b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(params[i])
		b.WriteByte('=')
		b.WriteString(gcsEscape(params[i+1]))
	}
	return b.String()
}

func gcsEscape(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-._~", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// gcsPage is the part of an objects list the driver reads.
type gcsPage struct {
	Items []struct {
		Name string `json:"name"`
	} `json:"items"`
	Prefixes      []string `json:"prefixes"`
	NextPageToken string   `json:"nextPageToken"`
}

func gcsNextToken(body []byte) string {
	var p gcsPage
	if json.Unmarshal(body, &p) != nil {
		return ""
	}
	return p.NextPageToken
}

// gcsListAll walks the reference's whole listing.
func gcsListAll(t *target, bucket string) ([][]byte, error) {
	var keys [][]byte
	token := ""
	for {
		query := "maxResults=1000"
		if token != "" {
			query += "&pageToken=" + gcsEscape(token)
		}
		resp, err := t.do(bucket, request{Method: "GET", Path: gcsPath, Query: query})
		if err != nil {
			return nil, err
		}
		if resp.Status != 200 {
			return nil, fmt.Errorf("status %d: %.300s", resp.Status, resp.Body)
		}
		var page gcsPage
		if err := json.Unmarshal(resp.Body, &page); err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			keys = append(keys, []byte(it.Name))
		}
		if token = page.NextPageToken; token == "" {
			return keys, nil
		}
	}
}

// gcsComparator holds two answers on the GCS route to the same status,
// the same compared headers (Content-Type, x-swath-replay-*, and Allow on
// a 405), and the same body. Both servers render compact JSON in a fixed
// field order and issue the same token bytes for the same state, so the
// body is compared byte for byte; when the bytes differ only inside the
// page token's value, Detail says so (still a failure: a token difference
// is a wire difference). A walk follows each server's own tokens and
// passes a page whose bytes agree apart from the token's value (the
// walk judges the tokens by where they lead), listing that page in the
// report's token_byte_differences: the walk's verdict is semantic, the
// list is the wire fidelity.
type gcsComparator struct{}

func (gcsComparator) compare(ref, got *response) diff {
	d := (bytesComparator{}).compare(ref, got)
	if a, b := values(ref.Header, "allow"), values(got.Header, "allow"); a != b {
		d.HeaderDiffs = append(d.HeaderDiffs, fmt.Sprintf("allow: %q | %q", a, b))
		d.OK = false
	}
	if d.Offset >= 0 {
		if equalMaskedTokens(ref.Body, got.Body) {
			d.Detail = "bytes equal with the page tokens' values masked: the tokens differ"
		} else {
			d.Detail = "documents differ"
		}
	}
	return d
}

// tokenValue is the value span of a page's nextPageToken member as the
// servers write it (a token is base64url and '.', never escaped).
var tokenValue = regexp.MustCompile(`"nextPageToken":"[^"\\]*"`)

// maskTokenValue blanks the token's value and keeps every other byte, so
// field order, spacing and escapes still count.
func maskTokenValue(b []byte) []byte {
	return tokenValue.ReplaceAll(b, []byte(`"nextPageToken":""`))
}

// equalMaskedTokens reports whether two bodies are byte-identical apart
// from their page tokens' values.
func equalMaskedTokens(a, b []byte) bool {
	return bytes.Equal(maskTokenValue(a), maskTokenValue(b))
}

// skip records a case that could not run as designed; it is neither
// compared nor passed.
func (d *driver) skip(name, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rep.Skipped = append(d.rep.Skipped, name+": "+reason)
}

// runGCSCases is runCases with GCS page tokens: a request naming
// TokenFrom gets the reference's nextPageToken for that request, edited
// by gcsTokenEdits when the case names one. A source page that issued no
// token leaves the case skipped: without a token it would be an ordinary
// listing passing for a token test.
func (d *driver) runGCSCases(kind string, cases []request) {
	var first, second []request
	for _, c := range cases {
		c.Path = cmpOrPath(c.Path)
		if c.TokenFrom != "" {
			second = append(second, c)
		} else {
			first = append(first, c)
		}
	}
	for _, batch := range [][]request{first, second} {
		work := make(chan request)
		var wg sync.WaitGroup
		for range d.o.parallel {
			wg.Go(func() {
				for c := range work {
					if c.TokenFrom != "" {
						tok, ok := d.refTokens.Load(c.TokenFrom)
						if !ok {
							d.skip(c.Name, "the source page "+strconv.Quote(c.TokenFrom)+" issued no token")
							continue
						}
						t := tok.(string)
						if edit := gcsTokenEdits[editOf(c.Name)]; edit != nil {
							t = edit(t)
						}
						c.Query += "&pageToken=" + gcsEscape(t)
					}
					res := result{Kind: kind, Name: c.Name, Method: c.Method, Target: d.got.url(d.o.bucket, c)}
					a, b, df, err := d.gcsCompareOne(c)
					if err != nil {
						res.Error = err.Error()
					} else {
						if competingFeatureOK(c, a, b, df) {
							df.OK = true
						}
						res.Pass, res.diff = df.OK, &df
						if tok := gcsNextToken(a.Body); tok != "" {
							d.refTokens.Store(c.Name, tok)
						}
					}
					if res.Pass {
						res.diff = nil
					}
					d.record(res)
				}
			})
		}
		for _, c := range batch {
			work <- c
		}
		close(work)
		wg.Wait()
	}
}

// gcsCompareOne sends c to both servers and compares the answers.
func (d *driver) gcsCompareOne(c request) (ref, got *response, df diff, err error) {
	if ref, err = d.ref.do(d.o.bucket, c); err != nil {
		return nil, nil, df, fmt.Errorf("%s: %w", d.ref.name, err)
	}
	if got, err = d.got.do(d.o.bucket, c); err != nil {
		return ref, nil, df, fmt.Errorf("%s: %w", d.got.name, err)
	}
	d.count(2)
	return ref, got, d.comp.compare(ref, got), nil
}

func cmpOrPath(p string) string {
	if p == "" {
		return gcsPath
	}
	return p
}

func (d *driver) runGCSWalks(walks []walk) {
	work := make(chan walk)
	var wg sync.WaitGroup
	for range d.o.parallel {
		wg.Go(func() {
			for w := range work {
				d.record(d.gcsWalkOne(w))
			}
		})
	}
	for _, w := range walks {
		work <- w
	}
	close(work)
	wg.Wait()
}

// gcsWalkOne walks a listing to its end on both servers, each following
// its own nextPageToken, and compares every page; the first difference
// ends the walk.
func (d *driver) gcsWalkOne(w walk) result {
	res := result{Kind: "walk", Name: w.Name, Method: "GET", Pass: true}
	refTok, gotTok := "", ""
	for {
		ra, rb := request{Method: "GET", Path: gcsPath, Query: w.Query}, request{Method: "GET", Path: gcsPath, Query: w.Query}
		if refTok != "" {
			ra.Query += "&pageToken=" + gcsEscape(refTok)
			rb.Query += "&pageToken=" + gcsEscape(gotTok)
		}
		res.Target = d.got.url(d.o.bucket, rb)
		a, errA := d.ref.do(d.o.bucket, ra)
		b, errB := d.got.do(d.o.bucket, rb)
		d.count(2)
		res.Pages++
		var df diff
		if errA == nil && errB == nil {
			df = d.comp.compare(a, b)
			if !df.OK && a.Status == b.Status && len(df.HeaderDiffs) == 0 && equalMaskedTokens(a.Body, b.Body) {
				// Every byte but the token values agrees: the walk judges
				// the tokens by where they lead, and the byte difference
				// is reported as a wire difference of its own.
				df.OK = true
				d.mu.Lock()
				d.rep.TokenBytes = append(d.rep.TokenBytes, fmt.Sprintf("walk %q page %d", w.Name, res.Pages))
				d.mu.Unlock()
			}
		}
		if errA != nil || errB != nil || !df.OK {
			res.Pass, res.FailAt = false, res.Pages
			if err := cmpOrErr(errA, errB); err != nil {
				res.Error = err.Error()
			} else {
				res.diff = &df
			}
			return res
		}
		refTok, gotTok = gcsNextToken(a.Body), gcsNextToken(b.Body)
		if refTok == "" {
			return res
		}
		if d.o.walkLimit > 0 && res.Pages >= d.o.walkLimit {
			res.Partial = true
			return res
		}
	}
}

func cmpOrErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

// gcsMatrix builds the scripted objects.list requests for a fixture whose
// keys (sorted) are given: the parameter table's accepted and refused
// values, prefixes at every depth with and without delimiters, offsets
// around keys and inside subtrees, tokens good, tampered and foreign,
// wrong buckets, paths and methods.
func gcsMatrix(keys [][]byte, rng *rand.Rand, maxPrefixes int) []request {
	var out []request
	add := func(name string, params ...string) {
		out = append(out, request{Name: name, Method: "GET", Query: gcsQuery(params...)})
	}
	raw := func(name, query string) {
		out = append(out, request{Name: name, Method: "GET", Query: query})
	}
	for _, mr := range []string{"1", "2", "3", "999", "1000", "1001", "5000", "9223372036854775807",
		"0", "abc", "-1", "01", "+5", "", " 7", "99999999999999999999", "1.5"} {
		add("maxResults="+mr, "maxResults", mr)
	}
	prefixes := directoryPrefixes(keys)
	if len(prefixes) > maxPrefixes {
		rng.Shuffle(len(prefixes), func(i, j int) { prefixes[i], prefixes[j] = prefixes[j], prefixes[i] })
		prefixes = prefixes[:maxPrefixes]
		slices.Sort(prefixes)
	}
	prefixes = append(prefixes, "zz-no-such-prefix/", "")
	multi := gcsMultiByteDelimiter(keys, rng)
	for _, p := range prefixes {
		add("prefix="+p, "prefix", p, "maxResults", "1000")
		add("prefix="+p+" delimiter=/", "prefix", p, "delimiter", "/")
		add("prefix="+p+" delimiter=/ maxResults=3", "prefix", p, "delimiter", "/", "maxResults", "3")
		add("prefix="+p+" delimiter="+multi, "prefix", p, "delimiter", multi, "maxResults", "1000")
		add("prefix="+p+" maxResults=1", "prefix", p, "maxResults", "1")
		add("prefix="+p+" delimiter=/ maxResults=1", "prefix", p, "delimiter", "/", "maxResults", "1")
	}
	add("delimiter=é", "delimiter", "é", "maxResults", "100")
	add("delimiter=-", "delimiter", "-", "maxResults", "100")
	add("delimiter=// ", "delimiter", "//", "maxResults", "100")
	if len(keys) > 0 {
		first, last := string(keys[0]), string(keys[len(keys)-1])
		mid := string(keys[len(keys)/2])
		pick := string(keys[rng.IntN(len(keys))])
		offsets := []struct{ name, v string }{
			{"before", "\x01"}, {"first", first}, {"mid", mid}, {"between", mid + "\x00"}, {"random", pick},
			{"last", last}, {"after", last + "\U0010FFFF"},
		}
		for _, s := range offsets {
			add("startOffset "+s.name, "startOffset", s.v, "maxResults", "50")
			add("startOffset "+s.name+" delimiter=/", "startOffset", s.v, "delimiter", "/", "maxResults", "50")
			add("endOffset "+s.name, "endOffset", s.v, "maxResults", "50")
			for _, e := range offsets {
				add("startOffset "+s.name+" endOffset "+e.name, "startOffset", s.v, "endOffset", e.v, "maxResults", "7")
			}
		}
		for _, p := range prefixes[:min(len(prefixes), 20)] {
			if p == "" {
				continue
			}
			parent := parentPrefix(p)
			inside := p + "m"
			add("prefix "+parent+" startOffset inside "+p, "prefix", parent, "delimiter", "/", "startOffset", inside, "maxResults", "20")
			add("prefix "+parent+" startOffset at "+p, "prefix", parent, "delimiter", "/", "startOffset", p, "maxResults", "20")
			add("prefix "+parent+" endOffset inside "+p, "prefix", parent, "delimiter", "/", "endOffset", inside, "maxResults", "20")
			add("prefix "+p+" startOffset before it", "prefix", p, "startOffset", parent, "maxResults", "20")
			add("prefix "+p+" endOffset at it", "prefix", p, "endOffset", p, "maxResults", "20")
			add("prefix "+parent+" offsets around "+p, "prefix", parent, "delimiter", "/", "startOffset", inside, "endOffset", p+"zzz", "maxResults", "2")
		}
	}
	for i, k := range gcsKeysNeedingEscape(keys, 20) {
		add(fmt.Sprintf("escaped key %d", i), "prefix", string(k), "maxResults", "5")
		add(fmt.Sprintf("escaped key %d startOffset", i), "startOffset", string(k), "maxResults", "5")
	}
	// The parameter table: accepted, refused and unsupported values.
	for _, kv := range [][2]string{
		{"projection", "full"}, {"projection", "noAcl"}, {"projection", "FULL"}, {"projection", ""},
		{"prettyPrint", "true"}, {"prettyPrint", "false"}, {"prettyPrint", "1"},
		{"alt", "json"}, {"alt", "xml"}, {"alt", ""},
		{"fields", ""}, {"fields", "items(name)"}, {"matchGlob", ""}, {"matchGlob", "**"}, {"filter", ""}, {"filter", "x"},
		{"versions", "false"}, {"versions", "true"}, {"softDeleted", "false"}, {"softDeleted", "true"},
		{"includeFoldersAsPrefixes", "false"}, {"includeFoldersAsPrefixes", "true"},
		{"includeTrailingDelimiter", "false"}, {"includeTrailingDelimiter", "true"}, {"includeTrailingDelimiter", ""},
		{"userProject", "p"}, {"Prefix", "a"}, {"list-type", "2"}, {"pageToken", ""}, {"prefix", strings.Repeat("a", 1025)},
		{"startOffset", strings.Repeat("é", 513)}, {"pageToken", strings.Repeat("a", 2049)},
	} {
		add(kv[0]+"="+trim(kv[1]), kv[0], kv[1], "maxResults", "3")
	}
	add("projection=full delimiter", "projection", "full", "delimiter", "/", "maxResults", "5")
	raw("duplicate maxResults", "maxResults=1&maxResults=2")
	raw("duplicate empty prefix", "prefix=&prefix=")
	raw("encoded name", "max%52esults=2&pre%66ix=")
	raw("empty name", "=x")
	raw("bare ampersand", "&")
	raw("trailing ampersand", "maxResults=2&")
	raw("bare name", "prefix&maxResults=2")
	raw("raw plus", "prefix=a+b&maxResults=1")
	raw("broken escape", "prefix=%2")
	raw("bad hex escape", "prefix=%ZZ")
	raw("lone percent", "prefix=%")
	raw("invalid utf8 escape", "prefix=%C3")
	raw("surrogate escape", "prefix=%ED%A0%80")
	raw("overlong escape", "prefix=%C0%AF")
	raw("raw non-ASCII", "prefix=é&maxResults=3")
	raw("raw invalid byte", "prefix=a\xff&maxResults=3")
	raw("raw truncated sequence", "prefix=a\xe1\x80&maxResults=3")
	raw("one refused feature beside an accepted one", "versions=true&softDeleted=false&maxResults=1")
	raw("refused features across the groups", "fields=x&versions=true&maxResults=1")
	for name, query := range gcsCompetingFeatures {
		raw(name, query)
	}
	// Tokens: malformed, tampered, padded, foreign, and real tokens
	// replayed with another scope or page size.
	for _, tok := range []struct{ name, v string }{
		{"bad prefix", "gcs2.AAAA"}, {"no body", "gcs1."}, {"short", "gcs1.AAAA"}, {"not base64", "gcs1.!!!!"},
		{"s3 token", "v2:AGE"}, {"foreign", "CgRhYmNk"}, {"newline", "gcs1.AA\nAA"},
	} {
		add("token "+tok.name, "pageToken", tok.v, "maxResults", "5")
	}
	// Token cases take a token the reference issued. Each source page is
	// one entry of a window that holds more, so it is truncated whenever
	// the fixture has two keys (two root entries, two keys at the middle).
	pageOne := func(name string, params ...string) {
		out = append(out, request{Name: name, Method: "GET", Query: gcsQuery(params...)})
	}
	next := func(name, from string, params ...string) {
		out = append(out, request{Name: name, Method: "GET", Query: gcsQuery(params...), TokenFrom: from})
	}
	pageOne("token page 1", "maxResults", "1")
	next("token page 2", "token page 1", "maxResults", "1")
	next("token page 2 other size", "token page 1", "maxResults", "1000")
	next("token page 2 other prefix", "token page 1", "maxResults", "1", "prefix", "a")
	next("token page 2 with delimiter", "token page 1", "maxResults", "1", "delimiter", "/")
	next("token page 2 with startOffset", "token page 1", "maxResults", "1", "startOffset", "\x01")
	next("token page 2 with endOffset", "token page 1", "maxResults", "1", "endOffset", "\U0010FFFF")
	for _, edit := range slices.Sorted(maps.Keys(gcsTokenEdits)) {
		next("token page 2 "+edit, "token page 1", "maxResults", "1")
	}
	pageOne("delimiter token page 1", "maxResults", "1", "delimiter", "/")
	next("delimiter token page 2", "delimiter token page 1", "maxResults", "1", "delimiter", "/")
	next("delimiter token page 2 without delimiter", "delimiter token page 1", "maxResults", "1")
	if len(keys) >= 3 {
		lo, hi := string(keys[len(keys)/2-1]), string(keys[len(keys)/2+1])
		pageOne("offset token page 1", "maxResults", "1", "startOffset", lo, "endOffset", hi)
		next("offset token page 2", "offset token page 1", "maxResults", "1", "startOffset", lo, "endOffset", hi)
		next("offset token page 2 other end", "offset token page 1", "maxResults", "1", "startOffset", lo)
		next("offset token page 2 other start", "offset token page 1", "maxResults", "1", "endOffset", hi)
	}
	out = append(out,
		request{Name: "unknown bucket", Method: "GET", Path: "/storage/v1/b/no-such-bucket-here/o", Query: "maxResults=1"},
		request{Name: "escaped bucket", Method: "GET", Path: "/storage/v1/b/%7Bbucket%7D/o", Query: "maxResults=1"},
		request{Name: "trailing slash", Method: "GET", Path: gcsPath + "/", Query: "maxResults=1"},
		request{Name: "object path", Method: "GET", Path: gcsPath + "/x", Query: ""},
		request{Name: "bucket resource", Method: "GET", Path: "/storage/v1/b/{bucket}", Query: ""},
		request{Name: "upload path", Method: "GET", Path: "/upload/storage/v1/b/{bucket}/o", Query: ""},
		request{Name: "root", Method: "GET", Path: "/", Query: ""},
		request{Name: "s3 path", Method: "GET", Path: "/{bucket}", Query: "list-type=2"},
		request{Name: "ambiguous list-type", Method: "GET", Path: gcsPath, Query: "list-type=2&maxResults=1"},
		request{Name: "ambiguous encoded list-type", Method: "GET", Path: gcsPath, Query: "list%2Dtype=2"},
		request{Name: "HEAD", Method: "HEAD", Path: gcsPath, Query: "maxResults=2"},
		request{Name: "POST", Method: "POST", Path: gcsPath, Query: "maxResults=2"},
		request{Name: "DELETE unknown bucket", Method: "DELETE", Path: "/storage/v1/b/no-such-bucket-here/o", Query: ""},
	)
	return out
}

// gcsTokenEdits alter a genuinely issued token (the reference's) before
// it is sent: the native probe runner's mutations, and the padding and
// line-break spellings the token decoder must judge. A case "token page 2
// EDIT" applies gcsTokenEdits[EDIT].
var gcsTokenEdits = map[string]func(string) string{
	"flip": func(t string) string {
		last := byte('A')
		if t[len(t)-1] == 'A' {
			last = 'B'
		}
		return t[:len(t)-1] + string(last)
	},
	"truncate": func(t string) string { return t[:max(1, len(t)-4)] },
	"flip-decoded": func(t string) string {
		body, ok := strings.CutPrefix(t, "gcs1.")
		raw, err := base64.RawURLEncoding.DecodeString(body)
		if !ok || err != nil || len(raw) == 0 {
			return t + "!"
		}
		raw[len(raw)-1] ^= 1
		return "gcs1." + base64.RawURLEncoding.EncodeToString(raw)
	},
	"padded":      func(t string) string { return t + strings.Repeat("=", (4-len(strings.TrimPrefix(t, "gcs1."))%4)%4) },
	"overpadded":  func(t string) string { return t + "===" },
	"line break":  func(t string) string { return t[:len(t)/2] + "\n" + t[len(t)/2:] },
	"gcs2 prefix": func(t string) string { return "gcs2." + strings.TrimPrefix(t, "gcs1.") },
}

func editOf(name string) string { return strings.TrimPrefix(name, "token page 2 ") }

// gcsCompetingFeatures are requests with two or more refused features of
// one group. The Java server picks the reason among them by a set order
// randomized per JVM launch, so these cases compare everything but the
// x-swath-replay-error value, which must name one of the refused
// features (gcsFeatureReasons).
var gcsCompetingFeatures = map[string]string{
	"competing features versions softDeleted":    "versions=true&softDeleted=true&maxResults=1",
	"competing features all four booleans":       "versions=true&softDeleted=true&includeFoldersAsPrefixes=true&includeTrailingDelimiter=true",
	"competing features fields filter matchGlob": "fields=x&filter=x&matchGlob=x",
}

// gcsFeatureReasons is the reason header a competing-feature case may
// carry: feature_NAME for a feature its query refuses.
func gcsFeatureReasons(query string) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.Split(query, "&") {
		name, value, _ := strings.Cut(part, "=")
		if value != "false" && value != "" && name != "maxResults" {
			out["feature_"+name] = true
		}
	}
	return out
}

// competingFeatureOK passes a competing-feature case whose only
// difference is a reason header naming another of its refused features.
func competingFeatureOK(c request, ref, got *response, df diff) bool {
	if _, ok := gcsCompetingFeatures[c.Name]; !ok || df.OK || ref.Status != got.Status || df.Offset >= 0 || len(df.HeaderDiffs) != 1 {
		return false
	}
	allowed := gcsFeatureReasons(c.Query)
	return strings.HasPrefix(df.HeaderDiffs[0], "x-swath-replay-error:") &&
		allowed[values(ref.Header, "x-swath-replay-error")] && allowed[values(got.Header, "x-swath-replay-error")]
}

func trim(s string) string {
	if len(s) > 20 {
		return s[:10] + "..." + strconv.Itoa(len(s))
	}
	return s
}

// gcsWalks lists the token walks: the whole bucket and every top-level
// prefix, with and without the delimiter, and an offset window, at each
// page size.
func gcsWalks(keys [][]byte, pageSizes []int, maxTop int) []walk {
	var out []walk
	tops := topLevel(keys)
	if len(tops) > maxTop {
		tops = tops[:maxTop]
	}
	var window []string
	if len(keys) > 2 {
		window = []string{"startOffset", string(keys[len(keys)/3]), "endOffset", string(keys[2*len(keys)/3])}
	}
	for _, mr := range pageSizes {
		m := fmt.Sprint(mr)
		out = append(out,
			walk{"bucket maxResults=" + m, gcsQuery("maxResults", m)},
			walk{"bucket delimiter=/ maxResults=" + m, gcsQuery("delimiter", "/", "maxResults", m)},
			walk{"bucket full maxResults=" + m, gcsQuery("maxResults", m, "projection", "full")})
		if window != nil {
			out = append(out,
				walk{"window maxResults=" + m, gcsQuery(append([]string{"maxResults", m}, window...)...)},
				walk{"window delimiter=/ maxResults=" + m, gcsQuery(append([]string{"maxResults", m, "delimiter", "/"}, window...)...)})
		}
		for _, t := range tops {
			out = append(out,
				walk{"prefix=" + t + " maxResults=" + m, gcsQuery("prefix", t, "maxResults", m)},
				walk{"prefix=" + t + " delimiter=/ maxResults=" + m, gcsQuery("prefix", t, "delimiter", "/", "maxResults", m)})
		}
	}
	return out
}

// gcsRandomRequest draws one request from the key space.
func gcsRandomRequest(keys [][]byte, rng *rand.Rand, prefixes []string) request {
	var params []string
	if len(prefixes) > 0 && rng.IntN(3) > 0 {
		params = append(params, "prefix", prefixes[rng.IntN(len(prefixes))])
	}
	switch rng.IntN(4) {
	case 0:
		params = append(params, "delimiter", "/")
	case 1:
		params = append(params, "delimiter", []string{"-", "_", "/a", "0", "é"}[rng.IntN(5)])
	}
	offset := func() string {
		k := keys[rng.IntN(len(keys))]
		switch rng.IntN(4) {
		case 0:
			k = k[:rng.IntN(len(k)+1)]
			for len(k) > 0 && !validUTF8(k) {
				k = k[:len(k)-1]
			}
		case 1:
			k = append(bytes.Clone(k), byte('0'+rng.IntN(75)))
		}
		return string(k)
	}
	if len(keys) > 0 && rng.IntN(2) == 0 {
		params = append(params, "startOffset", offset())
	}
	if len(keys) > 0 && rng.IntN(3) == 0 {
		params = append(params, "endOffset", offset())
	}
	params = append(params, "maxResults", fmt.Sprint([]int{1, 1, 2, 3, 7, 50, 1000, 1001}[rng.IntN(8)]))
	if rng.IntN(4) == 0 {
		params = append(params, "projection", "full")
	}
	return request{Name: "random", Method: "GET", Path: gcsPath, Query: gcsQuery(params...)}
}

func validUTF8(b []byte) bool { return utf8.Valid(b) }

func gcsMultiByteDelimiter(keys [][]byte, rng *rand.Rand) string {
	for range 100 {
		d := multiByteDelimiter(keys, rng)
		if validUTF8([]byte(d)) {
			return d
		}
	}
	return "/x"
}

// gcsKeysNeedingEscape is up to n keys with a byte a client must escape
// other than '/'.
func gcsKeysNeedingEscape(keys [][]byte, n int) [][]byte {
	var out [][]byte
	for _, k := range keys {
		if strings.Contains(gcsEscape(strings.ReplaceAll(string(k), "/", "")), "%") {
			out = append(out, k)
			if len(out) == n {
				break
			}
		}
	}
	return out
}
