// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command replaydiff is the replay servers' differential driver. It starts
// a reference server (the Java swath-replay launcher) and the Go
// swath-replay on the same fixture and bucket, sends both the same
// requests, and compares the answers: status, the compared headers
// (Content-Type, x-amz-*, x-swath-replay-*) and the body byte for byte.
//
// The requests are a scripted matrix built from the fixture's keys
// (max-keys edge values, prefixes at every depth, delimiters, start-after
// around keys and prefix boundaries, encoding, fetch-owner, tokens good
// and bad, wrong buckets and methods), full token walks of the bucket and
// of every top-level prefix, and a seeded random walk.
//
//	go run ./bench/replaydiff -java $JAVA_REPLAY -go ./swath-replay \
//	  -fixture /data/sorted -bucket bkt -report /tmp/diff.json
//
// With -live-endpoint https://BUCKET.s3.REGION.amazonaws.com the reference
// is live S3 instead (anonymous, the scripted matrix only, compared
// structurally with tokens masked); live buckets change, so treat a
// difference there as a lead to check, not a verdict. With -live-record
// the endpoint is bench/faultproxy -record in front of the provider: the
// whole live run is recorded once, annotated, and -capture replays it
// offline under the same comparison.
//
// -dump-tsv FILE writes the reference's full listing as a memstore TSV
// fixture and exits.
//
// -protocol azure compares the Azure List Blobs route instead (both servers
// serve s3,azure): the matrix in azure.go, marker walks in which each
// server follows its own markers, and a structural comparator.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

type options struct {
	java, goBin, fixture, goFixture, bucket, report, live, dumpTSV string
	protocol                                                       string
	javaArgs, goArgs, logDir                                       string
	walkSizes                                                      string
	random, parallel, walkLimit, maxPrefixes, maxTop               int
	seed                                                           uint64
	acceptSegments                                                 bool
	livePrefix, stableBefore, gcsMeta                              string
	liveScripted, liveRecord                                       bool
	liveWalkPages                                                  int
	capture                                                        string
	captureLimits                                                  captureLimits
	maskMetadata                                                   bool
	pins                                                           string
}

func main() {
	var o options
	flag.StringVar(&o.java, "java", "", "the Java swath-replay launcher (JAVA_HOME from the environment)")
	flag.StringVar(&o.goBin, "go", "", "the Go swath-replay binary")
	flag.StringVar(&o.fixture, "fixture", "", "the fixture both servers serve")
	flag.StringVar(&o.goFixture, "go-fixture", "", "a different fixture path for the Go server (e.g. a .tsv dump of the same listing)")
	flag.StringVar(&o.protocol, "protocol", "s3", "the route to compare: s3, gcs (both servers get --protocols gcs) or azure (both get --protocols s3,azure)")
	flag.StringVar(&o.bucket, "bucket", "bucket", "the bucket name")
	flag.StringVar(&o.javaArgs, "java-args", "--parquet-connections 2", "extra serve flags for the Java server")
	flag.StringVar(&o.goArgs, "go-args", "--parquet-connections 2", "extra serve flags for the Go server")
	flag.StringVar(&o.report, "report", "", "write the JSON report here and the text summary next to it (.txt)")
	flag.StringVar(&o.live, "live-endpoint", "", "compare with the live provider at this endpoint instead of Java: "+
		"https://s3.REGION.amazonaws.com (path-style) or https://BUCKET.s3.REGION.amazonaws.com for S3, https://storage.googleapis.com for GCS")
	flag.StringVar(&o.livePrefix, "live-prefix", "", "live: the scope every request stays under (the fixture's recorded prefix)")
	flag.StringVar(&o.stableBefore, "live-stable-before", "", "live: refuse to run unless every fixture name under the scope was modified before this RFC 3339 time")
	flag.StringVar(&o.gcsMeta, "gcs-metadata", "auto", "live gcs: compare item metadata as native (exact) or synthetic (masked); auto reads the fixture")
	flag.BoolVar(&o.liveScripted, "live-scripted", false, "live: also send the driver's own scripted matrix")
	flag.BoolVar(&o.liveRecord, "live-record", false, "live: the endpoint is a bench/faultproxy -record proxy in front of the provider; "+
		"each request names its case and how its token was formed in X-Record-* headers the proxy records and strips, for replaydiff -capture")
	flag.StringVar(&o.pins, "live-pins", "", "live and capture: a replay-expectations.json of the provider evidence; a difference whose message is its case's pinned mismatch is a documented difference")
	flag.BoolVar(&o.maskMetadata, "gcs-mask-metadata", false, "gcs: compare with each item's metadata fields removed (a native fixture on the Go server against the same rows, S3-shaped, on the Java server)")
	flag.StringVar(&o.capture, "capture", "", "replay a bench/faultproxy -record capture against the Go server on -fixture and compare with the captured answers")
	flag.Int64Var(&o.captureLimits.maxBody, "capture-max-body", 16<<20, "capture: the largest captured body in `bytes`, as recorded; an exchange over it fails")
	flag.Int64Var(&o.captureLimits.maxDecoded, "capture-max-decoded", 64<<20, "capture: the largest body in `bytes` once its content coding is undone; an exchange over it fails")
	flag.IntVar(&o.captureLimits.maxTokens, "capture-max-tokens", 1<<20, "capture: the most page tokens remembered for walks still to be replayed; past it the replay fails and stops")
	flag.IntVar(&o.captureLimits.maxDetails, "capture-max-details", 1000, "capture: the most failures, and separately the most skipped exchanges, the report details; past it they are only counted and the report says how many are not shown")
	flag.IntVar(&o.liveWalkPages, "live-walk-pages", 300, "live: walk a prefix at a page size only when the walk takes at most this many pages")
	flag.StringVar(&o.dumpTSV, "dump-tsv", "", "write the reference listing as a TSV fixture here and exit")
	flag.StringVar(&o.logDir, "log-dir", os.TempDir(), "where the servers' stderr logs go")
	flag.StringVar(&o.walkSizes, "walks", "1,7,1000", "page sizes of the token walks; empty for none")
	flag.IntVar(&o.walkLimit, "walk-limit", 0, "stop each walk after this many pages (0: walk to the end)")
	flag.IntVar(&o.maxPrefixes, "max-prefixes", 200, "prefixes sampled for the scripted matrix")
	flag.IntVar(&o.maxTop, "max-top", 50, "top-level prefixes walked")
	flag.IntVar(&o.random, "random", 500, "requests in the seeded random walk")
	flag.Uint64Var(&o.seed, "seed", 1, "the random walk's seed")
	flag.IntVar(&o.parallel, "parallel", 4, "requests in flight")
	flag.BoolVar(&o.acceptSegments, "accept-segment-improvement", false,
		"azure: count a reference 500 too_many_segments answered 200 by the Go server as the known improvement, not a difference")
	flag.Parse()
	if err := run(o); err != nil {
		log.Fatal(err)
	}
}

// target is one side of the comparison: a base URL and how the request
// path is formed.
type target struct {
	name, base string
	vhost      bool // live S3: the bucket is in the host name
	// recorder: the base is a bench/faultproxy -record proxy in front of
	// the provider (-live-record). Requests may carry X-Record-*
	// annotations, and the X-Content-Type-Options the proxy sets on every
	// answer it releases is removed: it is the proxy's, not the
	// provider's (the recording keeps the provider's headers as sent).
	recorder bool
	client   *http.Client
}

func (t *target) url(bucket string, r request) string {
	if r.Raw != "" {
		return t.base + " " + rawTarget(t, bucket, r)
	}
	path := r.Path
	switch {
	case t.vhost:
		path = "/"
	case path == "":
		path = "/" + bucket
	default:
		path = strings.ReplaceAll(path, "{bucket}", bucket)
	}
	if r.Query == "" {
		return t.base + path
	}
	return t.base + path + "?" + r.Query
}

func (t *target) do(bucket string, r request) (*response, error) {
	if r.Wire != nil {
		return t.doWire(bucket, r)
	}
	if r.Raw != "" {
		return t.doRaw(bucket, r)
	}
	req, err := http.NewRequest(r.Method, t.url(bucket, r), nil)
	if err != nil {
		return nil, err
	}
	for _, h := range r.Header {
		req.Header[h[0]] = append(req.Header[h[0]], h[1])
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if len(resp.TransferEncoding) > 0 {
		// The framing Go's client lifts out of the header map, put back
		// for the live modes' header classification.
		resp.Header["Transfer-Encoding"] = []string{strings.Join(resp.TransferEncoding, ", ")}
	}
	if t.recorder {
		resp.Header.Del("X-Content-Type-Options")
	}
	return &response{Status: resp.StatusCode, Header: resp.Header, Body: body}, err
}

// rawTarget fills r's raw target in: {bucket}, and {host} with the
// target's own host and port (the Host header the request carries).
func rawTarget(t *target, bucket string, r request) string {
	return strings.NewReplacer("{bucket}", bucket, "{host}", strings.TrimPrefix(t.base, "http://")).Replace(r.Raw)
}

// wireDeadline bounds a byte-stream request's whole connection.
var wireDeadline = time.Minute

// doWire sends r's whole byte stream on a connection of its own and
// returns the last answer read before the connection ends. Each answer
// must be read whole: a body cut short (fewer bytes than its
// Content-Length, a broken chunk; a body framed by the connection's end
// is whole when it ends) is an error, as is a successor answer
// that starts and does not parse, or a connection still open at the
// deadline. Only a connection that ends (EOF or reset) before any byte of
// a further answer ends the stream cleanly.
func (t *target) doWire(bucket string, r request) (*response, error) {
	host := strings.TrimPrefix(t.base, "http://")
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(wireDeadline))
	go io.WriteString(conn, r.Wire(bucket, host))
	br := bufio.NewReader(conn)
	var last *response
	for {
		if _, err := br.Peek(1); err != nil && last != nil && (err == io.EOF || errors.Is(err, syscall.ECONNRESET)) {
			return last, nil
		}
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			if last == nil {
				return nil, err
			}
			return nil, fmt.Errorf("after answer %d %s: %w", last.Status, http.StatusText(last.Status), err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil && !(closeDelimited(resp) && errors.Is(err, syscall.ECONNRESET)) {
			return nil, fmt.Errorf("answer %d: its body: %w", resp.StatusCode, err)
		}
		last = &response{Status: resp.StatusCode, Header: resp.Header, Body: body}
	}
}

// closeDelimited reports whether resp's body ends where the connection
// does (no Content-Length, not chunked), as net/http's answer to a
// request it cannot parse is framed. A reset ends such a body as an EOF
// would: the framing cannot tell a cut one from a whole one. (A server
// that closes with the client's bytes unread resets the connection.)
func closeDelimited(resp *http.Response) bool {
	return resp.ContentLength < 0 && len(resp.TransferEncoding) == 0
}

// doRaw sends r's raw target as the request line on a connection of its
// own and reads the one answer.
func (t *target) doRaw(bucket string, r request) (*response, error) {
	host := strings.TrimPrefix(t.base, "http://")
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Minute))
	target := rawTarget(t, bucket, r)
	var extra strings.Builder
	for _, h := range r.Header {
		extra.WriteString(h[0] + ": " + h[1] + "\r\n")
	}
	if r.BucketPad > 0 {
		extra.WriteString("X-Pad: " + strings.Repeat("a", max(0, r.BucketPad-len(bucket))) + "\r\n")
	}
	if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: %s\r\n%sConnection: close\r\n\r\n", r.Method, target, host, extra.String()); err != nil {
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: r.Method})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return &response{Status: resp.StatusCode, Header: resp.Header, Body: body}, err
}

// result is one compared request (or one walk) in the report.
type result struct {
	Kind    string `json:"kind"` // case, walk, random
	Name    string `json:"name"`
	Method  string `json:"method"`
	Target  string `json:"target"`
	Pass    bool   `json:"pass"`
	Pages   int    `json:"pages,omitempty"` // walks
	Error   string `json:"error,omitempty"`
	FailAt  int    `json:"fail_at_page,omitempty"`
	Partial bool   `json:"partial,omitempty"` // a walk stopped by -walk-limit
	// Skipped, set by a case that depends on another's token when that
	// answer had none, keeps it out of the results: it was not sent, and
	// the report lists it under skipped, neither passed nor failed.
	Skipped string `json:"skipped,omitempty"`
	*diff
}

type report struct {
	Reference string    `json:"reference"`
	Fixture   string    `json:"fixture"`
	GoFixture string    `json:"go_fixture,omitempty"`
	Bucket    string    `json:"bucket"`
	Seed      uint64    `json:"seed"`
	Started   time.Time `json:"started"`
	Seconds   float64   `json:"seconds"`
	Keys      int       `json:"keys"`
	Requests  int       `json:"requests"`
	Compared  int       `json:"compared"`
	Passed    int       `json:"passed"`
	Failed    int       `json:"failed"`
	// Skipped names the cases that could not run as designed (a token
	// case whose source page issued no token), with the reason: missing
	// coverage, never a pass. SkippedCount counts them all, those past
	// the detail limit too.
	Skipped      []string `json:"skipped,omitempty"`
	SkippedCount int      `json:"skipped_count"`
	// FailuresNotShown and SkipsNotShown count the failures and the
	// skips past the driver's detail limit (a capture's
	// -capture-max-details): counted in Failed and SkippedCount, their
	// details dropped so the report stays bounded however many there are.
	FailuresNotShown int `json:"failures_not_shown,omitempty"`
	SkipsNotShown    int `json:"skips_not_shown,omitempty"`
	// TokenBytes names the walk pages that agree in meaning but whose
	// page tokens differ in bytes: a wire difference, reported apart
	// from the walk's verdict.
	TokenBytes []string `json:"token_byte_differences,omitempty"`
	// Divergences are the requests the Go S3 route answers as live S3
	// does and the Java server does not (s3_divergences.go): compared on
	// the Go side, listed with what each server answered.
	Divergences []string `json:"documented_divergences,omitempty"`
	// Residuals are the documented differences the driver sends anyway so
	// they stay visible (Compare "residual"), with what each server
	// answered. They are not in Compared, Passed or Failed; a residual
	// request that gets no answer is a failure, not a residual.
	// The live and capture modes list here the differences
	// documentedDifference matches, with the reason, also outside the
	// verdict counts. ResidualsNotShown counts those past the driver's
	// detail limit.
	Residuals         []string `json:"documented_residuals,omitempty"`
	ResidualsNotShown int      `json:"residuals_not_shown,omitempty"`
	// Scope, Newest and Coverage are the live modes': the prefix every
	// request stayed under, the newest modification time the fixture
	// holds there, and each provider matrix case with its query or why it
	// was not sent.
	Scope    string   `json:"scope,omitempty"`
	Newest   string   `json:"newest_modified,omitempty"`
	Coverage []string `json:"coverage,omitempty"`
	Results  []result `json:"results"`
}

func run(o options) error {
	if o.capture != "" {
		return runCapture(o)
	}
	if o.live != "" && o.dumpTSV == "" {
		if o.walkSizes == "1,7,1000" {
			o.walkSizes = "7,1000"
		}
		if o.walkLimit == 0 {
			o.walkLimit = 2000 // a role bound to a large directory walks at most this far
		}
		return runLive(o)
	}
	if o.protocol == "gcs" {
		return runGCS(o)
	}
	ctx := context.Background()
	if o.goBin == "" && o.dumpTSV == "" || o.java == "" && o.live == "" || o.fixture == "" && o.live == "" {
		flag.Usage()
		return fmt.Errorf("-go, -java (or -live-endpoint) and -fixture are required")
	}
	if o.protocol == "azure" {
		o.javaArgs += azureServeArgs
		o.goArgs += azureServeArgs
		if o.walkSizes == "1,7,1000" {
			o.walkSizes = azureWalkSizes
		}
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: o.parallel * 2}, Timeout: 5 * time.Minute}
	var ref *target
	var comp comparator = bytesComparator{}
	if o.live != "" {
		ref = &target{name: "live", base: strings.TrimSuffix(o.live, "/"), vhost: true, client: liveClient(o.parallel)}
	} else {
		java, err := startServer(ctx, "java", o.java, serveArgs(o.fixture, o.bucket, o.javaArgs), filepath.Join(o.logDir, "replaydiff-java.log"))
		if err != nil {
			return err
		}
		defer java.stop()
		ref = &target{name: "java", base: java.endpoint, client: client}
	}
	keys, objs, err := listAll(ref, o.bucket)
	if err != nil {
		return fmt.Errorf("listing the reference: %w", err)
	}
	if o.dumpTSV != "" {
		return dump(o.dumpTSV, objs)
	}
	goFixture := o.fixture
	if o.goFixture != "" {
		goFixture = o.goFixture
	}
	goSrv, err := startServer(ctx, "go", o.goBin, serveArgs(goFixture, o.bucket, o.goArgs), filepath.Join(o.logDir, "replaydiff-go.log"))
	if err != nil {
		return err
	}
	defer goSrv.stop()
	got := &target{name: "go", base: goSrv.endpoint, client: client}

	rep := &report{Reference: ref.name, Fixture: o.fixture, GoFixture: o.goFixture, Bucket: o.bucket, Seed: o.seed, Started: time.Now(), Keys: len(keys)}
	rng := rand.New(rand.NewPCG(o.seed, 0x5eed))
	d := &driver{o: o, ref: ref, got: got, comp: comp, rep: rep}

	if o.protocol == "azure" {
		d.runAzure(keys, rng)
		rep.Seconds = time.Since(rep.Started).Seconds()
		return d.write()
	}
	d.runCases("case", append(s3Matrix(keys, rng, o.maxPrefixes), s3DivergenceCases(keys)...))
	{
		var sizes []int
		for _, s := range strings.Split(o.walkSizes, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
				sizes = append(sizes, n)
			}
		}
		d.runWalks(s3Walks(keys, sizes, o.maxTop))
		prefixes := directoryPrefixes(keys)
		random := make([]request, o.random)
		for i := range random {
			random[i] = randomRequest(keys, rng, prefixes)
			random[i].Name = fmt.Sprintf("random %d", i)
		}
		d.runCases("random", random)
	}
	rep.Seconds = time.Since(rep.Started).Seconds()
	return d.write()
}

func serveArgs(fixture, bucket, extra string) []string {
	args := []string{"--fixture", fixture, "--bucket", bucket, "--host", "127.0.0.1", "--port", "0", "--metrics-port", "0"}
	if !strings.Contains(extra, "--serving-mode") {
		args = append(args, "--serving-mode", "sorted")
	}
	return append(args, strings.Fields(extra)...)
}

type driver struct {
	o         options
	ref, got  *target
	comp      comparator
	mu        sync.Mutex
	rep       *report
	refTokens sync.Map // request name -> its reference answer's next token
	// tokenNames are the names already listed in TokenBytes.
	tokenNames map[string]bool
	// countPasses keeps passing results out of the report's list (they
	// are counted): a capture's report then grows with its failures,
	// not with every exchange.
	countPasses bool
	// maxDetails, when positive, bounds the failures and, apart from
	// them, the skips the report details; the rest are counted only. A
	// capture of any length full of failures then keeps its report, and
	// the memory it takes, to this many entries.
	maxDetails int
	// pins are the provider evidence's pinned mismatches by case id
	// (-live-pins), for a capture's annotated cases.
	pins map[string]string
}

func (d *driver) record(r result) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if r.Skipped != "" {
		d.addSkip(func() string { return fmt.Sprintf("%s %q: %s", r.Kind, r.Name, r.Skipped) })
		return
	}
	d.rep.Compared++
	switch {
	case r.Pass:
		d.rep.Passed++
		if !d.countPasses {
			d.rep.Results = append(d.rep.Results, r)
		}
	case d.maxDetails > 0 && d.rep.Failed-d.rep.FailuresNotShown >= d.maxDetails:
		d.rep.Failed++
		d.rep.FailuresNotShown++
	default:
		d.rep.Failed++
		d.rep.Results = append(d.rep.Results, r)
	}
}

// addSkip counts a skip and keeps its detail while under the detail
// limit; d.mu is held.
func (d *driver) addSkip(detail func() string) {
	d.rep.SkippedCount++
	if d.maxDetails > 0 && len(d.rep.Skipped) >= d.maxDetails {
		d.rep.SkipsNotShown++
		return
	}
	d.rep.Skipped = append(d.rep.Skipped, detail())
}

func (d *driver) count(n int) {
	d.mu.Lock()
	d.rep.Requests += n
	d.mu.Unlock()
}

// compareOne sends r to both servers and compares the answers.
func (d *driver) compareOne(r request) (*response, diff, error) {
	if d.s3DivergenceRef(r) {
		return d.compareS3(r)
	}
	a, err := d.ref.do(d.o.bucket, r)
	if err != nil {
		return nil, diff{}, fmt.Errorf("%s: %w", d.ref.name, err)
	}
	b, err := d.got.do(d.o.bucket, r)
	if err != nil {
		return a, diff{}, fmt.Errorf("%s: %w", d.got.name, err)
	}
	d.count(2)
	return a, narrow(r.Compare, d.comp.compare(a, b), a, b), nil
}

// noteResidual records what the two servers answered to a documented
// residual. It is neither compared, passed nor failed.
func (d *driver) noteResidual(name string, df diff) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rep.Residuals = append(d.rep.Residuals, fmt.Sprintf("%s: reference %d, go %d", name, df.Status[0], df.Status[1]))
}

// noteDocumented lists a live or captured answer whose difference is a
// documented one (documentedDifference), with the reason. Like a
// residual it is neither compared, passed nor failed; past the detail
// limit it is counted only.
func (d *driver) noteDocumented(name, why string, df diff) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.maxDetails > 0 && len(d.rep.Residuals) >= d.maxDetails {
		d.rep.ResidualsNotShown++
		return
	}
	d.rep.Residuals = append(d.rep.Residuals, fmt.Sprintf("%s: %s (provider %d, go %d)", name, why, df.Status[0], df.Status[1]))
}

// narrow applies a request's Compare mode to a verdict: "headers" sets the
// body aside and adds Cache-Control to the compared headers; "status"
// keeps the status alone.
func narrow(mode string, df diff, a, b *response) diff {
	switch mode {
	case "headers":
		df.Offset, df.RefExcerpt, df.GotExcerpt = -1, "", ""
		if x, y := values(a.Header, "Cache-Control"), values(b.Header, "Cache-Control"); x != y {
			df.HeaderDiffs = append(df.HeaderDiffs, fmt.Sprintf("cache-control: %q | %q", x, y))
		}
		df.OK = df.Status[0] == df.Status[1] && len(df.HeaderDiffs) == 0
	case "status":
		df.Offset, df.RefExcerpt, df.GotExcerpt, df.HeaderDiffs = -1, "", "", nil
		df.OK = df.Status[0] == df.Status[1]
	}
	return df
}

func (d *driver) runCases(kind string, cases []request) {
	// Requests that take a token from another wait for it; run those last.
	var first, second []request
	for _, c := range cases {
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
							d.record(result{Kind: kind, Name: c.Name, Method: c.Method, Target: d.got.url(d.o.bucket, c),
								Skipped: fmt.Sprintf("%q issued no continuation token on this fixture", c.TokenFrom)})
							continue
						}
						c.Query += "&continuation-token=" + s3.PercentEncode([]byte(tok.(string)))
					}
					res := result{Kind: kind, Name: c.Name, Method: c.Method, Target: d.got.url(d.o.bucket, c)}
					a, df, err := d.compareOne(c)
					if c.Compare == "residual" && err == nil {
						// A documented difference: listed with what each
						// server answered, outside the verdict counts. A
						// transport error is not a documented residual: it
						// falls through and fails the run.
						d.noteResidual(c.Name, df)
						continue
					}
					if err != nil {
						res.Error = err.Error()
					} else {
						res.Pass, res.diff = df.OK, &df
						if tok := nextToken(a.Body); tok != "" {
							d.refTokens.Store(c.Name, tok)
						}
					}
					if res.Pass {
						res.diff = nil // keep passing entries short
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

func (d *driver) runWalks(walks []walk) {
	work := make(chan walk)
	var wg sync.WaitGroup
	for range d.o.parallel {
		wg.Go(func() {
			for w := range work {
				d.record(d.walkOne(w))
			}
		})
	}
	for _, w := range walks {
		work <- w
	}
	close(work)
	wg.Wait()
}

// walkOne follows the reference's tokens to the end of the listing,
// comparing every page; the first difference ends the walk.
func (d *driver) walkOne(w walk) result {
	res := result{Kind: "walk", Name: w.Name, Method: "GET", Pass: true}
	query := w.Query
	for {
		r := request{Name: w.Name, Method: "GET", Query: query}
		if res.Pages > 0 {
			r.TokenFrom = w.Name // the query carries the reference's token
		}
		res.Target = d.got.url(d.o.bucket, r)
		a, df, err := d.compareOne(r)
		res.Pages++
		if err != nil || !df.OK {
			res.Pass, res.FailAt = false, res.Pages
			if err != nil {
				res.Error = err.Error()
			} else {
				res.diff = &df
			}
			return res
		}
		tok := nextToken(a.Body)
		if tok == "" {
			return res
		}
		if d.o.walkLimit > 0 && res.Pages >= d.o.walkLimit {
			res.Partial = true
			return res
		}
		query = w.Query + "&continuation-token=" + s3.PercentEncode([]byte(tok))
	}
}

var nextTokenRe = regexp.MustCompile(`<NextContinuationToken>([^<]*)</NextContinuationToken>`)

func nextToken(body []byte) string {
	m := nextTokenRe.FindSubmatch(body)
	if m == nil {
		return ""
	}
	return xmlUnescape(string(m[1]))
}

var xmlUnescaper = strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&apos;", "'")

func xmlUnescape(s string) string { return xmlUnescaper.Replace(s) }

// listPage is the part of a ListBucketResult the driver reads.
type listPage struct {
	NextContinuationToken string
	Contents              []struct {
		Key               string
		LastModified      *string
		ETag              *string
		Size              *int64
		StorageClass      *string
		ChecksumAlgorithm *string
		ChecksumType      *string
		Owner             *struct {
			ID          *string
			DisplayName *string
		}
	}
}

// listAll walks the reference's whole listing (encoding-type=url, so keys
// are exact bytes; fetch-owner, so the owner columns come along).
func listAll(t *target, bucket string) ([][]byte, []store.Object, error) {
	var keys [][]byte
	var objs []store.Object
	base := q("list-type", "2", "max-keys", "1000", "encoding-type", "url", "fetch-owner", "true")
	query := base
	for {
		resp, err := t.do(bucket, request{Method: "GET", Query: query})
		if err != nil {
			return nil, nil, err
		}
		if resp.Status != 200 {
			return nil, nil, fmt.Errorf("status %d: %.300s", resp.Status, resp.Body)
		}
		var page listPage
		if err := xml.Unmarshal(resp.Body, &page); err != nil {
			return nil, nil, err
		}
		for _, c := range page.Contents {
			k := listedKey(c.Key)
			keys = append(keys, k)
			o := store.Object{Key: k, Size: c.Size, StorageClass: c.StorageClass, ChecksumAlgorithm: c.ChecksumAlgorithm, ChecksumType: c.ChecksumType}
			if c.ETag != nil {
				e := strings.TrimSuffix(strings.TrimPrefix(*c.ETag, `"`), `"`)
				o.ETag = &e
			}
			if c.LastModified != nil {
				if tm, err := time.Parse("2006-01-02T15:04:05.000Z", *c.LastModified); err == nil {
					o.LastModified = &tm
				}
			}
			if c.Owner != nil {
				o.OwnerID, o.OwnerDisplayName = c.Owner.ID, c.Owner.DisplayName
			}
			objs = append(objs, o)
		}
		if page.NextContinuationToken == "" {
			return keys, objs, nil
		}
		query = base + "&continuation-token=" + s3.PercentEncode([]byte(page.NextContinuationToken))
	}
}

// listedKey decodes a name an encoding-type=url listing carries. S3 and
// the Go server write a space as '+' and a '+' as %2B; the Java server
// escapes '+' itself, so reading '+' as a space suits all three. A
// percent-decoder that keeps '+' (s3.PercentDecode, the request
// grammar's) would turn "a b/1" into "a+b/1".
func listedKey(s string) []byte {
	return s3.PercentDecode(strings.ReplaceAll(s, "+", "%20"))
}

func dump(path string, objs []store.Object) error {
	buf := []byte("# " + memstore.Columns + "\n")
	for i := range objs {
		buf = memstore.AppendTSV(buf, &objs[i])
	}
	return os.WriteFile(path, buf, 0o644)
}

func (d *driver) write() error {
	rep := d.rep
	text := d.summary()
	os.Stdout.Write(text)
	if d.o.report != "" {
		j, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(d.o.report, j, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(strings.TrimSuffix(d.o.report, filepath.Ext(d.o.report))+".txt", text, 0o644); err != nil {
			return err
		}
	}
	if rep.Failed > 0 {
		return fmt.Errorf("%d differences", rep.Failed)
	}
	return nil
}

// summary is the report's text form.
func (d *driver) summary() []byte {
	rep := d.rep
	var text bytes.Buffer
	fmt.Fprintf(&text, "replaydiff: reference=%s fixture=%s bucket=%s keys=%d seed=%d\n", rep.Reference, rep.Fixture, rep.Bucket, rep.Keys, rep.Seed)
	kinds := map[string][2]int{}
	for _, r := range rep.Results {
		k := kinds[r.Kind]
		if r.Pass {
			k[0]++
		} else {
			k[1]++
		}
		kinds[r.Kind] = k
	}
	if rep.Coverage != nil {
		sent := 0
		for _, c := range rep.Coverage {
			if strings.Contains(c, ": sent ") {
				sent++
			}
		}
		fmt.Fprintf(&text, "  scope %q (newest name modified %s): %d of %d provider matrix cases sent\n", rep.Scope, rep.Newest, sent, len(rep.Coverage))
		for _, c := range rep.Coverage {
			if !strings.Contains(c, ": sent ") {
				fmt.Fprintf(&text, "  NOT SENT %s\n", c)
			} else if strings.Contains(c, " [reduced shape: ") {
				fmt.Fprintf(&text, "  REDUCED SHAPE %s\n", c)
			}
		}
	}
	for _, kind := range []string{"provider", "case", "walk", "random"} {
		if k, ok := kinds[kind]; ok {
			fmt.Fprintf(&text, "  %-6s %6d passed %6d failed\n", kind, k[0], k[1])
		}
	}
	partial := 0
	for _, r := range rep.Results {
		if r.Partial {
			partial++
		}
		if r.Pass {
			continue
		}
		fmt.Fprintf(&text, "FAIL %s %q %s %s", r.Kind, r.Name, r.Method, r.Target)
		if r.FailAt > 0 {
			fmt.Fprintf(&text, " (page %d)", r.FailAt)
		}
		fmt.Fprintln(&text)
		if r.Error != "" {
			fmt.Fprintf(&text, "     error: %s\n", r.Error)
		}
		if r.diff != nil {
			fmt.Fprintf(&text, "     status %d vs %d; first body difference at byte %d %s\n", r.Status[0], r.Status[1], r.Offset, r.Detail)
			for _, h := range r.HeaderDiffs {
				fmt.Fprintf(&text, "     header %s\n", h)
			}
			if r.Offset >= 0 {
				fmt.Fprintf(&text, "     ref %s\n     got %s\n", r.RefExcerpt, r.GotExcerpt)
			}
		}
	}
	if partial > 0 {
		fmt.Fprintf(&text, "%d walks stopped at -walk-limit %d pages\n", partial, d.o.walkLimit)
	}
	if rep.FailuresNotShown > 0 {
		fmt.Fprintf(&text, "%d more failures not shown (-capture-max-details %d)\n", rep.FailuresNotShown, d.maxDetails)
	}
	for _, s := range rep.Skipped {
		fmt.Fprintf(&text, "SKIP %s\n", s)
	}
	if rep.SkipsNotShown > 0 {
		fmt.Fprintf(&text, "%d more skips not shown (-capture-max-details %d)\n", rep.SkipsNotShown, d.maxDetails)
	}
	for _, s := range rep.TokenBytes {
		fmt.Fprintf(&text, "TOKEN BYTES DIFFER %s\n", s)
	}
	for _, s := range rep.Residuals {
		fmt.Fprintf(&text, "DOCUMENTED RESIDUAL %s\n", s)
	}
	if rep.ResidualsNotShown > 0 {
		fmt.Fprintf(&text, "%d more documented residuals not shown (-capture-max-details %d)\n", rep.ResidualsNotShown, d.maxDetails)
	}
	for _, s := range sortedDivergences(rep.Divergences) {
		fmt.Fprintf(&text, "DOCUMENTED DIVERGENCE %s\n", s)
	}
	if len(rep.Divergences) > 0 {
		fmt.Fprintf(&text, "%d documented divergences (answered as live S3), %d requests whose answers differ only in token bytes\n",
			len(rep.Divergences), len(rep.TokenBytes))
	}
	fmt.Fprintf(&text, "%d compared (%d HTTP requests) in %.1fs: %d passed, %d failed, %d skipped, %d documented residuals",
		rep.Compared, rep.Requests, rep.Seconds, rep.Passed, rep.Failed, rep.SkippedCount, len(rep.Residuals)+rep.ResidualsNotShown)
	if n := rep.FailuresNotShown + rep.SkipsNotShown; n > 0 {
		fmt.Fprintf(&text, " (%d failures and %d skips not shown)", rep.FailuresNotShown, rep.SkipsNotShown)
	}
	text.WriteByte('\n')
	return text.Bytes()
}
