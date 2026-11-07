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
// difference there as a lead to check, not a verdict.
//
// -dump-tsv FILE writes the reference's full listing as a memstore TSV
// fixture and exits.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
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
	"time"

	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

type options struct {
	java, goBin, fixture, goFixture, bucket, report, live, dumpTSV string
	javaArgs, goArgs, logDir                                       string
	walkSizes                                                      string
	random, parallel, walkLimit, maxPrefixes, maxTop               int
	seed                                                           uint64
}

func main() {
	var o options
	flag.StringVar(&o.java, "java", "", "the Java swath-replay launcher (JAVA_HOME from the environment)")
	flag.StringVar(&o.goBin, "go", "", "the Go swath-replay binary")
	flag.StringVar(&o.fixture, "fixture", "", "the fixture both servers serve")
	flag.StringVar(&o.goFixture, "go-fixture", "", "a different fixture path for the Go server (e.g. a .tsv dump of the same listing)")
	flag.StringVar(&o.bucket, "bucket", "bucket", "the bucket name")
	flag.StringVar(&o.javaArgs, "java-args", "--parquet-connections 2", "extra serve flags for the Java server")
	flag.StringVar(&o.goArgs, "go-args", "--parquet-connections 2", "extra serve flags for the Go server")
	flag.StringVar(&o.report, "report", "", "write the JSON report here and the text summary next to it (.txt)")
	flag.StringVar(&o.live, "live-endpoint", "", "compare with live S3 at this virtual-host endpoint instead of Java")
	flag.StringVar(&o.dumpTSV, "dump-tsv", "", "write the reference listing as a TSV fixture here and exit")
	flag.StringVar(&o.logDir, "log-dir", os.TempDir(), "where the servers' stderr logs go")
	flag.StringVar(&o.walkSizes, "walks", "1,7,1000", "page sizes of the token walks; empty for none")
	flag.IntVar(&o.walkLimit, "walk-limit", 0, "stop each walk after this many pages (0: walk to the end)")
	flag.IntVar(&o.maxPrefixes, "max-prefixes", 200, "prefixes sampled for the scripted matrix")
	flag.IntVar(&o.maxTop, "max-top", 50, "top-level prefixes walked")
	flag.IntVar(&o.random, "random", 500, "requests in the seeded random walk")
	flag.Uint64Var(&o.seed, "seed", 1, "the random walk's seed")
	flag.IntVar(&o.parallel, "parallel", 4, "requests in flight")
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
	client     *http.Client
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
	if r.Raw != "" {
		return t.doRaw(bucket, r)
	}
	req, err := http.NewRequest(r.Method, t.url(bucket, r), nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return &response{Status: resp.StatusCode, Header: resp.Header, Body: body}, err
}

// rawTarget fills r's raw target in: {bucket}, and {host} with the
// target's own host and port (the Host header the request carries).
func rawTarget(t *target, bucket string, r request) string {
	return strings.NewReplacer("{bucket}", bucket, "{host}", strings.TrimPrefix(t.base, "http://")).Replace(r.Raw)
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
	if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", r.Method, target, host); err != nil {
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
	// Skipped is a request that depends on another's token when that
	// answer had none: it was not sent, and it neither passes nor fails.
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
	Skipped   int       `json:"skipped"`
	Results   []result  `json:"results"`
}

func run(o options) error {
	ctx := context.Background()
	if o.goBin == "" && o.dumpTSV == "" || o.java == "" && o.live == "" || o.fixture == "" && o.live == "" {
		flag.Usage()
		return fmt.Errorf("-go, -java (or -live-endpoint) and -fixture are required")
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: o.parallel * 2}, Timeout: 5 * time.Minute}
	var ref *target
	var comp comparator = bytesComparator{}
	if o.live != "" {
		ref = &target{name: "live", base: strings.TrimSuffix(o.live, "/"), vhost: true, client: client}
		comp = liveComparator{}
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

	cases := s3Matrix(keys, rng, o.maxPrefixes)
	if o.live != "" {
		live := cases[:0]
		for _, c := range cases {
			if c.Live {
				live = append(live, c)
			}
		}
		cases = live
	}
	d.runCases("case", cases)
	if o.live == "" {
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
	args := []string{"--fixture", fixture, "--bucket", bucket, "--host", "127.0.0.1", "--port", "0", "--metrics-port", "0", "--serving-mode", "sorted"}
	return append(args, strings.Fields(extra)...)
}

type driver struct {
	o         options
	ref, got  *target
	comp      comparator
	mu        sync.Mutex
	rep       *report
	refTokens sync.Map // request name -> its reference answer's next token
}

func (d *driver) record(r result) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rep.Results = append(d.rep.Results, r)
	if r.Skipped != "" {
		d.rep.Skipped++
		return
	}
	d.rep.Compared++
	if r.Pass {
		d.rep.Passed++
	} else {
		d.rep.Failed++
	}
}

func (d *driver) count(n int) {
	d.mu.Lock()
	d.rep.Requests += n
	d.mu.Unlock()
}

// compareOne sends r to both servers and compares the answers.
func (d *driver) compareOne(r request) (*response, diff, error) {
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
			k := s3.PercentDecode(c.Key)
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

func dump(path string, objs []store.Object) error {
	buf := []byte("# " + memstore.Columns + "\n")
	for i := range objs {
		buf = memstore.AppendTSV(buf, &objs[i])
	}
	return os.WriteFile(path, buf, 0o644)
}

func (d *driver) write() error {
	rep := d.rep
	var text bytes.Buffer
	fmt.Fprintf(&text, "replaydiff: reference=%s fixture=%s bucket=%s keys=%d seed=%d\n", rep.Reference, rep.Fixture, rep.Bucket, rep.Keys, rep.Seed)
	kinds := map[string][3]int{}
	for _, r := range rep.Results {
		k := kinds[r.Kind]
		switch {
		case r.Skipped != "":
			k[2]++
		case r.Pass:
			k[0]++
		default:
			k[1]++
		}
		kinds[r.Kind] = k
	}
	for _, kind := range []string{"case", "walk", "random"} {
		if k, ok := kinds[kind]; ok {
			fmt.Fprintf(&text, "  %-6s %6d passed %6d failed %4d skipped\n", kind, k[0], k[1], k[2])
		}
	}
	partial := 0
	for _, r := range rep.Results {
		if r.Partial {
			partial++
		}
		if r.Skipped != "" {
			fmt.Fprintf(&text, "SKIP %s %q: %s\n", r.Kind, r.Name, r.Skipped)
			continue
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
	fmt.Fprintf(&text, "%d compared (%d HTTP requests) in %.1fs: %d passed, %d failed, %d skipped\n", rep.Compared, rep.Requests, rep.Seconds, rep.Passed, rep.Failed, rep.Skipped)
	os.Stdout.Write(text.Bytes())
	if d.o.report != "" {
		j, err := json.MarshalIndent(rep, "", " ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(d.o.report, j, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(strings.TrimSuffix(d.o.report, filepath.Ext(d.o.report))+".txt", text.Bytes(), 0o644); err != nil {
			return err
		}
	}
	if rep.Failed > 0 {
		return fmt.Errorf("%d differences", rep.Failed)
	}
	return nil
}
