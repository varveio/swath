// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package scalefake

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configure a Server.
type Options struct {
	Bucket string // the one bucket served; default "bkt"
	// Latency delays every listing answer, plus a uniform draw from
	// [0, Jitter); a request cancelled meanwhile is dropped.
	Latency, Jitter time.Duration
	// Fail503 is the fraction of listing requests answered 503 (S3
	// SlowDown, GCS backendError) after the delay, drawn from FaultSeed
	// and the request's sequence number.
	Fail503   float64
	FaultSeed uint64
	// Log, if set, receives one tab-separated line per listing request:
	// unix ms, api, status, keys, prefixes, microseconds, prefix, bound.
	Log io.Writer
}

// Server answers S3 ListObjectsV2 (path-style: GET /BUCKET?list-type=2)
// and GCS objects.list (GET /storage/v1/b/BUCKET/o) over a Keyspace, and
// its own accounting at GET /__scalefake/stats.
type Server struct {
	ks  *Keyspace
	opt Options

	seq        atomic.Uint64
	requests   atomic.Int64 // listing requests received
	pages      atomic.Int64 // listing pages answered 200
	keys       atomic.Int64
	prefixes   atomic.Int64
	throttled  atomic.Int64
	errors     atomic.Int64 // 4xx answers
	bytesOut   atomic.Int64
	inflight   atomic.Int64
	peak       atomic.Int64
	windowPeak atomic.Int64
	busyNs     atomic.Int64 // summed request durations: mean inflight = d(busy)/dt
	logMu      sync.Mutex
	started    time.Time
}

// NewServer serves ks.
func NewServer(ks *Keyspace, opt Options) *Server {
	if opt.Bucket == "" {
		opt.Bucket = "bkt"
	}
	return &Server{ks: ks, opt: opt, started: time.Now()}
}

// Stats is the server's accounting.
type Stats struct {
	AsOfUnixMs   int64  `json:"as_of_unix_ms"`
	UptimeMs     int64  `json:"uptime_ms"`
	Requests     int64  `json:"requests"`
	Pages        int64  `json:"pages"`
	Keys         int64  `json:"keys"`
	Prefixes     int64  `json:"prefixes"`
	Throttled    int64  `json:"throttled"`
	Errors       int64  `json:"errors"`
	Bytes        int64  `json:"bytes"`
	Inflight     int64  `json:"inflight"`
	PeakInflight int64  `json:"peak_inflight"`
	WindowPeak   int64  `json:"window_peak_inflight"` // since the previous Stats call that reset it
	BusyNs       int64  `json:"busy_ns"`
	KeyspaceKeys int64  `json:"keyspace_keys"`
	Shape        string `json:"shape"`
}

// Stats reads the accounting; reset starts a new window for WindowPeak.
func (s *Server) Stats(reset bool) Stats {
	now := time.Now()
	st := Stats{AsOfUnixMs: now.UnixMilli(), UptimeMs: now.Sub(s.started).Milliseconds(),
		Requests: s.requests.Load(), Pages: s.pages.Load(), Keys: s.keys.Load(), Prefixes: s.prefixes.Load(),
		Throttled: s.throttled.Load(), Errors: s.errors.Load(), Bytes: s.bytesOut.Load(),
		Inflight: s.inflight.Load(), PeakInflight: s.peak.Load(), BusyNs: s.busyNs.Load(),
		KeyspaceKeys: s.ks.Len(), Shape: s.ks.Shape().String()}
	if reset {
		st.WindowPeak = s.windowPeak.Swap(st.Inflight)
	} else {
		st.WindowPeak = s.windowPeak.Load()
	}
	return st
}

func raise(a *atomic.Int64, v int64) {
	for {
		o := a.Load()
		if v <= o || a.CompareAndSwap(o, v) {
			return
		}
	}
}

const gcsBase = "/storage/v1/b/"

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/__scalefake/stats":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Stats(r.URL.Query().Get("reset") != ""))
		return
	case r.URL.Path == "/__scalefake/count":
		fmt.Fprintln(w, s.ks.Count(r.URL.Query().Get("prefix")))
		return
	case r.Method != http.MethodGet && r.Method != http.MethodHead:
		s.errors.Add(1)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gcs := strings.HasPrefix(r.URL.Path, gcsBase)
	if !gcs && r.URL.Query().Get("list-type") != "2" {
		// HEAD bucket, GetBucketLocation and the like: say where it is.
		if r.URL.Path == "/"+s.opt.Bucket || r.URL.Path == "/"+s.opt.Bucket+"/" {
			w.Header().Set("x-amz-bucket-region", "us-east-1")
			if _, ok := r.URL.Query()["location"]; ok {
				fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
			}
			return
		}
		s.errors.Add(1)
		s3Error(w, http.StatusNotFound, "NoSuchBucket", "no such bucket")
		return
	}
	start := time.Now()
	s.requests.Add(1)
	n := s.inflight.Add(1)
	raise(&s.peak, n)
	raise(&s.windowPeak, n)
	defer func() {
		s.inflight.Add(-1)
		s.busyNs.Add(int64(time.Since(start)))
	}()
	seq := s.seq.Add(1)
	if d := s.delay(seq); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-r.Context().Done():
			t.Stop()
			return
		}
	}
	var status, keys, prefixes int
	var q Query
	if gcs {
		status, keys, prefixes, q = s.serveGCS(w, r, seq)
	} else {
		status, keys, prefixes, q = s.serveS3(w, r, seq)
	}
	if s.opt.Log != nil {
		s.logMu.Lock()
		api := "s3"
		if gcs {
			api = "gcs"
		}
		fmt.Fprintf(s.opt.Log, "%d\t%s\t%d\t%d\t%d\t%d\t%q\t%q\n", time.Now().UnixMilli(), api, status, keys, prefixes,
			time.Since(start).Microseconds(), q.Prefix, q.After)
		s.logMu.Unlock()
	}
}

func (s *Server) delay(seq uint64) time.Duration {
	d := s.opt.Latency
	if s.opt.Jitter > 0 {
		d += time.Duration(hash2(s.opt.FaultSeed^0x1a7e, seq) % uint64(s.opt.Jitter))
	}
	return d
}

// fail reports whether request seq is answered 503.
func (s *Server) fail(seq uint64) bool {
	return s.opt.Fail503 > 0 && float64(hash2(s.opt.FaultSeed, seq)>>11)/(1<<53) < s.opt.Fail503
}

func (s *Server) account(w http.ResponseWriter, body []byte, p Page) {
	s.pages.Add(1)
	s.keys.Add(int64(p.Keys))
	s.prefixes.Add(int64(len(p.Entries) - p.Keys))
	s.bytesOut.Add(int64(len(body)))
	_, _ = w.Write(body)
}

func s3Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message><RequestId>scalefake</RequestId></Error>`, code, msg)
}

var bufs = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func (s *Server) serveS3(w http.ResponseWriter, r *http.Request, seq uint64) (int, int, int, Query) {
	v := r.URL.Query()
	q := Query{Prefix: v.Get("prefix"), Delimiter: v.Get("delimiter"), After: v.Get("start-after"), Token: v.Get("continuation-token")}
	if r.URL.Path != "/"+s.opt.Bucket && r.URL.Path != "/"+s.opt.Bucket+"/" {
		s.errors.Add(1)
		s3Error(w, http.StatusNotFound, "NoSuchBucket", "no such bucket")
		return 404, 0, 0, q
	}
	if mk := v.Get("max-keys"); mk != "" {
		n, err := strconv.Atoi(mk)
		if err != nil || n < 0 {
			s.errors.Add(1)
			s3Error(w, http.StatusBadRequest, "InvalidArgument", "max-keys")
			return 400, 0, 0, q
		}
		q.Max = max(n, 1) // 0 would be an empty page; the client never sends it
	}
	if s.fail(seq) {
		s.throttled.Add(1)
		s3Error(w, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.")
		return 503, 0, 0, q
	}
	p, err := s.ks.List(q)
	if err != nil {
		s.errors.Add(1)
		s3Error(w, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect")
		return 400, 0, 0, q
	}
	urlEnc := v.Get("encoding-type") == "url"
	enc := func(b *bytes.Buffer, x string) {
		if urlEnc {
			appendURL(b, x)
		} else {
			appendXML(b, x)
		}
	}
	b := bufs.Get().(*bytes.Buffer)
	defer bufs.Put(b)
	b.Reset()
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>`)
	appendXML(b, s.opt.Bucket)
	b.WriteString("</Name><Prefix>")
	enc(b, q.Prefix)
	b.WriteString("</Prefix>")
	if q.After != "" {
		b.WriteString("<StartAfter>")
		enc(b, q.After)
		b.WriteString("</StartAfter>")
	}
	fmt.Fprintf(b, "<KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys>", len(p.Entries), pageMax(q.Max))
	if q.Delimiter != "" {
		b.WriteString("<Delimiter>")
		enc(b, q.Delimiter)
		b.WriteString("</Delimiter>")
	}
	if urlEnc {
		b.WriteString("<EncodingType>url</EncodingType>")
	}
	fmt.Fprintf(b, "<IsTruncated>%t</IsTruncated>", p.Next != "")
	if q.Token != "" {
		b.WriteString("<ContinuationToken>" + q.Token + "</ContinuationToken>")
	}
	if p.Next != "" {
		b.WriteString("<NextContinuationToken>" + p.Next + "</NextContinuationToken>")
	}
	var num []byte
	for _, e := range p.Entries {
		if e.Prefix {
			continue
		}
		m := s.ks.Meta(e.Index)
		b.WriteString("<Contents><Key>")
		enc(b, e.Name)
		b.WriteString("</Key><LastModified>")
		b.WriteString(time.Unix(m.Mtime, 0).UTC().Format("2006-01-02T15:04:05.000Z"))
		b.WriteString("</LastModified><ETag>&quot;")
		b.WriteString(m.ETag)
		b.WriteString("&quot;</ETag><Size>")
		b.Write(strconv.AppendInt(num[:0], m.Size, 10))
		b.WriteString("</Size><StorageClass>STANDARD</StorageClass></Contents>")
	}
	for _, e := range p.Entries {
		if e.Prefix {
			b.WriteString("<CommonPrefixes><Prefix>")
			enc(b, e.Name)
			b.WriteString("</Prefix></CommonPrefixes>")
		}
	}
	b.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	s.account(w, b.Bytes(), p)
	return 200, p.Keys, len(p.Entries) - p.Keys, q
}

// pageMax is the page size a request's max gives.
func pageMax(n int) int {
	if n <= 0 || n > MaxPage {
		return MaxPage
	}
	return n
}

func gcsError(w http.ResponseWriter, status int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":{"code":%d,"message":%q,"errors":[{"reason":%q,"message":%q}]}}`, status, msg, reason, msg)
}

type gcsObject struct {
	Name           string `json:"name"`
	Bucket         string `json:"bucket"`
	Size           string `json:"size"`
	Updated        string `json:"updated"`
	ETag           string `json:"etag"`
	Generation     string `json:"generation"`
	Metageneration string `json:"metageneration"`
	StorageClass   string `json:"storageClass"`
	MD5            string `json:"md5Hash"`
	CRC32C         string `json:"crc32c"`
}

type gcsPage struct {
	Kind     string      `json:"kind"`
	Items    []gcsObject `json:"items,omitempty"`
	Prefixes []string    `json:"prefixes,omitempty"`
	Next     string      `json:"nextPageToken,omitempty"`
}

func (s *Server) serveGCS(w http.ResponseWriter, r *http.Request, seq uint64) (int, int, int, Query) {
	v := r.URL.Query()
	q := Query{Prefix: v.Get("prefix"), Delimiter: v.Get("delimiter"), After: v.Get("startOffset"), Inclusive: true,
		End: v.Get("endOffset"), Token: v.Get("pageToken"), RollUpBelow: true}
	if r.URL.Path != gcsBase+s.opt.Bucket+"/o" {
		s.errors.Add(1)
		gcsError(w, http.StatusNotFound, "notFound", "The specified bucket does not exist.")
		return 404, 0, 0, q
	}
	if mr := v.Get("maxResults"); mr != "" {
		n, err := strconv.Atoi(mr)
		if err != nil || n < 0 {
			s.errors.Add(1)
			gcsError(w, http.StatusBadRequest, "invalid", "maxResults")
			return 400, 0, 0, q
		}
		q.Max = n
	}
	if s.fail(seq) {
		s.throttled.Add(1)
		gcsError(w, http.StatusServiceUnavailable, "backendError", "Backend Error")
		return 503, 0, 0, q
	}
	p, err := s.ks.List(q)
	if err != nil {
		s.errors.Add(1)
		gcsError(w, http.StatusBadRequest, "invalid", "Invalid page token")
		return 400, 0, 0, q
	}
	out := gcsPage{Kind: "storage#objects", Next: p.Next}
	for _, e := range p.Entries {
		if e.Prefix {
			out.Prefixes = append(out.Prefixes, e.Name)
			continue
		}
		m := s.ks.Meta(e.Index)
		var crc [4]byte
		binary.BigEndian.PutUint32(crc[:], uint32(hash2(s.ks.Shape().Seed, uint64(e.Index)^0xc3c)))
		md5 := []byte(m.ETag[:16])
		out.Items = append(out.Items, gcsObject{Name: e.Name, Bucket: s.opt.Bucket, Size: strconv.FormatInt(m.Size, 10),
			Updated: time.Unix(m.Mtime, 0).UTC().Format("2006-01-02T15:04:05.000Z"), ETag: "C" + m.ETag[:12] + "=",
			Generation: strconv.FormatInt(m.Mtime*1000000+e.Index%1000000, 10), Metageneration: "1", StorageClass: "STANDARD",
			MD5: base64.StdEncoding.EncodeToString(md5), CRC32C: base64.StdEncoding.EncodeToString(crc[:])})
	}
	body, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	s.account(w, body, p)
	return 200, p.Keys, len(p.Entries) - p.Keys, q
}

func appendXML(b *bytes.Buffer, s string) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteByte(c)
		}
	}
}

// appendURL percent-encodes s as S3's encoding-type=url does (everything
// but unreserved bytes and '/'), copying runs of safe bytes whole.
func appendURL(b *bytes.Buffer, s string) {
	const hex = "0123456789ABCDEF"
	for len(s) > 0 {
		i := 0
		for i < len(s) && urlSafe[s[i]] {
			i++
		}
		b.WriteString(s[:i])
		if i == len(s) {
			return
		}
		c := s[i]
		b.Write([]byte{'%', hex[c>>4], hex[c&15]})
		s = s[i+1:]
	}
}

var urlSafe = func() (t [256]bool) {
	for c := range 256 {
		t[c] = 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' || c == '/'
	}
	return t
}()
