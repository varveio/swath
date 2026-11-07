// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func sp(s string) *string { return &s }

func object(key string, size int64, micros int64, etag string) store.Object {
	t := time.UnixMicro(micros).UTC()
	return store.Object{Key: []byte(key), Size: &size, LastModified: &t, ETag: sp(etag), StorageClass: sp("STANDARD")}
}

// TestRenderListGolden pins a whole page against the bytes the Java server
// answered for the same request over the same three objects
// (GET /bkt?list-type=2&max-keys=3 on the synthetic 10k fixture), apart
// from the token: the Java server's was
// v2:AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3o, which
// carries no checksum (token.go).
func TestRenderListGolden(t *testing.T) {
	s, _ := memstore.New([]store.Object{
		object("logs/alpha/dt=2026-01-01/part-00000.json.gz", 1000, 1767225600000000, "cfcd208495d565ef66e7dff9f98764da"),
		object("logs/alpha/dt=2026-01-01/part-00004.json.gz", 32676, 1767225604000000, "a87ff679a2f3e71d9181a67b7542122c"),
		object("logs/alpha/dt=2026-01-01/part-00008.json.gz", 64352, 1767225608000000, "c9f0f895fb98ab9159f51fd0297e236d"),
		object("logs/alpha/dt=2026-01-01/part-00012.json.gz", 96028, 1767225612000000, "c20ad4d76fe97759aa27a0c99bff6710"),
	}, memstore.Options{})
	req, err := ParseRequest("bkt", "list-type=2&max-keys=3")
	if err != nil {
		t.Fatal(err)
	}
	res, err := (&Pager{Store: s}).List(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	const want = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><Prefix></Prefix><NextContinuationToken>AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3qV49Nz</NextContinuationToken><KeyCount>3</KeyCount><MaxKeys>3</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>logs/alpha/dt=2026-01-01/part-00000.json.gz</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;cfcd208495d565ef66e7dff9f98764da&quot;</ETag><Size>1000</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>logs/alpha/dt=2026-01-01/part-00004.json.gz</Key><LastModified>2026-01-01T00:00:04.000Z</LastModified><ETag>&quot;a87ff679a2f3e71d9181a67b7542122c&quot;</ETag><Size>32676</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>logs/alpha/dt=2026-01-01/part-00008.json.gz</Key><LastModified>2026-01-01T00:00:08.000Z</LastModified><ETag>&quot;c9f0f895fb98ab9159f51fd0297e236d&quot;</ETag><Size>64352</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>`
	if got := string(AppendList(nil, res)); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

func TestRenderListElementsAndOrder(t *testing.T) {
	size := int64(10)
	mod := time.UnixMicro(1_767_225_600_123_456)
	req := &Request{Bucket: "bucket", Prefix: b("a/"), Delimiter: b("/"), StartAfter: b("a/0"), MaxKeys: 1000, EncodingURL: true, FetchOwner: true}
	res := &Result{Request: req, Truncated: true, NextToken: EncodeToken([]byte("a/dir/"), false), Entries: []Entry{
		{CommonPrefix: []byte("a/dir/")},
		{Object: &store.Object{Key: []byte("a/has space.txt"), Size: &size, LastModified: &mod, ETag: sp("abc&def"),
			StorageClass: sp("STANDARD"), OwnerID: sp("owner-1"), OwnerDisplayName: sp("Alice & Bob"),
			ChecksumAlgorithm: sp("CRC32"), ChecksumType: sp("FULL_OBJECT")}},
	}}
	xml := string(AppendList(nil, res))
	for _, s := range []string{"<Name>bucket</Name>", "<Prefix>a/</Prefix>", "<Delimiter>/</Delimiter>", "<StartAfter>a/0</StartAfter>",
		"<EncodingType>url</EncodingType>", "<KeyCount>2</KeyCount>", "<IsTruncated>true</IsTruncated>", "<Key>a/has+space.txt</Key>",
		"<LastModified>2026-01-01T00:00:00.123Z</LastModified>", "<ETag>&quot;abc&amp;def&quot;</ETag>",
		"<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>", "<ChecksumType>FULL_OBJECT</ChecksumType>",
		"<Owner><ID>owner-1</ID><DisplayName>Alice &amp; Bob</DisplayName></Owner>", "<Prefix>a/dir/</Prefix>", "<NextContinuationToken>AGEvZGlyL1jPDYI</NextContinuationToken>"} {
		if !strings.Contains(xml, s) {
			t.Errorf("missing %s in %s", s, xml)
		}
	}
	order := []string{"<StartAfter>", "<KeyCount>", "<MaxKeys>", "<Delimiter>", "<EncodingType>", "<Contents>", "<CommonPrefixes>"}
	for i := 1; i < len(order); i++ {
		if strings.Index(xml, order[i-1]) > strings.Index(xml, order[i]) {
			t.Errorf("%s after %s", order[i-1], order[i])
		}
	}
	// Without fetch-owner the owner is not rendered.
	req.FetchOwner = false
	if strings.Contains(string(AppendList(nil, res)), "<Owner>") {
		t.Error("owner rendered without fetch-owner")
	}
}

// Unencoded values are text: XML metacharacters escaped (including '\”
// as &apos;), malformed UTF-8 replaced, a null time the epoch's
// neighbourhood rounded toward the earlier millisecond, extreme numbers
// as they are.
func TestRenderAdversarialUnencodedValues(t *testing.T) {
	size := int64(math.MinInt64)
	mod := time.UnixMicro(-1)
	req := &Request{Bucket: "b&<>\"'", Prefix: []byte{'a', '&', '<', '>', '"', '\'', 0xff}, MaxKeys: math.MinInt32, FetchOwner: true}
	res := &Result{Request: req, Entries: []Entry{{Object: &store.Object{Key: []byte("k/😀&<>'\""), Size: &size, LastModified: &mod,
		ETag: sp("e&<>\"'😀"), StorageClass: sp("ST😀&"), OwnerID: sp("id<"), OwnerDisplayName: sp("display'"),
		ChecksumAlgorithm: sp("CRC&"), ChecksumType: sp("TYPE>")}}}}
	want := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Name>b&amp;&lt;&gt;&quot;&apos;</Name>` +
		"<Prefix>a&amp;&lt;&gt;&quot;&apos;�</Prefix>" +
		`<KeyCount>1</KeyCount><MaxKeys>-2147483648</MaxKeys>` +
		`<IsTruncated>false</IsTruncated><Contents>` +
		`<Key>k/😀&amp;&lt;&gt;&apos;&quot;</Key>` +
		`<LastModified>1969-12-31T23:59:59.999Z</LastModified>` +
		`<ETag>&quot;e&amp;&lt;&gt;&quot;&apos;😀&quot;</ETag>` +
		`<ChecksumAlgorithm>CRC&amp;</ChecksumAlgorithm><ChecksumType>TYPE&gt;</ChecksumType>` +
		`<Size>-9223372036854775808</Size><StorageClass>ST😀&amp;</StorageClass>` +
		`<Owner><ID>id&lt;</ID><DisplayName>display&apos;</DisplayName></Owner>` +
		`</Contents></ListBucketResult>`
	if got := string(AppendList(nil, res)); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

func TestRenderNullSizeAndTimeAsZeroAndEpoch(t *testing.T) {
	res := &Result{Request: &Request{Bucket: "b", MaxKeys: 1}, Entries: []Entry{{Object: &store.Object{Key: []byte("k")}}}}
	xml := string(AppendList(nil, res))
	if !strings.Contains(xml, "<Contents><Key>k</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><Size>0</Size></Contents>") {
		t.Errorf("%s", xml)
	}
}

func TestPercentEncodesEveryUnsafeByte(t *testing.T) {
	raw := []byte{'/', ' ', 0xff, '&', '<', '>'}
	req := &Request{Bucket: "bucket", Prefix: raw, Delimiter: b("/"), StartAfter: raw, MaxKeys: 1, EncodingURL: true}
	xml := string(AppendList(nil, &Result{Request: req, Entries: []Entry{{CommonPrefix: raw}}}))
	for _, s := range []string{"<Prefix>/+%FF%26%3C%3E</Prefix>", "<StartAfter>/+%FF%26%3C%3E</StartAfter>",
		"<Delimiter>/</Delimiter>", "<CommonPrefixes><Prefix>/+%FF%26%3C%3E</Prefix></CommonPrefixes>"} {
		if !strings.Contains(xml, s) {
			t.Errorf("missing %s in %s", s, xml)
		}
	}
}

func TestTokenSuppressesTheIgnoredStartAfter(t *testing.T) {
	req := &Request{Bucket: "bucket", StartAfter: b("conflicting-boundary"), Token: "opaque-token", TokenSet: true, MaxKeys: 1000}
	xml := string(AppendList(nil, &Result{Request: req}))
	if !strings.Contains(xml, "<ContinuationToken>opaque-token</ContinuationToken>") || strings.Contains(xml, "<StartAfter>") {
		t.Errorf("%s", xml)
	}
}

// TestRenderError pins the document for the replay's own refusals: no
// details between Message and RequestId, and error text escaped as S3
// escapes it (TestLiveErrorDocuments), a list-type refusal naming the
// parameter and its value.
func TestRenderError(t *testing.T) {
	got := string(AppendError(nil, &Error{Status: 500, Code: "InternalError", Message: "a&<>\"'\x01\t\xff"}))
	const want = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>a&amp;&lt;&gt;"'&#1;` + "\t\ufffd" +
		`</Message><RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>`
	if got != want {
		t.Errorf("got %q", got)
	}
	for q, details := range map[string]string{
		"max-keys=1":              "<ArgumentName>list-type</ArgumentName>",
		"list-type=1":             "<ArgumentName>list-type</ArgumentName><ArgumentValue>1</ArgumentValue>",
		"list-type=&max-keys=abc": "<ArgumentName>list-type</ArgumentName><ArgumentValue></ArgumentValue>",
	} {
		_, err := ParseRequest("bkt", q)
		var e *Error
		if !errors.As(err, &e) {
			t.Fatalf("%s: %v", q, err)
		}
		want := `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidArgument</Code><Message>only ListObjectsV2 is supported</Message>` +
			details + `<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>`
		if got := string(AppendError(nil, e)); got != want {
			t.Errorf("%s: got %s", q, got)
		}
	}
}

// TestLastModifiedYears pins LastModified outside four-digit common-era
// years: each want is the Java server's own rendering of the same second.
func TestLastModifiedYears(t *testing.T) {
	for sec, want := range map[int64]string{
		253402300800:   "+10000-01-01T00:00:00.000Z",
		253402300799:   "9999-12-31T23:59:59.000Z",
		-62135596800:   "0001-01-01T00:00:00.000Z",   // 1 CE
		-62135596801:   "0001-12-31T23:59:59.000Z",   // the last second of year 0 (1 BCE)
		-62167219200:   "0001-01-01T00:00:00.000Z",   // the first second of year 0
		-62167219201:   "0002-12-31T23:59:59.000Z",   // year -1 (2 BCE)
		-62198755200:   "0002-01-01T00:00:00.000Z",   // year -1
		-377705116800:  "+10000-01-01T00:00:00.000Z", // year -9999 (10000 BCE)
		-30610224000:   "1000-01-01T00:00:00.000Z",
		-9223372036854: "+290309-12-21T19:59:06.000Z", // the earliest whole second in signed microseconds
		9223372036854:  "+294247-01-10T04:00:54.000Z", // the latest
		0:              "1970-01-01T00:00:00.000Z",
	} {
		var ts timestamps
		if got := string(ts.appendMillis(nil, sec*1_000_000)); got != want {
			t.Errorf("second %d: %s, want %s", sec, got, want)
		}
	}
	var ts timestamps
	if got := string(ts.appendMillis(nil, 253402300800_123_999)); got != "+10000-01-01T00:00:00.123Z" {
		t.Errorf("milliseconds: %s", got)
	}
}

// TestOneKeyAndEmptyPagesUseSmallFirstChunks is the contract's
// actualS3OneKeyAndEmptyPagesUseSmallPostPageFirstChunks: a max-keys=1000
// request reserves for a full page before paging, but a page of one key or
// none renders into a single 4 KiB chunk and gives the rest back.
func TestOneKeyAndEmptyPagesUseSmallFirstChunks(t *testing.T) {
	for _, keys := range [][]string{{"a"}, nil} {
		p := pager(keys...)
		req, err := ParseRequest("bucket", "list-type=2&max-keys=1000")
		if err != nil {
			t.Fatal(err)
		}
		budget := output.NewBudget(1<<20, nil)
		o, err := output.New(budget, EstimateSize(req.PageSize()), 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		if budget.Charged() != 320_512 {
			t.Errorf("reserved %d", budget.Charged())
		}
		res := list(t, p, req)
		o.SetFirstChunkHint(EstimateSize(len(res.Entries)))
		RenderList(o, res)
		n := 0
		for _, v := range o.Body() {
			n += len(v)
		}
		if o.Err() != nil || n >= 4096 || o.Capacity() != 4096 || budget.Charged() != 4096 {
			t.Errorf("%d keys: err %v body %d capacity %d charged %d", len(keys), o.Err(), n, o.Capacity(), budget.Charged())
		}
		o.Close()
		if budget.Charged() != 0 {
			t.Errorf("charged %d after close", budget.Charged())
		}
	}
}

// TestRenderStopsAtTheCap renders a thousand ~1 KiB keys against an 8 KiB
// cap: rendering stops at the cap with response_too_large, and nothing past
// the cap was ever charged (so nothing past it was allocated).
func TestRenderStopsAtTheCap(t *testing.T) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("%04d/%s", i, strings.Repeat("k ", 500))
	}
	req, _ := ParseRequest("bucket", "list-type=2&max-keys=1000&encoding-type=url")
	res := list(t, pager(keys...), req)
	budget := output.NewBudget(1<<30, nil)
	o, err := output.New(budget, min(8192, EstimateSize(req.PageSize())), 8192)
	if err != nil {
		t.Fatal(err)
	}
	o.SetFirstChunkHint(EstimateSize(len(res.Entries)))
	RenderList(o, res)
	if o.Err() != output.ErrTooLarge || budget.Peak() != 8192 || o.Capacity() != 8192 || o.Len() > 8192 {
		t.Errorf("err %v peak %d capacity %d len %d", o.Err(), budget.Peak(), o.Capacity(), o.Len())
	}
	o.Close()
}

// TestRenderSurrogateBytesUnencoded is the lone-UTF-16-surrogate case of
// the contract's adversarial rendering test in the form a byte key can
// take: the bytes ED A0 80, a surrogate encoded as UTF-8 (which UTF-8
// forbids), render as the single U+FFFD the contract's decoder makes of
// them, and as %ED%A0%80 with encoding-type=url.
func TestRenderSurrogateBytesUnencoded(t *testing.T) {
	key := []byte{'k', 0xED, 0xA0, 0x80, 'z'}
	for _, enc := range []bool{false, true} {
		req := &Request{Bucket: "b", MaxKeys: 1, EncodingURL: enc}
		got := string(AppendList(nil, &Result{Request: req, Entries: []Entry{{Object: &store.Object{Key: key}}}}))
		want := "<Key>k\uFFFDz</Key>"
		if enc {
			want = "<Key>k%ED%A0%80z</Key>"
		}
		if !strings.Contains(got, want) {
			t.Errorf("url=%v: %s", enc, got)
		}
	}
}

// TestRowFastPathMatchesUnits renders pages of adversarial objects (XML
// metacharacters, malformed UTF-8, control characters) through
// outputs of many chunk sizes and caps, once with whole Contents elements
// appended where they fit and once unit by unit, and requires the same
// bytes, refusal, chunk allocations and percent-encoding counts.
func TestRowFastPathMatchesUnits(t *testing.T) {
	size, neg := int64(123456789), int64(math.MinInt64)
	mod, early := time.UnixMicro(1_767_225_600_123_456), time.UnixMicro(-1)
	objects := []store.Object{
		object("logs/alpha/dt=2026-01-01/part-00000.json.gz", 1000, 1767225600000000, "cfcd208495d565ef66e7dff9f98764da"),
		{Key: []byte("k/😀&<>'\" space"), Size: &neg, LastModified: &early, ETag: sp("e&<>\"'😀"), StorageClass: sp("ST😀&"),
			OwnerID: sp("id<"), OwnerDisplayName: sp("display'"), ChecksumAlgorithm: sp("CRC&"), ChecksumType: sp("TYPE>")},
		{Key: []byte{'k', 0xED, 0xA0, 0x80, 'z', 0xff}, ETag: sp(string([]byte{0xff, 'x'})), OwnerID: sp("o")},
		{Key: []byte("k\x00\x01\t\n\r\x1f\x7f/"), ETag: sp("\x0be"), StorageClass: sp("S\x1f"), OwnerID: sp("\x01")},
		{Key: []byte(strings.Repeat("long key/", 40)), Size: &size, LastModified: &mod},
		{Key: []byte("k")},
	}
	var entries []Entry
	for i := 0; i < 3*len(objects); i++ {
		o := objects[i%len(objects)]
		entries = append(entries, Entry{Object: &o})
	}
	type outcome struct {
		body                      string
		err                       error
		length, capacity, charged int
		onePass, fallback, chunks int
	}
	render := func(req *Request, chunk, limit int, fast bool) outcome {
		rowFastPath = fast
		defer func() { rowFastPath = true }()
		var chunks int
		budget := output.NewBudget(1<<30, func(output.Reason, int) { chunks++ })
		o, err := output.NewChunked(budget, min(limit, 4096), limit, chunk, nil)
		if err != nil {
			t.Fatal(err)
		}
		RenderList(o, &Result{Request: req, Entries: entries})
		var body strings.Builder
		for _, v := range o.Body() {
			body.Write(v)
		}
		one, fall := o.PercentPaths()
		got := outcome{body.String(), o.Err(), o.Len(), o.Capacity(), o.Charged(), one, fall, chunks}
		o.Close()
		return got
	}
	for _, enc := range []bool{false, true} {
		for _, owner := range []bool{false, true} {
			req := &Request{Bucket: "b", MaxKeys: 1000, EncodingURL: enc, FetchOwner: owner}
			// Every chunk size up to 1,200 bytes puts each element's start
			// at many distances from a chunk edge, so the key meets the
			// percent encoder's one-third-of-the-room threshold on both sides.
			chunks := []int{4096, 1 << 20}
			for c := 1; c <= 1200; c++ {
				chunks = append(chunks, c)
			}
			for _, chunk := range chunks {
				limits := []int{999, 1 << 24}
				if chunk%97 == 0 || chunk > 1200 {
					limits = append(limits, 50, 5000)
				}
				for _, limit := range limits {
					fast, units := render(req, chunk, limit, true), render(req, chunk, limit, false)
					if fast != units {
						t.Fatalf("url=%v owner=%v chunk %d cap %d:\nfast  %+v\nunits %+v", enc, owner, chunk, limit, fast, units)
					}
				}
			}
		}
	}
}

// TestTimestampCacheMatchesFreshFormatting formats a run of times through
// one cache (same second, same day, the next and the previous day, before
// the epoch, around year 0 and past year 9999) and each through a fresh
// one: the cached time-of-day rewrite must agree with the full formatting.
func TestTimestampCacheMatchesFreshFormatting(t *testing.T) {
	seconds := []int64{0, 0, 1, 59, 3599, 86399, 86400, 86401, 43200, -1, -86400, -86401, -86399,
		253402300799, 253402300800, 253402300801, -62167219201, -62167219200, -62167219199, 1767225600, 1767225601, 1767225600}
	r := rand.New(rand.NewPCG(3, 4))
	for range 2000 {
		seconds = append(seconds, seconds[len(seconds)-1]+r.Int64N(200_000)-100_000)
	}
	var cached timestamps
	for _, sec := range seconds {
		micros := sec*1_000_000 + r.Int64N(1_000_000)
		var fresh timestamps
		if got, want := string(cached.appendMillis(nil, micros)), string(fresh.appendMillis(nil, micros)); got != want {
			t.Fatalf("%d: cached %s, fresh %s", micros, got, want)
		}
	}
}
