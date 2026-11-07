// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"math"
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
// (GET /bkt?list-type=2&max-keys=3 on the synthetic 10k fixture).
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
	const want = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><Prefix></Prefix><NextContinuationToken>v2:AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3o</NextContinuationToken><KeyCount>3</KeyCount><MaxKeys>3</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>logs/alpha/dt=2026-01-01/part-00000.json.gz</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;cfcd208495d565ef66e7dff9f98764da&quot;</ETag><Size>1000</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>logs/alpha/dt=2026-01-01/part-00004.json.gz</Key><LastModified>2026-01-01T00:00:04.000Z</LastModified><ETag>&quot;a87ff679a2f3e71d9181a67b7542122c&quot;</ETag><Size>32676</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>logs/alpha/dt=2026-01-01/part-00008.json.gz</Key><LastModified>2026-01-01T00:00:08.000Z</LastModified><ETag>&quot;c9f0f895fb98ab9159f51fd0297e236d&quot;</ETag><Size>64352</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>`
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
		"<EncodingType>url</EncodingType>", "<KeyCount>2</KeyCount>", "<IsTruncated>true</IsTruncated>", "<Key>a/has%20space.txt</Key>",
		"<LastModified>2026-01-01T00:00:00.123Z</LastModified>", "<ETag>&quot;abc&amp;def&quot;</ETag>",
		"<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>", "<ChecksumType>FULL_OBJECT</ChecksumType>",
		"<Owner><ID>owner-1</ID><DisplayName>Alice &amp; Bob</DisplayName></Owner>", "<Prefix>a/dir/</Prefix>", "<NextContinuationToken>v2:"} {
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
	for _, s := range []string{"<Prefix>/%20%FF%26%3C%3E</Prefix>", "<StartAfter>/%20%FF%26%3C%3E</StartAfter>",
		"<Delimiter>/</Delimiter>", "<CommonPrefixes><Prefix>/%20%FF%26%3C%3E</Prefix></CommonPrefixes>"} {
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
	for _, blank := range []string{"", "   "} {
		req := &Request{Bucket: "bucket", StartAfter: b("a/2"), Token: blank, TokenSet: true, MaxKeys: 1000}
		xml := string(AppendList(nil, &Result{Request: req}))
		if !strings.Contains(xml, "<StartAfter>a/2</StartAfter>") || strings.Contains(xml, "<ContinuationToken>") {
			t.Errorf("blank %q: %s", blank, xml)
		}
	}
}

func TestRenderError(t *testing.T) {
	got := string(AppendError(nil, errNoSuchBucket, "/b%6Bt"))
	const want = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><Resource>/b%6Bt</Resource><RequestId>S3LISTINGREPLAY</RequestId></Error>`
	if got != want {
		t.Errorf("got %s", got)
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
