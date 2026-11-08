// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// The tests in this file pin answers live S3 gave on 2026-11-07 to
// anonymous ListObjectsV2 requests on a public us-east-1 bucket, where the
// replay wire contract answered differently. Each request is transcribed
// onto synthetic keys of the same shape; the want is S3's answer under the
// provider conformance normalizations: a continuation token's value is
// masked, an error is its status, code and message, and its whole
// document in TestLiveErrorDocuments with RequestId and HostId, which vary
// on S3, the replay's own.

// liveKeys is a small tree of the shape the probes listed: six keys in
// one directory, then a directory of three subdirectories.
var liveKeys = []string{
	"index.html",
	"r/k01", "r/k02", "r/k03", "r/k04", "r/k05", "r/k06",
	"t/a/k1", "t/a/k2", "t/a/k3", "t/b/k1", "t/c/k1",
}

var tokenValueRe = regexp.MustCompile(`<(Next)?ContinuationToken>[^<]*<`)

// livePage answers query over liveKeys and splits the document into its
// head (everything before the first entry, token values masked) and the
// listed entries ("O:key" or "P:prefix").
func livePage(t *testing.T, query string) (head string, entries string) {
	t.Helper()
	req, err := ParseRequest("bkt", query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	res, err := (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	doc := string(AppendList(nil, res))
	doc = tokenValueRe.ReplaceAllString(doc, "<${1}ContinuationToken>T<")
	if i := strings.Index(doc, "<Contents>"); i >= 0 {
		head = doc[:i]
	} else if i := strings.Index(doc, "<CommonPrefixes>"); i >= 0 {
		head = doc[:i]
	} else {
		head = strings.TrimSuffix(doc, "</ListBucketResult>")
	}
	return strings.TrimPrefix(head, xmlDecl+listOpen), render(res)
}

// liveError answers query and returns its S3 error.
func liveError(t *testing.T, query string) *Error {
	t.Helper()
	req, err := ParseRequest("bkt", query)
	if err == nil {
		_, err = (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("%s: %v, want an S3 error", query, err)
	}
	return e
}

func wantLiveError(t *testing.T, query string, status int, code, message string) {
	t.Helper()
	if e := liveError(t, query); e.Status != status || e.Code != code || e.Message != message {
		t.Errorf("%s: %d %s %q, want %d %s %q", query, e.Status, e.Code, e.Message, status, code, message)
	}
}

func wantLivePage(t *testing.T, query, head, entries string) {
	t.Helper()
	gotHead, gotEntries := livePage(t, query)
	if gotHead != head || gotEntries != entries {
		t.Errorf("%s:\n got %s | %s\nwant %s | %s", query, gotHead, gotEntries, head, entries)
	}
}

// An empty start-after bounds nothing but is echoed: S3 answered
// prefix=P&start-after=&max-keys=2 with <StartAfter></StartAfter> after
// the Prefix and the first two keys under P.
func TestLiveEmptyStartAfterIsEchoed(t *testing.T) {
	wantLivePage(t, "list-type=2&prefix=r/&start-after=&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><StartAfter></StartAfter><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
	// Absent, it is not echoed.
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}

// A malformed escape ('%' not followed by two hex digits) drops the whole
// parameter: S3 answered prefix=P/a%ZZ, prefix=P/pct% and prefix=P/a%4
// with an empty Prefix and the bucket's first keys; a malformed
// start-after, delimiter, max-keys, encoding-type or continuation-token
// was likewise absent, and a valid duplicate after a malformed one was
// used.
func TestLiveMalformedEscapeDropsTheParameter(t *testing.T) {
	root := "<Name>bkt</Name><Prefix></Prefix><NextContinuationToken>T</NextContinuationToken>" +
		"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>"
	for _, p := range []string{"r/a%ZZ", "r/pct%", "r/a%4", "r/%g1"} {
		wantLivePage(t, "list-type=2&prefix="+p+"&max-keys=2", root, "O:index.html O:r/k01")
	}
	wantLivePage(t, "list-type=2&pre%ZZfix=r/&max-keys=2", root, "O:index.html O:r/k01")
	inR := "<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>" +
		"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>"
	for _, q := range []string{"start-after=r/k%ZZ", "delimiter=%ZZ", "encoding-type=url%ZZ", "continuation-token=%ZZ", "fetch-owner=true%ZZ"} {
		wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&"+q, inR, "O:r/k01 O:r/k02")
	}
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2%ZZ",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/a%ZZ&prefix=r/&max-keys=2", inR, "O:r/k01 O:r/k02")
}

// The first of duplicated parameters wins: S3 answered
// prefix=P&prefix=P/key with P's listing, max-keys=1&max-keys=2 with one
// key and MaxKeys 1, an empty max-keys before a 2 with the default, and
// an empty start-after before a key with the empty echo and P's first
// keys.
func TestLiveFirstDuplicateWins(t *testing.T) {
	wantLivePage(t, "list-type=2&prefix=r/&prefix=r/k01",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=1&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01")
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/&start-after=&start-after=r/k01&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><StartAfter></StartAfter><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}

// '+' in a query value is a space: S3 answered max-keys=+2 with 400
// InvalidArgument (its ArgumentValue was " 2"), max-keys=%2B2 with two
// keys, and prefix=a+b with the echo "a b".
func TestLivePlusIsASpace(t *testing.T) {
	if e := liveError(t, "list-type=2&prefix=r/&max-keys=+2"); e.Status != 400 || e.Code != "InvalidArgument" {
		t.Errorf("max-keys=+2: %v", e)
	}
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=%2B2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
	wantLivePage(t, "list-type=2&prefix=r+k&max-keys=2",
		"<Name>bkt</Name><Prefix>r k</Prefix><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>false</IsTruncated>", "")
}

// A present but empty or blank continuation-token is refused: S3 answered
// continuation-token=, =%20 and =+ with 400 InvalidArgument "The
// continuation token provided is incorrect", also beside a start-after,
// and a malformed token likewise.
func TestLiveEmptyTokenIsRefused(t *testing.T) {
	for _, tok := range []string{"", "%20", "+", "abc"} {
		wantLiveError(t, "list-type=2&prefix=r/&max-keys=2&continuation-token="+tok, 400, "InvalidArgument", "The continuation token provided is incorrect")
	}
	wantLiveError(t, "list-type=2&prefix=r/&max-keys=2&continuation-token=&start-after=r/k01", 400, "InvalidArgument", "The continuation token provided is incorrect")
}

// An altered token is refused: S3 answered its own token with the last
// character replaced and with four characters dropped by 400
// InvalidArgument "The continuation token provided is incorrect". With
// one decoded byte flipped S3 refused the second-to-last byte and served
// the others (token.go); the replay refuses every flip, pinned here with
// a byte in the middle.
func TestLiveAlteredTokenIsRefused(t *testing.T) {
	req, err := ParseRequest("bkt", "list-type=2&prefix=r/&max-keys=2")
	if err != nil {
		t.Fatal(err)
	}
	res, err := (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	if err != nil || res.NextToken == "" {
		t.Fatalf("%+v %v", res, err)
	}
	tok := res.NextToken
	last := "A"
	if strings.HasSuffix(tok, "A") {
		last = "B"
	}
	payload, _ := base64.RawURLEncoding.DecodeString(tok)
	payload[len(payload)/2] ^= 1
	for _, bad := range []string{tok[:len(tok)-1] + last, tok[:len(tok)-4], base64.RawURLEncoding.EncodeToString(payload)} {
		wantLiveError(t, "list-type=2&prefix=r/&max-keys=2&continuation-token="+url.QueryEscape(bad), 400, "InvalidArgument",
			"The continuation token provided is incorrect")
	}
	// Unaltered, it resumes.
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&continuation-token="+url.QueryEscape(tok),
		"<Name>bkt</Name><Prefix>r/</Prefix><ContinuationToken>T</ContinuationToken><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k03 O:r/k04")
}

// A flat walk's token sent with a delimiter resumes at the token's key and
// rolls up what follows, the prefix the key falls in included: S3 answered
// a token issued after the second key of D/access/ (no delimiter) by
// listing D/access/ and D/archive/ as common prefixes, truncated before
// D/doc/. A delimiter walk's own token does not repeat a prefix.
func TestLiveFlatTokenWithDelimiterRelistsItsPrefix(t *testing.T) {
	for _, rollup := range []bool{false, true} {
		p := &Pager{Store: memstore.Keys(memstore.Options{Rollup: rollup}, liveKeys...)}
		page := func(query string) *Result {
			req, err := ParseRequest("bkt", query)
			if err != nil {
				t.Fatal(err)
			}
			res, err := p.List(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			return res
		}
		flat := page("list-type=2&prefix=t/&max-keys=2")
		if render(flat) != "O:t/a/k1 O:t/a/k2" {
			t.Fatalf("flat page %s", render(flat))
		}
		next := page("list-type=2&prefix=t/&delimiter=/&max-keys=2&continuation-token=" + url.QueryEscape(flat.NextToken))
		if render(next) != "P:t/a/ P:t/b/" || !next.Truncated {
			t.Errorf("rollup=%v: %s truncated=%v, want P:t/a/ P:t/b/ truncated", rollup, render(next), next.Truncated)
		}
		delim := page("list-type=2&prefix=t/&delimiter=/&max-keys=1")
		next = page("list-type=2&prefix=t/&delimiter=/&max-keys=2&continuation-token=" + url.QueryEscape(delim.NextToken))
		if render(delim) != "P:t/a/" || render(next) != "P:t/b/ P:t/c/" || next.Truncated {
			t.Errorf("rollup=%v: delimiter walk %s then %s", rollup, render(delim), render(next))
		}
		// A start-after inside a prefix still passes it.
		if sa := page("list-type=2&prefix=t/&delimiter=/&start-after=t/a/k2"); render(sa) != "P:t/b/ P:t/c/" {
			t.Errorf("rollup=%v: start-after inside t/a/: %s", rollup, render(sa))
		}
	}
}

// encoding-type takes "url" in any case and echoes it as sent; anything
// else is refused: S3 answered encoding-type=bogus and encoding-type= with
// 400 InvalidArgument "Invalid Encoding Method specified in Request", and
// encoding-type=URL with <EncodingType>URL</EncodingType>. The refusal
// comes after a bad max-keys and before a bad continuation-token.
func TestLiveEncodingTypeIsChecked(t *testing.T) {
	const msg = "Invalid Encoding Method specified in Request"
	for _, v := range []string{"bogus", "", "urlx", "ur"} {
		wantLiveError(t, "list-type=2&prefix=r/&max-keys=2&encoding-type="+v, 400, "InvalidArgument", msg)
	}
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&encoding-type=URL",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><EncodingType>URL</EncodingType><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
	wantLiveError(t, "list-type=2&max-keys=abc&encoding-type=bogus", 400, "InvalidArgument", "Provided max-keys not an integer or within integer range")
	wantLiveError(t, "list-type=2&continuation-token=&encoding-type=bogus", 400, "InvalidArgument", msg)
}

// marker belongs to ListObjects V1: S3 answered list-type=2 with
// marker=KEY and with marker= by 400 InvalidArgument "Marker unsupported
// with REST.GET.BUCKET in list-type=2", after a bad max-keys and before
// a bad encoding-type or continuation-token.
func TestLiveMarkerIsRefused(t *testing.T) {
	const msg = "Marker unsupported with REST.GET.BUCKET in list-type=2"
	for _, q := range []string{"marker=r/k02", "marker=", "marker=x&encoding-type=bogus", "marker=x&continuation-token="} {
		wantLiveError(t, "list-type=2&prefix=r/&max-keys=2&"+q, 400, "InvalidArgument", msg)
	}
	wantLiveError(t, "list-type=2&marker=x&max-keys=abc", 400, "InvalidArgument", "Provided max-keys not an integer or within integer range")
	// A malformed escape drops it, as any parameter.
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&marker=%ZZ",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}

// TestLiveURLEncodingTable pins encoding-type=url's table: S3 echoed a
// start-after holding each byte 0x01..0x7F with letters, digits, '-',
// '_', '.', '/' and '*' as they are, a space as '+' and every other byte
// as %XX in upper-case hex, and listed public keys holding ' ', '+', '=',
// '@', '[', ']', '~', ',' and ':' the same way (a space as '+', the rest
// escaped). Keys are rendered through the row fast path and through the
// unit path.
func TestLiveURLEncodingTable(t *testing.T) {
	var all []byte
	var want strings.Builder
	for c := 1; c < 0x80; c++ {
		all = append(all, byte(c))
		switch {
		case c == ' ':
			want.WriteByte('+')
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', strings.IndexByte("-_./*", byte(c)) >= 0:
			want.WriteByte(byte(c))
		default:
			want.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{byte(c)})))
		}
	}
	all = append(all, 0xC3, 0xA9)
	want.WriteString("%C3%A9")
	key := "a" + string(all) + "b"
	encoded := "a" + want.String() + "b"
	for _, fast := range []bool{true, false} {
		rowFastPath = fast
		req := &Request{Bucket: "bkt", StartAfter: []byte(key), MaxKeys: 1, EncodingURL: true}
		doc := string(AppendList(nil, &Result{Request: req, Entries: []Entry{{Object: &store.Object{Key: []byte(key)}}}}))
		rowFastPath = true
		if !strings.Contains(doc, "<StartAfter>"+encoded+"</StartAfter>") || !strings.Contains(doc, "<Key>"+encoded+"</Key>") {
			t.Errorf("fast=%v: %s\nwant %s", fast, doc, encoded)
		}
	}
	for raw, enc := range map[string]string{
		"abyss-1.9+sealer/contigs.fa":    "abyss-1.9%2Bsealer/contigs.fa",
		"Karyotype Report - X+T p36.pdf": "Karyotype+Report+-+X%2BT+p36.pdf",
		"Y2Fw==":                         "Y2Fw%3D%3D", "CHM13@all/x": "CHM13%40all/x", "MAF[47].png": "MAF%5B47%5D.png", "trackDb.txt~": "trackDb.txt%7E",
		"disorder,_and": "disorder%2C_and", "20:0-110221.vcf.gz": "20%3A0-110221.vcf.gz",
	} {
		if got := string(output.AppendPercentEncoded(nil, []byte(raw))); got != enc {
			t.Errorf("%q: %q, want %q", raw, got, enc)
		}
	}
}

// A NUL is refused in the prefix and in a start-after: S3 answered
// prefix=P%00, start-after=P%00 and start-after=a%00b with 400
// InvalidArgument "Value must be a sequence of Unicode characters and
// cannot include Null." (ArgumentName prefix or key-marker), after a bad
// max-keys, encoding-type or continuation-token, the prefix's before the
// start-after's. With a valid token the start-after is not read and its
// NUL is not refused; a NUL delimiter is served and echoed as the
// character reference &#x0;.
func TestLiveNULIsRefused(t *testing.T) {
	const msg = "Value must be a sequence of Unicode characters and cannot include Null."
	for _, q := range []string{"prefix=r/%00", "prefix=r/&start-after=r/%00", "prefix=r/&start-after=a%00b", "prefix=a%00&start-after=b%00"} {
		wantLiveError(t, "list-type=2&max-keys=2&"+q, 400, "InvalidArgument", msg)
	}
	wantLiveError(t, "list-type=2&start-after=a%00&max-keys=abc", 400, "InvalidArgument", "Provided max-keys not an integer or within integer range")
	wantLiveError(t, "list-type=2&start-after=a%00&encoding-type=bogus", 400, "InvalidArgument", "Invalid Encoding Method specified in Request")
	wantLiveError(t, "list-type=2&prefix=a%00&continuation-token=", 400, "InvalidArgument", "The continuation token provided is incorrect")
	tok := EncodeToken([]byte("r/k02"), false)
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&start-after=a%00&continuation-token="+url.QueryEscape(tok),
		"<Name>bkt</Name><Prefix>r/</Prefix><ContinuationToken>T</ContinuationToken><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k03 O:r/k04")
	wantLivePage(t, "list-type=2&prefix=r/&delimiter=%00&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><Delimiter>&#x0;</Delimiter><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}

// Key parameters are read as UTF-8 with replacement: S3 answered
// start-after=P%FF with <StartAfter>P�</StartAfter> (encoded
// %EF%BF%BD) and nothing after it under P, %FF%FF with two replacements,
// %C3 and %F0%9F%98 (truncated) and %ED%A0%80 (an encoded surrogate)
// with one, %E2%82A with one and the A, and prefix=P%FF and delimiter=%FF
// with the replacement echoed.
func TestLiveKeyParametersAreUTF8(t *testing.T) {
	const r = "%EF%BF%BD"
	wantLivePage(t, "list-type=2&prefix=r/&start-after=r/%FF&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><StartAfter>r/�</StartAfter><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>false</IsTruncated>", "")
	for in, out := range map[string]string{"%FF%FF": r + r, "%C3": r, "%F0%9F%98": r, "%ED%A0%80": r, "%E2%82A": r + "A"} {
		wantLivePage(t, "list-type=2&start-after=u"+in+"&max-keys=2&encoding-type=url",
			"<Name>bkt</Name><Prefix></Prefix><StartAfter>u"+out+"</StartAfter><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys>"+
				"<EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>", "")
	}
	wantLivePage(t, "list-type=2&prefix=r/%FF&max-keys=2&encoding-type=url",
		"<Name>bkt</Name><Prefix>r/"+r+"</Prefix><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>", "")
	wantLivePage(t, "list-type=2&prefix=r/&delimiter=%FF&max-keys=2&encoding-type=url",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys>"+
			"<Delimiter>"+r+"</Delimiter><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>", "O:r/k01 O:r/k02")
}

// TestLiveControlCharactersAreReferences pins how listing text carries a
// C0 control character: S3 echoed start-after=a%XXb for every byte
// 0x01..0x1F with the byte as a lower-case hexadecimal character
// reference without padding (&#x1;, &#xb;, &#x1f;) except TAB, LF and CR,
// which it wrote as they are, as it wrote DEL, the C1 controls U+0080,
// U+0085 and U+009F, U+2028, U+FFFE and U+FFFF; a prefix and a delimiter
// the same way, a NUL delimiter as &#x0;; and with encoding-type=url
// every control character as %XX. The bucket's keys hold no control
// character, so Key and CommonPrefixes/Prefix, written by the same
// listing text rule, are pinned on the rule; both go through the row fast
// path and the unit path.
func TestLiveControlCharactersAreReferences(t *testing.T) {
	ref := func(c byte) string {
		if c == '\t' || c == '\n' || c == '\r' || c == 0x7f {
			return string([]byte{c})
		}
		return "&#x" + strconv.FormatInt(int64(c), 16) + ";"
	}
	if ref(1) != "&#x1;" || ref(0x0b) != "&#xb;" || ref(0x10) != "&#x10;" || ref(0x1f) != "&#x1f;" {
		t.Fatal("ref")
	}
	for c := byte(1); c < 0x20; c++ {
		wantLivePage(t, "list-type=2&max-keys=0&start-after=a%"+strings.ToUpper(hex.EncodeToString([]byte{c}))+"b",
			"<Name>bkt</Name><Prefix></Prefix><StartAfter>a"+ref(c)+"b</StartAfter><KeyCount>0</KeyCount><MaxKeys>0</MaxKeys><IsTruncated>false</IsTruncated>", "")
	}
	head := "<Name>bkt</Name><Prefix></Prefix><StartAfter>a\u0080b\u0085c\u009fd\u2028e\ufffef\uffffg\x7fh</StartAfter>" +
		"<KeyCount>0</KeyCount><MaxKeys>0</MaxKeys><IsTruncated>false</IsTruncated>"
	wantLivePage(t, "list-type=2&max-keys=0&start-after=a%C2%80b%C2%85c%C2%9Fd%E2%80%A8e%EF%BF%BEf%EF%BF%BFg%7Fh", head, "")
	wantLivePage(t, "list-type=2&max-keys=0&prefix=a%01b%09c%0Ad%0De%7Ff",
		"<Name>bkt</Name><Prefix>a&#x1;b\tc\nd\re\x7ff</Prefix><KeyCount>0</KeyCount><MaxKeys>0</MaxKeys><IsTruncated>false</IsTruncated>", "")
	wantLivePage(t, "list-type=2&max-keys=0&prefix=a&delimiter=%01%09%0A%0D%1F%7F",
		"<Name>bkt</Name><Prefix>a</Prefix><KeyCount>0</KeyCount><MaxKeys>0</MaxKeys><Delimiter>&#x1;\t\n\r&#x1f;\x7f</Delimiter><IsTruncated>false</IsTruncated>", "")
	wantLivePage(t, "list-type=2&max-keys=0&prefix=a&delimiter=%00",
		"<Name>bkt</Name><Prefix>a</Prefix><KeyCount>0</KeyCount><MaxKeys>0</MaxKeys><Delimiter>&#x0;</Delimiter><IsTruncated>false</IsTruncated>", "")
	wantLivePage(t, "list-type=2&max-keys=0&encoding-type=url&prefix=a%01b%09&start-after=c%0Ad%1F&delimiter=%01%0D",
		"<Name>bkt</Name><Prefix>a%01b%09</Prefix><StartAfter>c%0Ad%1F</StartAfter><KeyCount>0</KeyCount><MaxKeys>0</MaxKeys>"+
			"<Delimiter>%01%0D</Delimiter><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>", "")
	var all, refs []byte
	for c := 0; c < 0x20; c++ {
		all = append(all, byte(c))
		refs = append(refs, ref(byte(c))...)
	}
	all, refs = append(all, 0x7f), append(refs, 0x7f)
	key := "k" + string(all)
	want := "k" + string(refs)
	for _, fast := range []bool{true, false} {
		rowFastPath = fast
		req := &Request{Bucket: "bkt", MaxKeys: 2}
		doc := string(AppendList(nil, &Result{Request: req, Entries: []Entry{
			{Object: &store.Object{Key: []byte(key), ETag: sp(string(all))}}, {CommonPrefix: []byte(key + "/")}}}))
		rowFastPath = true
		if !strings.Contains(doc, "<Key>"+want+"</Key>") || !strings.Contains(doc, "<ETag>&quot;"+string(refs)+"&quot;</ETag>") ||
			!strings.Contains(doc, "<CommonPrefixes><Prefix>"+want+"/</Prefix></CommonPrefixes>") {
			t.Errorf("fast=%v: %q", fast, doc)
		}
	}
}

// TestLiveErrorDocuments pins S3's error document for each refusal the
// route shares with S3: Code, Message, then ArgumentName and
// ArgumentValue (InvalidArgument), BucketName (NoSuchBucket) or Method and
// ResourceType (MethodNotAllowed), then RequestId and HostId, each as S3
// answered on 2026-11-07. S3 names a negative max-keys maxKeys and a
// start-after key-marker, echoes no value for a token or a marker, and
// writes error text with '&', '<' and '>' as entities, quotes as they are,
// and a C0 control character other than TAB, LF and CR as a decimal
// reference (&#1;). RequestId and HostId are per request on S3 and the
// x-amz-request-id and x-amz-id-2 values here.
func TestLiveErrorDocuments(t *testing.T) {
	const tail = "<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>"
	invalid := func(msg, name string, value ...string) string {
		d := "<Error><Code>InvalidArgument</Code><Message>" + msg + "</Message><ArgumentName>" + name + "</ArgumentName>"
		for _, v := range value {
			d += "<ArgumentValue>" + v + "</ArgumentValue>"
		}
		return d + tail
	}
	const (
		notInt = "Provided max-keys not an integer or within integer range"
		neg    = "Argument maxKeys must be an integer between 0 and 2147483647"
		tok    = "The continuation token provided is incorrect"
		enc    = "Invalid Encoding Method specified in Request"
		marker = "Marker unsupported with REST.GET.BUCKET in list-type=2"
		null   = "Value must be a sequence of Unicode characters and cannot include Null."
	)
	for _, c := range []struct{ method, path, query, want string }{
		{"GET", "/bkt", "list-type=2&max-keys=abc", invalid(notInt, "max-keys", "abc")},
		{"GET", "/bkt", "list-type=2&max-keys=+2", invalid(notInt, "max-keys", " 2")},
		{"GET", "/bkt", "list-type=2&max-keys=%207", invalid(notInt, "max-keys", " 7")},
		{"GET", "/bkt", "list-type=2&max-keys=2147483648", invalid(notInt, "max-keys", "2147483648")},
		{"GET", "/bkt", "list-type=2&max-keys=-2147483649", invalid(notInt, "max-keys", "-2147483649")},
		{"GET", "/bkt", "list-type=2&max-keys=a%01b%FF", invalid(notInt, "max-keys", "a&#1;b\ufffd")},
		{"GET", "/bkt", "list-type=2&max-keys=x%FF%FFy%E2%82Az%ED%A0%80", invalid(notInt, "max-keys", "x\ufffd\ufffdy\ufffdAz\ufffd")},
		{"GET", "/bkt", "list-type=2&max-keys=-1", invalid(neg, "maxKeys", "-1")},
		{"GET", "/bkt", "list-type=2&max-keys=-1&encoding-type=url", invalid(neg, "maxKeys", "-1")},
		{"GET", "/bkt", "list-type=2&continuation-token=", invalid(tok, "continuation-token")},
		{"GET", "/bkt", "list-type=2&continuation-token=abc", invalid(tok, "continuation-token")},
		{"GET", "/bkt", "list-type=2&encoding-type=bogus", invalid(enc, "encoding-type", "bogus")},
		{"GET", "/bkt", "list-type=2&encoding-type=", invalid(enc, "encoding-type", "")},
		{"GET", "/bkt", "list-type=2&encoding-type=a%22b%27c%3Ed%3Ce%26f", invalid(enc, "encoding-type", "a\"b'c&gt;d&lt;e&amp;f")},
		{"GET", "/bkt", "list-type=2&encoding-type=%00%08%0B%0C%0E%10%7F", invalid(enc, "encoding-type", "&#0;&#8;&#11;&#12;&#14;&#16;\x7f")},
		{"GET", "/bkt", "list-type=2&encoding-type=x%FF%FFy%E2%82Az%C3", invalid(enc, "encoding-type", "x\ufffd\ufffdy\ufffdAz\ufffd")},
		{"GET", "/bkt", "list-type=2&marker=x", invalid(marker, "marker")},
		{"GET", "/bkt", "list-type=2&marker=", invalid(marker, "marker")},
		{"GET", "/bkt", "list-type=2&prefix=a%00b", invalid(null, "prefix", "a&#0;b")},
		{"GET", "/bkt", "list-type=2&prefix=a%00%01%09%0A%0D%1F%7Fb%FF%3C%26", invalid(null, "prefix", "a&#0;&#1;\t\n\r&#31;\x7fb\ufffd&lt;&amp;")},
		{"GET", "/bkt", "list-type=2&start-after=a%00b", invalid(null, "key-marker", "a&#0;b")},
		{"GET", "/bkt", "list-type=2&start-after=a%00%01%3C", invalid(null, "key-marker", "a&#0;&#1;&lt;")},
		{"GET", "/bkt", "list-type=2&start-after=a%00&encoding-type=url", invalid(null, "key-marker", "a&#0;")},
		{"GET", "/no-such-bucket-zq9x7", "list-type=2", "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>" +
			"<BucketName>no-such-bucket-zq9x7</BucketName>" + tail},
		{"GET", "/no%2Dsuch-bucket", "list-type=2", "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>" +
			"<BucketName>no-such-bucket</BucketName>" + tail},
		{"PATCH", "/bkt", "list-type=2", "<Error><Code>MethodNotAllowed</Code><Message>The specified method is not allowed against this resource.</Message>" +
			"<Method>PATCH</Method><ResourceType>BUCKET</ResourceType>" + tail},
	} {
		_, err := ParseHTTP(c.method, c.path, c.query, "bkt")
		var e *Error
		if !errors.As(err, &e) {
			t.Errorf("%s %s?%s: %v, want an S3 error", c.method, c.path, c.query, err)
			continue
		}
		if got := string(AppendError(nil, e)); got != xmlDecl+c.want {
			t.Errorf("%s %s?%s:\n got %q\nwant %q", c.method, c.path, c.query, got, xmlDecl+c.want)
		}
	}
}

// TestLiveXMLDeclarationNewline pins the bytes before the root element.
// Live S3 answered list-type=2&max-keys=1 with
//
//	<?xml version="1.0" encoding="UTF-8"?>\n<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>
//
// and list-type=2&max-keys=-1 with 400 and
//
//	<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>InvalidArgument</Code>...
//
// on 2026-11-07: the declaration with version before encoding, both
// double-quoted, upper-case UTF-8, then one LF, in a listing and in an
// error document alike.
func TestLiveXMLDeclarationNewline(t *testing.T) {
	const decl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	req, err := ParseRequest("bkt", "list-type=2&max-keys=1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(AppendList(nil, res)), decl+`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name>`; !strings.HasPrefix(got, want) {
		t.Errorf("listing:\n got %.120q\nwant %q...", got, want)
	}
	_, err = ParseHTTP("GET", "/bkt", "list-type=2&max-keys=-1", "bkt")
	var e *Error
	if !errors.As(err, &e) || e.Status != 400 {
		t.Fatalf("max-keys=-1: %v", err)
	}
	want := decl + "<Error><Code>InvalidArgument</Code><Message>Argument maxKeys must be an integer between 0 and 2147483647</Message>" +
		"<ArgumentName>maxKeys</ArgumentName><ArgumentValue>-1</ArgumentValue>" +
		"<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>"
	if got := string(AppendError(nil, e)); got != want {
		t.Errorf("error document:\n got %q\nwant %q", got, want)
	}
}

// TestLiveOwnerBeforeStorageClass pins where Owner sits in a Contents
// element: S3 answered list-type=2&max-keys=2&fetch-owner=true, with and
// without encoding-type=url and with delimiter=/, on two public us-east-1
// buckets (one whose objects carry checksums, one whose objects carry
// none) with
//
//	<Contents><Key>K</Key><LastModified>T</LastModified><ETag>&quot;E&quot;</ETag>
//	<ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>
//	<Size>N</Size><Owner><ID>(64 hex digits)</ID></Owner><StorageClass>STANDARD</StorageClass></Contents>
//
// (the checksum elements absent on the second bucket): Owner after Size
// and before StorageClass, holding ID alone, and with fetch-owner=false no
// Owner. A recording without owner columns answers fetch-owner=true
// without Owner. Both the row fast path and the unit path render it.
func TestLiveOwnerBeforeStorageClass(t *testing.T) {
	id := strings.Repeat("0123456789abcdef", 4)
	size1, size2 := int64(36822), int64(3358)
	mod1, mod2 := time.UnixMicro(1_764_016_424_000_000), time.UnixMicro(1_662_746_724_000_000)
	objs := []store.Object{
		{Key: []byte("index.html"), Size: &size1, LastModified: &mod1, ETag: sp("0123456789abcdef0123456789abcdef"),
			ChecksumAlgorithm: sp("CRC32"), ChecksumType: sp("FULL_OBJECT"), StorageClass: sp("STANDARD"), OwnerID: &id},
		{Key: []byte("r/k 01.csv.gz"), Size: &size2, LastModified: &mod2, ETag: sp("fedcba9876543210fedcba9876543210"),
			StorageClass: sp("STANDARD"), OwnerID: &id},
		{Key: []byte("t/a/k1"), Size: &size2, LastModified: &mod2, ETag: sp("fedcba9876543210fedcba9876543210"),
			StorageClass: sp("STANDARD"), OwnerID: &id},
	}
	owned, err := memstore.New(objs, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range objs {
		objs[i].OwnerID = nil
	}
	unowned, err := memstore.New(objs, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	owner := "<Owner><ID>" + id + "</ID></Owner>"
	first := func(owner string) string {
		return "<Contents><Key>index.html</Key><LastModified>2025-11-24T20:33:44.000Z</LastModified>" +
			"<ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag>" +
			"<ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>" +
			"<Size>36822</Size>" + owner + "<StorageClass>STANDARD</StorageClass></Contents>"
	}
	second := func(key, owner string) string {
		return "<Contents><Key>" + key + "</Key><LastModified>2022-09-09T18:05:24.000Z</LastModified>" +
			"<ETag>&quot;fedcba9876543210fedcba9876543210&quot;</ETag>" +
			"<Size>3358</Size>" + owner + "<StorageClass>STANDARD</StorageClass></Contents>"
	}
	for _, c := range []struct {
		st    store.Store
		query string
		want  string
	}{
		{owned, "list-type=2&max-keys=2&fetch-owner=true", first(owner) + second("r/k 01.csv.gz", owner)},
		{owned, "list-type=2&max-keys=2&fetch-owner=true&encoding-type=url", first(owner) + second("r/k+01.csv.gz", owner)},
		{owned, "list-type=2&max-keys=2&fetch-owner=true&delimiter=/", first(owner) + "<CommonPrefixes><Prefix>r/</Prefix></CommonPrefixes>"},
		{owned, "list-type=2&max-keys=2&fetch-owner=false", first("") + second("r/k 01.csv.gz", "")},
		{owned, "list-type=2&max-keys=2", first("") + second("r/k 01.csv.gz", "")},
		{unowned, "list-type=2&max-keys=2&fetch-owner=true", first("") + second("r/k 01.csv.gz", "")},
	} {
		req, err := ParseRequest("bkt", c.query)
		if err != nil {
			t.Fatal(err)
		}
		res, err := (&Pager{Store: c.st}).List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		for _, fast := range []bool{true, false} {
			rowFastPath = fast
			doc := string(AppendList(nil, res))
			rowFastPath = true
			i, j := strings.Index(doc, "<Contents>"), strings.LastIndex(doc, "</ListBucketResult>")
			if i < 0 || j < i || doc[i:j] != c.want {
				t.Errorf("%s fast=%v:\n got %s\nwant %s", c.query, fast, doc, c.want)
			}
		}
	}
}

// TestLiveFetchOwnerIsExactlyTrue pins which fetch-owner values ask for
// owners: S3 answered fetch-owner=true, =%74rue, a true before a TRUE or
// a false, and a true after a malformed fetch-owner with Owner; and
// fetch-owner=TRUE, =True, =yes, =1, =, a bare fetch-owner, =true%20,
// =+true, =true%00, a true after a TRUE, a false or an empty value, and
// Fetch-Owner=true without it. The value is read as every parameter is
// (the first one counts, a malformed one is dropped) and must then be
// "true" exactly.
func TestLiveFetchOwnerIsExactlyTrue(t *testing.T) {
	id := strings.Repeat("0123456789abcdef", 4)
	size := int64(36822)
	st, err := memstore.New([]store.Object{{Key: []byte("index.html"), Size: &size, StorageClass: sp("STANDARD"), OwnerID: &id}}, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]bool{
		"fetch-owner=true": true, "fetch-owner=%74rue": true, "fetch-owner=true&fetch-owner=TRUE": true,
		"fetch-owner=true&fetch-owner=false": true, "fetch-owner=tru%ZZ&fetch-owner=true": true,
		"fetch-owner=TRUE": false, "fetch-owner=True": false, "fetch-owner=yes": false, "fetch-owner=1": false,
		"fetch-owner=": false, "fetch-owner": false, "fetch-owner=true%20": false, "fetch-owner=+true": false,
		"fetch-owner=true%00": false, "fetch-owner=TRUE&fetch-owner=true": false, "fetch-owner=false&fetch-owner=true": false,
		"fetch-owner=&fetch-owner=true": false, "Fetch-Owner=true": false,
	} {
		req, err := ParseRequest("bkt", "list-type=2&max-keys=1&"+q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		res, err := (&Pager{Store: st}).List(context.Background(), req)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		doc := string(AppendList(nil, res))
		owner := "<Size>36822</Size><Owner><ID>" + id + "</ID></Owner><StorageClass>STANDARD</StorageClass></Contents>"
		if !want {
			owner = "<Size>36822</Size><StorageClass>STANDARD</StorageClass></Contents>"
		}
		if !strings.Contains(doc, owner) {
			t.Errorf("%s: %s\nwant %s", q, doc, owner)
		}
	}
}
