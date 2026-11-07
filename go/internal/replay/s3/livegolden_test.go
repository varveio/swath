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
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// The tests in this file pin answers live S3 gave on 2026-11-07 to
// anonymous ListObjectsV2 requests on a public us-east-1 bucket, where the
// replay wire contract answered differently. Each request is transcribed
// onto synthetic keys of the same shape; the want is S3's answer under the
// provider conformance normalizations: a continuation token's value is
// masked, an error is its status, code and message (S3's own document also
// carries ArgumentName, ArgumentValue and HostId, which the replay's does
// not).

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
// character replaced, with four characters dropped, and with one decoded
// byte flipped, by 400 InvalidArgument "The continuation token provided
// is incorrect".
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
	wantLiveError(t, "list-type=2&max-keys=abc&encoding-type=bogus", 400, "InvalidArgument", "max-keys must be an integer")
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
	wantLiveError(t, "list-type=2&marker=x&max-keys=abc", 400, "InvalidArgument", "max-keys must be an integer")
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
// NUL is not refused; a NUL delimiter is served (S3 writes it as the
// character reference &#x0;, the replay as the raw byte: control
// characters in text are a documented difference).
func TestLiveNULIsRefused(t *testing.T) {
	const msg = "Value must be a sequence of Unicode characters and cannot include Null."
	for _, q := range []string{"prefix=r/%00", "prefix=r/&start-after=r/%00", "prefix=r/&start-after=a%00b", "prefix=a%00&start-after=b%00"} {
		wantLiveError(t, "list-type=2&max-keys=2&"+q, 400, "InvalidArgument", msg)
	}
	wantLiveError(t, "list-type=2&start-after=a%00&max-keys=abc", 400, "InvalidArgument", "max-keys must be an integer")
	wantLiveError(t, "list-type=2&start-after=a%00&encoding-type=bogus", 400, "InvalidArgument", "Invalid Encoding Method specified in Request")
	wantLiveError(t, "list-type=2&prefix=a%00&continuation-token=", 400, "InvalidArgument", "The continuation token provided is incorrect")
	tok := EncodeToken([]byte("r/k02"), false)
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&start-after=a%00&continuation-token="+url.QueryEscape(tok),
		"<Name>bkt</Name><Prefix>r/</Prefix><ContinuationToken>T</ContinuationToken><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k03 O:r/k04")
	wantLivePage(t, "list-type=2&prefix=r/&delimiter=%00&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><Delimiter>\x00</Delimiter><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}
