// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package vercel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/tree"
)

const (
	testRoot  = "vercel://store_Test123"
	testToken = "vercel_blob_rw_Test123_fixture-secret"
)

func TestOpenStoreBinding(t *testing.T) {
	for _, tc := range []struct{ root, token string }{
		{testRoot, ""},
		{testRoot, "Bearer secret"},
		{testRoot, "vercel_blob_rw_Test123"},
		{testRoot, "vercel_blob_rw_Test123_"},
		{testRoot, "vercel_blob_rw_Test123_bad\ntoken"},
		{testRoot, "vercel_blob_rw_Test123_bad\x00token"},
		{testRoot, "vercel_blob_rw_Test123_é"},
		{testRoot, "vercel_blob_rw_Other123_fixture-secret"},
		{"vercel://store_test123", testToken},
	} {
		t.Run(tc.root+"/"+strconv.Itoa(len(tc.token)), func(t *testing.T) {
			requests := 0
			hc := &http.Client{Transport: listTransport(func(*http.Request) (*http.Response, error) {
				requests++
				return nil, errors.New("unexpected network call")
			})}
			_, err := Open(t.Context(), tc.root, Options{Token: tc.token, HTTPClient: hc})
			var cred *objhttp.CredentialError
			if !errors.As(err, &cred) || cred.Source != TokenEnv || objhttp.Classify(err) != objhttp.ClassAuth ||
				strings.Contains(err.Error(), "fixture-secret") || requests != 0 {
				t.Fatalf("Open = %v, requests = %d, want safe credential failure before HTTP", err, requests)
			}
		})
	}
	for _, root := range []string{"vercel://", "vercel://Test123", "vercel://store_", "vercel://store_Test@host", testRoot + "/\xff", "https://" + testRoot} {
		if _, _, err := ParseURL(root); err == nil {
			t.Errorf("accepted invalid root %q", root)
		}
	}
	const prefix = "/a//../literal%2f?x+#e\u0301"
	r := WithPrefix(testRoot+"/old", prefix)
	if s, p, err := ParseURL(r); err != nil || s != "store_Test123" || p != prefix {
		t.Fatalf("ParseURL(WithPrefix(...)) = %q, %q, %v", s, p, err)
	}
}

func TestOpenPageSize(t *testing.T) {
	hc := &http.Client{Transport: listTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid page size made a listing request")
		return nil, nil
	})}
	for _, n := range []int{-1, 1001} {
		_, err := Open(t.Context(), testRoot, Options{Token: testToken, PageSize: n, HTTPClient: hc})
		if !errors.Is(err, source.ErrUnsupported) {
			t.Errorf("Open with page size %d = %v, want unsupported", n, err)
		}
	}
}

func TestReportTLS(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		o, err := Open(t.Context(), testRoot, Options{Token: testToken, Trust: source.Trust{NoVerify: insecure}})
		if err != nil {
			t.Fatal(err)
		}
		st := o.Report().Stats.(*Stats)
		if st.NoVerifySSL != insecure || st.Credentials != "read-write-token" {
			t.Fatalf("report: no_verify_ssl = %v, credentials = %q", st.NoVerifySSL, st.Credentials)
		}
	}
}

// The fixture returns reverse-ordered pages of direct blobs and virtual
// folders. Its upload corpus, independent of the folded response, defines
// the exact expected rows and metadata for every requested scope.
func TestFoldedWalk(t *testing.T) {
	const stamp = "2026-01-02T03:04:05.123456789+02:30"
	const uploadedAt = 1767314045123456
	keys := []string{"/leading", "a", "a-foo", "a.foo", "a/", "a//", "a//x", "a/marker/", "a/nested/x", "a0", "ab/x", "b/",
		"e\u0301/x", "k?%+/x", "é/x", "\ue000", "\U00010000"}
	want := make([]schema.Row[Object], len(keys))
	blobs := make(map[string]wireBlob, len(keys))
	for i, key := range keys {
		etag := fmt.Sprintf(`"opaque-%d"`, i)
		want[i] = schema.Row[Object]{Key: key, Data: Object{Size: int64(i), UploadedAt: uploadedAt, ETag: etag}}
		blobs[key] = wireBlob{key, int64(i), stamp, etag}
	}
	for _, n := range []int{1, 4} {
		for _, limit := range []int{1, 2, 1000} {
			for _, scope := range []string{"", "a", "a/", "a/ne", "k?%+", "/", "e", "missing/"} {
				t.Run(fmt.Sprintf("workers=%d/limit=%d/scope=%q", n, limit, scope), func(t *testing.T) {
					hc := &http.Client{Transport: listTransport(func(r *http.Request) (*http.Response, error) {
						q := r.URL.Query()
						if q.Get("mode") != "folded" || q.Get("limit") != strconv.Itoa(limit) || r.Header.Get("Authorization") != "Bearer "+testToken {
							return nil, errors.New("incorrect folded request")
						}
						prefix := q.Get("prefix")
						if prefix != "" && !strings.HasSuffix(prefix, "/") {
							return nil, errors.New("partial prefix sent as folder")
						}
						page, err := foldedPage(blobs, prefix, q.Get("cursor"), limit)
						if err != nil {
							return nil, err
						}
						return response(page)
					})}
					o, got, err := run(t, scope, Options{Workers: n, PageSize: limit, HTTPClient: hc})
					if err != nil {
						t.Fatal(err)
					}
					expected := slices.DeleteFunc(slices.Clone(want), func(r schema.Row[Object]) bool { return !strings.HasPrefix(r.Key, scope) })
					if !slices.Equal(got, expected) {
						t.Fatalf("inventory = %+v, want %+v", got, expected)
					}
					report := o.Report()
					counts := report.Stats.(*Stats).Counters
					var exactScopes int64
					if scope != "" && !strings.HasSuffix(scope, "/") {
						exactScopes = 1
					}
					if got := counts.Value("tree.scope_exact_prefixes"); got != exactScopes {
						t.Fatalf("exact-prefix scopes = %d, want %d", got, exactScopes)
					}
					if o.Listed() != int64(len(expected)) || !report.Coverage.Complete || report.Coverage.Prefix != scope ||
						counts.Value("tree.pages") < 1 || counts.Value("attempts") != o.Requests().Attempts || o.folders.Staged() != 0 {
						t.Fatalf("listed %d, coverage %+v, counts %+v", o.Listed(), report.Coverage, counts)
					}
				})
			}
		}
	}
}

func TestFoldedWalkRetriesDefaultHTTPPolicy(t *testing.T) {
	var queries []string
	hc := &http.Client{Transport: listTransport(func(r *http.Request) (*http.Response, error) {
		queries = append(queries, r.URL.RawQuery)
		switch len(queries) {
		case 1:
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody}, nil
		case 2:
			return response(wirePage{
				Blobs:   []wireBlob{{"scope/a", 3, "2026-01-01T00:00:00Z", "e1"}},
				Folders: []string{},
			})
		default:
			return nil, errors.New("unexpected list attempt")
		}
	})}
	o, rows, err := run(t, "scope/", Options{HTTPClient: hc})
	want := rowSink{{Key: "scope/a", Data: Object{Size: 3, UploadedAt: 1767225600000000, ETag: "e1"}}}
	if err != nil || !slices.Equal(rows, want) {
		t.Fatalf("walk = %v, %v, want %v", rows, err, want)
	}
	report := o.Report()
	v := report.Stats.(*Stats).Counters.Value
	if len(queries) != 2 || queries[0] != "limit=1000&mode=folded&prefix=scope%2F" || queries[0] != queries[1] || v("attempts") != 2 ||
		v("retries") != 1 || v("tree.requeues") != 0 || v("tree.pages") != 1 ||
		o.Answered() != 1 || !report.Coverage.Complete {
		t.Fatalf("queries %q, attempts %d, retries %d, requeues %d, pages %d, answered %d, coverage %+v",
			queries, v("attempts"), v("retries"), v("tree.requeues"), v("tree.pages"), o.Answered(), report.Coverage)
	}
}

func TestFoldedWalkStopsAfterHTTPJSONRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		retry := objhttp.Retry{}
		retry.Defaults()
		calls := 0
		hc := &http.Client{Transport: listTransport(func(*http.Request) (*http.Response, error) {
			calls++
			body := "{\"blobs\":["
			// The first request after the shared client's attempt budget
			// would succeed if the folder walker erroneously requeued.
			if calls > retry.MaxAttempts {
				body = "{\"blobs\":[],\"folders\":[],\"hasMore\":false}"
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		})}
		o, rows, err := run(t, "", Options{HTTPClient: hc})
		r := o.Report()
		v := r.Stats.(*Stats).Counters.Value
		if !errors.Is(err, objhttp.ErrSyntax) || len(rows) != 0 || r.Coverage.Complete ||
			calls != retry.MaxAttempts || v("parse_failures") != int64(retry.MaxAttempts) || v("retries") != int64(retry.MaxAttempts-1) || v("tree.requeues") != 0 {
			t.Fatalf("rows %d, err %v, attempts %d, parse_failures %d, retries %d, requeues %d, coverage %+v",
				len(rows), err, calls, v("parse_failures"), v("retries"), v("tree.requeues"), r.Coverage)
		}
	})
}

func TestFoldedWalkRejectsCursorCycle(t *testing.T) {
	for _, tc := range []struct {
		name string
		next map[string]string
	}{
		{"self", map[string]string{"": "first", "first": "first"}},
		{"previous", map[string]string{"": "first", "first": "second", "second": "first"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			hc := &http.Client{Transport: listTransport(func(r *http.Request) (*http.Response, error) {
				attempts++
				cursor, ok := tc.next[r.URL.Query().Get("cursor")]
				if !ok || attempts > len(tc.next) {
					return nil, errors.New("unexpected cursor request")
				}
				return response(wirePage{Blobs: []wireBlob{}, Folders: []string{}, HasMore: true, Cursor: cursor})
			})}
			o, rows, err := run(t, "", Options{Workers: 2, HTTPClient: hc})
			report := o.Report()
			violations := report.Stats.(*Stats).Counters.Value("tree.violations.token")
			if !errors.Is(err, source.ErrProtocol) || len(rows) != 0 || report.Coverage.Complete ||
				o.folders.Staged() != 0 || attempts != len(tc.next) || violations != 1 {
				t.Fatalf("walk = %v, %v, coverage %+v, staged %d, attempts %d, violations %d",
					rows, err, report.Coverage, o.folders.Staged(), attempts, violations)
			}
		})
	}
}

func TestFoldedWalkFolderLimit(t *testing.T) {
	hc := &http.Client{Transport: listTransport(func(*http.Request) (*http.Response, error) {
		return response(wirePage{Blobs: []wireBlob{{"x", 1, "2026-01-01T00:00:00Z", strings.Repeat("X", 2<<10)}}, Folders: []string{}})
	})}
	o, rows, err := run(t, "", Options{FolderBytes: 1 << 10, HTTPClient: hc})
	var limit *tree.FolderLimitError
	if !errors.As(err, &limit) || len(rows) != 0 || o.Report().Coverage.Complete || o.folders.Staged() != 0 {
		t.Fatalf("walk = %v, %v, coverage %+v, staged %d", rows, err, o.Report().Coverage, o.folders.Staged())
	}
}

func TestFoldedWalkSharedStagingLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	firstStaged := make(chan struct{})
	blob := func(key string) wireBlob {
		return wireBlob{key, 1, "2026-01-01T00:00:00Z", strings.Repeat("X", 600)}
	}
	hc := &http.Client{Transport: listTransport(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		switch q.Get("prefix") {
		case "a/":
			if q.Get("cursor") == "" {
				return response(wirePage{Blobs: []wireBlob{blob("a/x")}, Folders: []string{}, HasMore: true, Cursor: "next"})
			}
			close(firstStaged)
			<-r.Context().Done()
			return nil, r.Context().Err()
		case "b/":
			select {
			case <-firstStaged:
				return response(wirePage{Blobs: []wireBlob{blob("b/x")}, Folders: []string{}})
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		default:
			return nil, errors.New("unexpected folder")
		}
	})}
	o, err := Open(ctx, testRoot, Options{Token: testToken, Workers: 2, FolderBytes: 2 << 10, StagingBytes: 1 << 10, HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	firstErr := make(chan error, 1)
	go func() { firstErr <- o.walk.Expand(ctx, tree.FolderRange{Prefix: "a/"}, rejectRows{}) }()
	err = o.walk.Expand(ctx, tree.FolderRange{Prefix: "b/"}, rejectRows{})
	cancel()
	if first := <-firstErr; !errors.Is(first, context.Canceled) {
		t.Errorf("held folder = %v, want canceled", first)
	}
	var limit *tree.FolderLimitError
	if !errors.As(err, &limit) || o.folders.Staged() != 0 {
		t.Fatalf("second folder = %v, staged %d", err, o.folders.Staged())
	}
}

func TestObjectCodec(t *testing.T) {
	want := Object{Size: 1 << 42, UploadedAt: -1234567, ETag: `"opaque-é"`}
	d := schema.NewDecoder(Schema.Codec.Append(nil, &want))
	var got Object
	Schema.Codec.Decode(d, &got)
	if d.Err() != nil || d.Len() != 0 || got != want {
		t.Fatalf("decoded %+v (%d bytes left, %v), want %+v", got, d.Len(), d.Err(), want)
	}
}

type rejectRows struct{}

func (rejectRows) Rows([]schema.Row[Object]) error { return errors.New("unexpected rows") }
func (rejectRows) Child(tree.FolderRange) error    { return errors.New("unexpected child folder") }

type rowSink []schema.Row[Object]

func (s *rowSink) Write(_ context.Context, rows [][]schema.Row[Object]) error {
	for _, seg := range rows {
		*s = append(*s, seg...)
	}
	return nil
}

func run(t *testing.T, scope string, opt Options) (*Opened, rowSink, error) {
	t.Helper()
	ctx := t.Context()
	opt.Token = testToken
	o, err := Open(ctx, testRoot+"/"+scope, opt)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := o.Bound().Prepare(bind.Env{Order: order.Options{Workers: max(opt.Workers, 1), Budget: 80}})
	if err != nil {
		t.Fatal(err)
	}
	var got rowSink
	_, err = prepared.Run(ctx, &got)
	return o, got, err
}

type listTransport func(*http.Request) (*http.Response, error)

func (r listTransport) RoundTrip(req *http.Request) (*http.Response, error) { return r(req) }

type wireBlob struct {
	Pathname   string `json:"pathname"`
	Size       int64  `json:"size"`
	UploadedAt string `json:"uploadedAt"`
	ETag       string `json:"etag"`
}

type wirePage struct {
	Blobs   []wireBlob `json:"blobs"`
	Folders []string   `json:"folders"`
	HasMore bool       `json:"hasMore"`
	Cursor  string     `json:"cursor,omitempty"`
}

func response(page wirePage) (*http.Response, error) {
	b, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b)), ContentLength: int64(len(b))}, nil
}

func foldedPage(blobs map[string]wireBlob, prefix, cursor string, limit int) (wirePage, error) {
	entries := map[string]bool{} // true for a virtual folder
	for key := range blobs {
		rel, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		if slash := strings.IndexByte(rel, '/'); slash >= 0 {
			entries[prefix+rel[:slash+1]] = true
		} else {
			entries[key] = false
		}
	}
	names := make([]string, 0, len(entries))
	for key := range entries {
		names = append(names, key)
	}
	slices.Sort(names)
	slices.Reverse(names)
	offset := 0
	if cursor != "" {
		var err error
		offset, err = strconv.Atoi(cursor)
		if err != nil || offset <= 0 || offset >= len(names) {
			return wirePage{}, errors.New("fixture received a cursor outside its folder")
		}
	}
	end := min(offset+limit, len(names))
	page := wirePage{Blobs: []wireBlob{}, Folders: []string{}, HasMore: end < len(names)}
	if page.HasMore {
		page.Cursor = strconv.Itoa(end)
	}
	for _, key := range names[offset:end] {
		if entries[key] {
			page.Folders = append(page.Folders, key)
		} else {
			page.Blobs = append(page.Blobs, blobs[key])
		}
	}
	return page, nil
}
