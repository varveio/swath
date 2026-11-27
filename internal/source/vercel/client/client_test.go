// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/objhttp/objhttptest"
)

const (
	fixtureToken = "vercel_blob_rw_Store1_fixtureSecret"
	emptyPage    = `{"blobs":[],"folders":[],"hasMore":false}`
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func newClient(t *testing.T, rt transportFunc, attempts int) *Client {
	t.Helper()
	c, err := New(Config{
		Retry:      objhttp.Retry{MaxAttempts: attempts, BaseBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond},
		Token:      fixtureToken,
		HTTPClient: &http.Client{Transport: rt},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func TestWireSnapshot(t *testing.T) {
	var got strings.Builder
	index := 0
	inputs := []ListInput{
		{Limit: MaxPageSize},
		{Prefix: "space +%&?/#/é/", Limit: 7},
		{Prefix: "space +%&?/#/é/", Limit: 7, Cursor: "1+//a==="},
	}
	c := newClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer "+fixtureToken || r.Header.Get("X-Api-Version") != "13" {
			t.Fatal("request lacked the store bearer token or pinned API version")
		}
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.String(), Endpoint+"?") || r.Body != nil {
			t.Fatal("invalid listing method, endpoint or body")
		}
		in := inputs[index]
		index++
		if q := r.URL.Query(); q.Get("prefix") != in.Prefix || q.Get("cursor") != in.Cursor || q.Get("mode") != "folded" {
			t.Fatalf("request query did not preserve parameters: %v", q)
		}
		if ua := r.Header.Get("User-Agent"); ua != buildinfo.UserAgent() {
			t.Fatalf("User-Agent = %q", ua)
		}
		got.WriteString(objhttptest.Request(r, r.URL.Scheme+"://"+r.URL.Host, 0, func(name, value string) (string, bool) {
			switch name {
			case "User-Agent":
				return "swath/<version>", true
			case "Authorization":
				return "Bearer <store-token>", true
			}
			return value, true
		}))
		return response(200, emptyPage), nil
	}, 1)
	for _, in := range inputs {
		if _, err := c.List(t.Context(), in); err != nil {
			t.Fatal(err)
		}
	}
	objhttptest.Snapshot(t, "testdata/wire.txt", got.String())
}

func TestRetrySamePage(t *testing.T) {
	var query string
	var calls int
	c := newClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			query = r.URL.RawQuery
		} else if r.URL.RawQuery != query {
			t.Fatal("retry changed the page request")
		}
		switch calls {
		case 1:
			return response(429, `{"error":{"code":"rate_limited"}}`), nil
		case 2:
			return response(503, `{"error":{"code":"service_unavailable"}}`), nil
		case 3:
			return response(200, `{"blobs":[`), nil
		default:
			return response(200, `{"blobs":[{"pathname":"a/a","size":0,"uploadedAt":"2026-11-27T12:35:00.123Z","etag":"\"b\""}],"folders":["a/a/"],"hasMore":true,"cursor":"new+cursor="}`), nil
		}
	}, 4)
	got, err := c.List(t.Context(), ListInput{Prefix: "a/", Cursor: "old/cursor=", Limit: 2})
	want := []Blob{{Pathname: "a/a", Size: 0, UploadedAt: time.Date(2026, 11, 27, 12, 35, 0, 123_000_000, time.UTC), ETag: `"b"`}}
	if err != nil || !slices.Equal(got.Blobs, want) || !slices.Equal(got.Folders, []string{"a/a/"}) || got.NextCursor != "new+cursor=" {
		t.Fatalf("List = %+v, %v", got, err)
	}
	n := c.Counters()
	if calls != 4 || n.Attempts != 4 || n.Retries != 3 || n.Throttles != 1 || n.ParseFailures != 1 || c.Answered() != 1 || c.Live().Inflight != 0 {
		t.Fatalf("calls = %d, counters = %+v, answered = %d, live = %+v", calls, n, c.Answered(), c.Live())
	}
}

func TestInvalidPageStops(t *testing.T) {
	c := newClient(t, func(*http.Request) (*http.Response, error) {
		return response(200, `{"blobs":[],"folders":[],"hasMore":true}`), nil
	}, 2)
	_, err := c.List(t.Context(), ListInput{Limit: MaxPageSize})
	if !errors.Is(err, objhttp.ErrInvalidPage) || !errors.Is(err, source.ErrProtocol) || objhttp.Requeue(err) || c.Counters().Attempts != 1 {
		t.Fatalf("missing cursor: err = %v, counters = %+v", err, c.Counters())
	}
}

func TestSameHostRedirectStops(t *testing.T) {
	calls, forwarded := 0, false
	c := newClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Path != "/api/blob" {
			forwarded = r.Header.Get("Authorization") != ""
			return response(200, emptyPage), nil
		}
		redirect := response(http.StatusFound, "")
		redirect.Header.Set("Location", Endpoint+"/other")
		return redirect, nil
	}, 2)
	_, err := c.List(t.Context(), ListInput{Limit: MaxPageSize})
	failure := objhttp.Normalize(err)
	if failure == nil || failure.Status != http.StatusFound || calls != 1 || forwarded {
		t.Fatalf("redirect: err = %v, requests = %d, forwarded = %v", err, calls, forwarded)
	}
}

type brokenBody struct{ err error }

func (b brokenBody) Read([]byte) (int, error) { return 0, b.err }
func (b brokenBody) Close() error             { return nil }

func TestSecretSafeFailures(t *testing.T) {
	const cursor = "opaque+cursor/="
	for _, test := range []struct {
		name  string
		reply transportFunc
		class objhttp.Class
	}{
		{"redirect", func(*http.Request) (*http.Response, error) {
			r := response(302, "<html>"+fixtureToken+"</html>")
			r.Header.Set("Location", "%invalid?credential="+fixtureToken)
			return r, nil
		}, objhttp.ClassOther},
		{"forbidden", func(*http.Request) (*http.Response, error) {
			body := fmt.Sprintf(`{"error":{"code":%q,"message":%q}}`, fixtureToken, cursor)
			return response(403, body), nil
		}, objhttp.ClassAccessDenied},
		{"other-store-token", func(*http.Request) (*http.Response, error) {
			return nil, errors.New("incoming error with vercel_blob_rw_Store2_secret")
		}, objhttp.ClassOther},
		{"transport", func(r *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("transport failed on %s using %s", r.URL, r.Header.Get("Authorization"))
		}, objhttp.ClassOther},
		{"body read", func(*http.Request) (*http.Response, error) {
			r := response(200, "")
			r.Body = brokenBody{fmt.Errorf("bad trailer %s: %w", fixtureToken, io.ErrUnexpectedEOF)}
			return r, nil
		}, objhttp.ClassOther},
		{"ordinary transport", func(*http.Request) (*http.Response, error) {
			return nil, io.ErrUnexpectedEOF
		}, objhttp.ClassOther},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			c := newClient(t, func(r *http.Request) (*http.Response, error) {
				calls++
				return test.reply(r)
			}, 1)
			_, err := c.List(t.Context(), ListInput{Cursor: cursor, Limit: MaxPageSize})
			if err == nil || objhttp.Classify(err) != test.class || calls != 1 {
				t.Fatalf("error = %v, class = %s, calls = %d", err, objhttp.Classify(err), calls)
			}
			for e := err; e != nil; e = errors.Unwrap(e) {
				if containsCredential(e.Error(), fixtureToken, cursor) {
					t.Fatal("credential or cursor retained in error")
				}
			}
		})
	}
}

func TestCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	rt := func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	}
	c := newClient(t, rt, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.List(ctx, ListInput{Limit: MaxPageSize})
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || c.Counters().Attempts != 1 || c.Live().Inflight != 0 {
		t.Fatalf("canceled list: err = %v, counters = %+v", err, c.Counters())
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 35, 0, 0, time.UTC)
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"", 0},
		{"-3", 0},
		{"2", 2 * time.Second},
		{"25", 5 * time.Second},
		{"999999999999999999999999999", 5 * time.Second},
		{now.Add(3 * time.Second).Format(http.TimeFormat), 3 * time.Second},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 5 * time.Second},
		{now.Add(-time.Second).Format(http.TimeFormat), 0},
	} {
		if got := retryAfter(tc.header, now, 5*time.Second); got != tc.want {
			t.Errorf("Retry-After %q: got %v, want %v", tc.header, got, tc.want)
		}
	}
}
