// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/source/s3/s3test"
)

// keys fill several pages of the fake, in byte order; one is outside the
// prefix. The fake answers raw, not URL-encoded (TestWalkDecodes covers that).
var keys = []string{"p/a b", "p/a%2Fb", "p/a+b", "p/k", "p/k5", "p/k9", "p/x", "p/é", "q/out"}

func walkOf(t *testing.T, startAfter string) (string, stats) {
	t.Helper()
	s3test.NoCredentials(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	c, _, err := client(context.Background(), s3test.Bucket(t, keys), "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	st, err := walker{c: c, bucket: s3test.Name, prefix: "p/", startAfter: startAfter, maxKeys: 3}.walk(context.Background(), &out)
	if err != nil {
		t.Fatal(err)
	}
	return out.String(), st
}

// Every key under the prefix, decoded, in the store's order, across pages.
func TestWalk(t *testing.T) {
	got, st := walkOf(t, "")
	want := strings.Join(keys[:8], "\n") + "\n"
	if got != want {
		t.Fatalf("walk wrote\n%q\nwant\n%q", got, want)
	}
	if st.keys != 8 || st.pages != 3 || st.raw != 3 || len(st.sizes) != 2 {
		t.Fatalf("stats %v: want 8 keys in 3 pages, counted as not URL-encoded", st)
	}
}

func TestWalkStartAfter(t *testing.T) {
	got, _ := walkOf(t, "p/k")
	if want := "p/k5\np/k9\np/x\np/é\n"; got != want {
		t.Fatalf("walk after p/k wrote %q, want %q", got, want)
	}
}

// A URL-encoded answer is decoded with form semantics, as the AWS SDKs
// decode it: '+' is a space, %2B a '+', %01 a control character; the
// continuation token is followed.
func TestWalkDecodes(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	page := func(token string, truncated bool, keys ...string) string {
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><EncodingType>url</EncodingType><IsTruncated>%t</IsTruncated>`, truncated)
		if token != "" {
			fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", token)
		}
		for _, k := range keys {
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>0</Size></Contents>", k)
		}
		return b.String() + "</ListBucketResult>"
	}
	var asked []string
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		asked = append(asked, q.Get("encoding-type")+"|"+q.Get("continuation-token"))
		if q.Get("continuation-token") == "" {
			fmt.Fprint(w, page("t1", true, "p/%01ctl", "p/a+b", "p/a%2Bb"))
			return
		}
		fmt.Fprint(w, page("", false, "p/%C3%A9"))
	}))
	c, _, err := client(context.Background(), url, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	st, err := walker{c: c, bucket: "bkt", prefix: "p/", maxKeys: 3}.walk(context.Background(), &out)
	if err != nil {
		t.Fatal(err)
	}
	if want := "p/\x01ctl\np/a b\np/a+b\np/é\n"; out.String() != want {
		t.Fatalf("walk wrote %q, want %q", out.String(), want)
	}
	if st.raw != 0 || st.pages != 2 || strings.Join(asked, ",") != "url|,url|t1" {
		t.Fatalf("stats %v, requests %q: want 2 URL-encoded pages, the second after token t1", st, asked)
	}
}

// A truncated page whose continuation token is empty, or one already
// followed (the one the request carried, or an earlier one), ends the walk
// with an error instead of a loop or a silent stop.
func TestWalkRefusesBadTokens(t *testing.T) {
	for name, next := range map[string]string{"empty": "", "repeated": "t1", "cycle": "cycle"} {
		t.Run(name, func(t *testing.T) {
			s3test.NoCredentials(t)
			t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			calls := 0
			url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				tok := "t1"
				if r.URL.Query().Get("continuation-token") != "" {
					tok = next
				}
				if next == "cycle" { // t1, t2, t1, ...
					tok = map[string]string{"": "t1", "t1": "t2", "t2": "t1"}[r.URL.Query().Get("continuation-token")]
				}
				// The fake ends the listing on its fourth page, so a walk that
				// follows a bad token stops instead of looping, and fails below.
				fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken><Contents><Key>p/%d</Key><Size>0</Size></Contents></ListBucketResult>`, calls < 4, tok, calls)
			}))
			c, _, err := client(context.Background(), url, "us-east-1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = walker{c: c, bucket: "bkt", prefix: "p/", maxKeys: 1}.walk(context.Background(), &bytes.Buffer{})
			want := 2 // the second page returns the bad token
			if next == "cycle" {
				want = 3 // the third page returns t1 again
			}
			if err == nil || calls != want {
				t.Fatalf("err %v after %d requests: want an error on page %d", err, calls, want)
			}
		})
	}
}
