// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/s3"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
)

// TestRemoteS3UploadIDSecrecy: a part's upload fails in transport, and the
// transport error quotes the request URL with the multipart upload's id
// (here with characters a URL escapes); neither the run's error, its
// warnings, its report nor the work directory's error file shows the id in
// any form. The failed part's upload is aborted.
func TestRemoteS3UploadIDSecrecy(t *testing.T) {
	url0 := bigBucket(t)
	id := func(n int64) string { return fmt.Sprintf("secret+up/%d=x", n) }
	f := s3fake.NewWithIDs(dstBucket, id)
	f.MinPartSize = s3Part
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	tr := roundTripper(func(q *http.Request) (*http.Response, error) {
		if q.URL.Query().Get("partNumber") == "2" {
			return nil, errors.New("connection reset")
		}
		return http.DefaultTransport.RoundTrip(q)
	})
	out := &RemoteOutput{Location: "s3://" + dstBucket + "/s", ChunkSize: s3Part, Open: func(ctx context.Context, st *dataset.UploadStats) (*dataset.Remote, func() error, error) {
		c := awss3.New(awss3.Options{BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
			HTTPClient: &http.Client{Transport: tr}, RetryMaxAttempts: 1})
		l, _ := objfile.ParseLocation("s3://" + dstBucket + "/s")
		w, err := s3.NewWriter(c, dstBucket, s3.WriteOptions{PartSize: s3Part, MinPartSize: s3Part, MaxAttempts: 1, Retried: st.Retried})
		if err != nil {
			return nil, nil, err
		}
		return &dataset.Remote{Location: l, Region: "us-east-1", Opener: objfile.Root(s3.New(c, dstBucket), "s"), Writer: objfile.RootWriter(w, "s")}, func() error { return nil }, nil
	}}
	ws := filepath.Join(t.TempDir(), "work")
	cfg := remoteConfig(url0, ws, false, out)
	var said []string
	var mu sync.Mutex
	cfg.Warn = func(m string) { mu.Lock(); said = append(said, m); mu.Unlock() }
	cfg.Settle = func(r Report, err error) {
		mu.Lock()
		said = append(said, r.Outcome.Error, fmt.Sprint(err))
		mu.Unlock()
	}
	rep, err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("a run whose parts fail succeeded")
	}
	b, _ := json.Marshal(rep)
	errFile, _ := os.ReadFile(filepath.Join(ws, dataset.ErrorName))
	texts := append(said, err.Error(), string(b), string(errFile))
	for _, text := range texts {
		for n := int64(1); n < 200; n++ {
			for _, form := range []string{id(n), url.QueryEscape(id(n)), url.PathEscape(id(n))} {
				if strings.Contains(text, form) {
					t.Fatalf("upload id %q escaped: %s", form, text)
				}
			}
		}
		if strings.Contains(text, "uploadId") {
			t.Fatalf("an upload URL's query escaped: %s", text)
		}
	}
	if len(f.Uploads()) != 0 {
		t.Fatalf("uploads left open: %v", f.Uploads())
	}
}
