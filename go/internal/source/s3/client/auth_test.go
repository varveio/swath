// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// onePerRequest is a credentials provider that fails a second lookup for
// the same request: it may be retrieved once per request sent.
type onePerRequest struct {
	retrieved, sent *atomic.Int32
}

func (p onePerRequest) Retrieve(context.Context) (aws.Credentials, error) {
	if p.retrieved.Add(1) > p.sent.Load()+1 {
		return aws.Credentials{}, errors.New("a second lookup for one request")
	}
	return aws.Credentials{AccessKeyID: "AKTEST", SecretAccessKey: "s", Source: "test"}, nil
}

// TestCredentialsRetrievedOncePerAttempt: the credentials are looked up
// once per attempt, before it is counted, and the request is signed with
// them: the SDK does not look them up again inside the counted call.
func TestCredentialsRetrievedOncePerAttempt(t *testing.T) {
	s3test.NoCredentials(t)
	var retrieved, sent atomic.Int32
	var auth atomic.Value
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		auth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, okPage)
	}))
	c := newTest(t, testConfig(url))
	c.auth.credentials, c.auth.anonymous, c.auth.source = onePerRequest{&retrieved, &sent}, false, "test"
	for range 3 {
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
	}
	if retrieved.Load() != sent.Load() || c.Counters().Attempts != 3 ||
		!strings.HasPrefix(auth.Load().(string), "AWS4-HMAC-SHA256 Credential=AKTEST/") {
		t.Fatalf("%d lookups for %d requests, %+v, authorization %q", retrieved.Load(), sent.Load(), c.Counters(), auth.Load())
	}
}
