// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
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
	n := p.retrieved.Add(1)
	if n > p.sent.Load()+1 {
		return aws.Credentials{}, errors.New("a second lookup for one request")
	}
	return aws.Credentials{AccessKeyID: fmt.Sprintf("AKTEST%d", n), SecretAccessKey: "s", SessionToken: fmt.Sprintf("token%d", n), Source: "test"}, nil
}

// TestCredentialsRetrievedOncePerAttempt: the credentials are looked up
// once per attempt, before it is counted, and the request is signed with
// them: the SDK does not look them up again inside the counted call.
func TestCredentialsRetrievedOncePerAttempt(t *testing.T) {
	s3test.NoCredentials(t)
	var retrieved, sent atomic.Int32
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := sent.Add(1)
		if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKTEST%d/", n)) || r.Header.Get("X-Amz-Security-Token") != fmt.Sprintf("token%d", n) {
			t.Errorf("attempt %d: authorization %q, token %q", n, auth, r.Header.Get("X-Amz-Security-Token"))
		}
		if n < 3 {
			w.WriteHeader(500)
			io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
			return
		}
		io.WriteString(w, okPage)
	}))
	c := newTest(t, testConfig(url))
	c.auth.credentials, c.auth.anonymous, c.auth.source = onePerRequest{&retrieved, &sent}, false, "test"
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if retrieved.Load() != 3 || sent.Load() != 3 || c.Counters().Attempts != 3 || c.Counters().Retries != 2 {
		t.Fatalf("%d lookups for %d requests, %+v", retrieved.Load(), sent.Load(), c.Counters())
	}
}
