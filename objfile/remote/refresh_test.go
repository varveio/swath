// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestOpenDestinationS3RefreshesAhead: a destination's expiring
// credentials (web identity through STS here) are refreshed ahead of their
// expiry: ones valid for 4 seconds are replaced after 3.
func TestOpenDestinationS3RefreshesAhead(t *testing.T) {
	awsEnv(t)
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	cfg := filepath.Join(dir, "config")
	if err := os.WriteFile(token, []byte("jwt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg, []byte("[profile dst]\nrole_arn = arn:aws:iam::111111111111:role/w\nweb_identity_token_file = "+token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfg)
	var calls atomic.Int32
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := calls.Add(1)
		exp := time.Now().Add(4 * time.Second).UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AKWEB%d</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, i, exp)
	}))
	t.Cleanup(sts.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	_, url, auth := s3Destination(t)
	d, err := OpenDestination(context.Background(), "s3://bkt/ds", Options{Endpoint: url, Profile: "dst", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	signed := func(key string) bool {
		found := false
		auth.Range(func(k, _ any) bool {
			found = found || strings.HasPrefix(k.(string), "AWS4-HMAC-SHA256 Credential="+key+"/")
			return !found
		})
		return found
	}
	if _, err := d.Writer().PutIfAbsent(context.Background(), "a", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if !signed("AKWEB1") || signed("AKWEB2") {
		t.Fatal("the first write was not signed with the first credentials")
	}
	time.Sleep(3200 * time.Millisecond)
	if _, err := d.Writer().PutIfAbsent(context.Background(), "b", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if !signed("AKWEB2") || calls.Load() != 2 {
		t.Fatalf("after 3 seconds: %d STS calls, AKWEB2 signed %v", calls.Load(), signed("AKWEB2"))
	}
}
