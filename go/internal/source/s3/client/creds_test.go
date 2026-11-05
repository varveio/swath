// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveCredentials(t *testing.T) {
	dir := t.TempDir()
	cred := filepath.Join(dir, "credentials")
	conf := filepath.Join(dir, "config")
	os.WriteFile(cred, []byte(`
[default]
aws_access_key_id = AKDEFAULT
aws_secret_access_key = sdefault

[work]
aws_access_key_id=AKWORK
aws_secret_access_key=swork
aws_session_token = tok
; comment
[half]
aws_access_key_id = AKHALF
`), 0o600)
	os.WriteFile(conf, []byte(`
[default]
region = eu-central-1
[profile work]
region = ap-south-1
s3 =
  max_concurrent_requests = 20
[profile role]
role_arn = arn:aws:iam::1:role/x
source_profile = default
[profile sso]
sso_session = corp
[profile confkeys]
aws_access_key_id = AKCONF
aws_secret_access_key = sconf
`), 0o600)
	files := map[string]string{"AWS_SHARED_CREDENTIALS_FILE": cred, "AWS_CONFIG_FILE": conf, "HOME": dir}
	cases := []struct {
		name      string
		profile   string
		anonymous bool
		env       map[string]string
		key       string // "" = anonymous
		token     string
		region    string
		err       string
	}{
		{name: "anonymous wins over env", anonymous: true, env: map[string]string{"AWS_ACCESS_KEY_ID": "AKENV", "AWS_SECRET_ACCESS_KEY": "s"}},
		{name: "env", env: map[string]string{"AWS_ACCESS_KEY_ID": "AKENV", "AWS_SECRET_ACCESS_KEY": "s", "AWS_SESSION_TOKEN": "t"}, key: "AKENV", token: "t"},
		{name: "env half", env: map[string]string{"AWS_ACCESS_KEY_ID": "AKENV"}, err: "set together"},
		{name: "default profile", env: files, key: "AKDEFAULT", region: "eu-central-1"},
		{name: "AWS_PROFILE", env: with(files, "AWS_PROFILE", "work"), key: "AKWORK", token: "tok", region: "ap-south-1"},
		{name: "flag beats env keys", profile: "work", env: with(files, "AWS_ACCESS_KEY_ID", "AKENV", "AWS_SECRET_ACCESS_KEY", "s"), key: "AKWORK", token: "tok", region: "ap-south-1"},
		{name: "keys in config", profile: "confkeys", env: files, key: "AKCONF"},
		{name: "half profile", profile: "half", env: files, err: "set together"},
		{name: "assume role deferred", profile: "role", env: files, err: "role_arn"},
		{name: "sso deferred", profile: "sso", env: files, err: "sso_session"},
		{name: "missing explicit profile", profile: "nope", env: files, err: "not found"},
		{name: "nothing: anonymous", env: map[string]string{"HOME": t.TempDir()}},
		{name: "web identity deferred", env: map[string]string{"HOME": t.TempDir(), "AWS_WEB_IDENTITY_TOKEN_FILE": "/x"}, err: "AWS_WEB_IDENTITY_TOKEN_FILE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := func(k string) string { return c.env[k] }
			got, region, err := resolveCredentials(c.profile, c.anonymous, env)
			if c.err != "" {
				if err == nil || !strings.Contains(err.Error(), c.err) {
					t.Fatalf("err %v, want %q", err, c.err)
				}
				if strings.Contains(c.name, "deferred") && !errors.Is(err, ErrUnsupportedCredentials) {
					t.Fatalf("err %v is not ErrUnsupportedCredentials", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.AccessKeyID != c.key || got.SessionToken != c.token || region != c.region {
				t.Fatalf("got %+v region %q", got, region)
			}
		})
	}
}

func with(m map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	for i := 0; i < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}
