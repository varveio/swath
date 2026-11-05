// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Credentials sign requests. Source says where they came from.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Source          string // "environment", "profile NAME", or "anonymous"
}

// Anonymous reports whether requests go unsigned.
func (c Credentials) Anonymous() bool { return c.AccessKeyID == "" }

// ErrUnsupportedCredentials marks a credential source this client does not
// implement yet (instance roles, SSO, web identity, assume-role, processes).
var ErrUnsupportedCredentials = errors.New("unsupported credential source")

// Env reads the process environment; tests replace it.
type Env func(string) string

// resolveCredentials finds credentials, in order:
//  1. anonymous when asked;
//  2. with an explicit profile (the flag), that profile;
//  3. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN);
//  4. the profile AWS_PROFILE names, else "default", from the shared
//     credentials file (AWS_SHARED_CREDENTIALS_FILE, ~/.aws/credentials)
//     and config file (AWS_CONFIG_FILE, ~/.aws/config);
//  5. anonymous, when none of that yields keys and nothing points at an
//     unsupported source.
//
// It also returns the profile's region, if any.
func resolveCredentials(profile string, anonymous bool, env Env) (Credentials, string, error) {
	if anonymous {
		return Credentials{Source: "anonymous"}, "", nil
	}
	if profile == "" {
		id, secret := env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")
		switch {
		case id != "" && secret != "":
			return Credentials{AccessKeyID: id, SecretAccessKey: secret, SessionToken: env("AWS_SESSION_TOKEN"), Source: "environment"}, "", nil
		case id != "" || secret != "":
			return Credentials{}, "", errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together")
		}
	}
	explicit := profile != ""
	if profile == "" {
		profile = env("AWS_PROFILE")
		explicit = profile != ""
	}
	if profile == "" {
		profile = "default"
	}
	home := env("HOME")
	if home == "" {
		home = env("USERPROFILE")
	}
	credFile := firstNonEmpty(env("AWS_SHARED_CREDENTIALS_FILE"), filepath.Join(home, ".aws", "credentials"))
	confFile := firstNonEmpty(env("AWS_CONFIG_FILE"), filepath.Join(home, ".aws", "config"))
	creds, credFound, err := readSection(credFile, profile)
	if err != nil {
		return Credentials{}, "", err
	}
	confName := "profile " + profile
	if profile == "default" {
		confName = "default"
	}
	conf, confFound, err := readSection(confFile, confName)
	if err != nil {
		return Credentials{}, "", err
	}
	region := conf["region"]
	get := func(k string) string { return firstNonEmpty(creds[k], conf[k]) }
	if id, secret := get("aws_access_key_id"), get("aws_secret_access_key"); id != "" || secret != "" {
		if id == "" || secret == "" {
			return Credentials{}, "", fmt.Errorf("profile %s: aws_access_key_id and aws_secret_access_key must be set together", profile)
		}
		return Credentials{AccessKeyID: id, SecretAccessKey: secret, SessionToken: get("aws_session_token"), Source: "profile " + profile}, region, nil
	}
	for _, k := range []string{"role_arn", "sso_session", "sso_start_url", "credential_process", "web_identity_token_file", "credential_source"} {
		if get(k) != "" {
			return Credentials{}, "", fmt.Errorf("%w: profile %s uses %s; pass -anonymous for a public bucket, or export keys", ErrUnsupportedCredentials, profile, k)
		}
	}
	if explicit && !credFound && !confFound {
		return Credentials{}, "", fmt.Errorf("profile %q not found in %s or %s", profile, credFile, confFile)
	}
	for _, k := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"} {
		if env(k) != "" {
			return Credentials{}, "", fmt.Errorf("%w: %s is set; pass -anonymous for a public bucket, or export keys", ErrUnsupportedCredentials, k)
		}
	}
	return Credentials{Source: "anonymous"}, region, nil
}

// readSection reads one [section] of an AWS INI file: key = value lines,
// comments starting with # or ;, indented (nested) lines ignored. A
// missing file is an empty section.
func readSection(path, section string) (map[string]string, bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	out, found, in := map[string]string{}, false, false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		raw := sc.Text()
		line := strings.TrimSpace(raw)
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' && line[len(line)-1] == ']' {
			in = strings.TrimSpace(line[1:len(line)-1]) == section
			found = found || in
			continue
		}
		if !in || raw[0] == ' ' || raw[0] == '\t' {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			out[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	return out, found, nil
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}
