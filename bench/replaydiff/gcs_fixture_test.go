// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// gcsToken is a gcs1. token with the given flag, progress, binding byte
// (repeated) and boundary.
func gcsToken(flag, progress, bind byte, boundary string) string {
	raw := append([]byte{flag, progress}, bytes.Repeat([]byte{bind}, gcsBindingBytes)...)
	return "gcs1." + base64.RawURLEncoding.EncodeToString(append(raw, boundary...))
}

// TestGCSTokenEffect: comparing different fixtures, two answers whose
// tokens differ only in the scope binding agree (and are counted); a
// token that resumes elsewhere, a missing token, an undecodable one and
// any other byte still differ. Without the mode a binding difference is
// a failure.
func TestGCSTokenEffect(t *testing.T) {
	h := http.Header{"Content-Type": {"application/json; charset=utf-8"}}
	page := func(tok string) *response {
		body := `{"kind":"storage#objects","items":[{"name":"a"}]`
		if tok != "" {
			body += `,"nextPageToken":"` + tok + `"`
		}
		return &response{200, h, []byte(body + "}")}
	}
	c := gcsComparator{tokenEffect: true, effectOnly: new(atomic.Int64)}
	ref := page(gcsToken(0, 0, 1, "a"))
	if d := c.compare(ref, page(gcsToken(0, 0, 2, "a"))); !d.OK || c.effectOnly.Load() != 1 {
		t.Errorf("same effect, other binding: %+v (%d counted)", d, c.effectOnly.Load())
	}
	if d := c.compare(ref, page(gcsToken(0, 0, 1, "a"))); !d.OK || c.effectOnly.Load() != 1 {
		t.Errorf("identical answers: %+v (%d counted)", d, c.effectOnly.Load())
	}
	for name, got := range map[string]*response{
		"another boundary":  page(gcsToken(0, 0, 2, "b")),
		"a longer boundary": page(gcsToken(0, 0, 2, "a\x00")),
		"another flag":      page(gcsToken(1, 0, 2, "a")),
		"another progress":  page(gcsToken(0, 1, 2, "a")),
	} {
		if d := c.compare(ref, got); d.OK || !strings.Contains(d.Detail, "differ in effect") {
			t.Errorf("%s: %+v", name, d)
		}
	}
	for name, got := range map[string]*response{
		"no token":          page(""),
		"undecodable token": page("gcs1.!!"),
		"another item":      {200, h, bytes.Replace(page(gcsToken(0, 0, 2, "a")).Body, []byte(`"a"`), []byte(`"b"`), 1)},
		"another status":    {400, h, page(gcsToken(0, 0, 2, "a")).Body},
	} {
		if d := c.compare(ref, got); d.OK {
			t.Errorf("%s compared equal", name)
		}
	}
	if d := c.compare(page(""), page("")); !d.OK {
		t.Errorf("no tokens on either: %+v", d)
	}
	if d := (gcsComparator{}).compare(ref, page(gcsToken(0, 0, 2, "a"))); d.OK {
		t.Error("a binding difference passed without the mode")
	}
}

// gcsServer serves keys on the GCS route under a fixture identity.
func gcsServer(t *testing.T, identity string, keys []string) string {
	t.Helper()
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
		ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: true}, keys...),
		ServingMode: "MEMORY", FixtureIdentity: identity, Protocols: []string{server.ProtocolGCS}})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	return fmt.Sprintf("http://127.0.0.1:%d", srv.Port())
}

var pageTokenParam = regexp.MustCompile(`(^|&)pageToken=([^&]*)`)

// gcsEditingProxy serves base's answers with each request's pageToken
// passed through in and each answer's body through out (nil: unchanged).
func gcsEditingProxy(t *testing.T, base string, in func(string) string, out func([]byte) []byte) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	p := httputil.NewSingleHostReverseProxy(u)
	director := p.Director
	p.Director = func(r *http.Request) {
		director(r)
		if in != nil {
			r.URL.RawQuery = pageTokenParam.ReplaceAllStringFunc(r.URL.RawQuery, func(m string) string {
				sep, v, _ := strings.Cut(m, "pageToken=")
				tok, err := url.QueryUnescape(v)
				if err != nil {
					return m
				}
				return sep + "pageToken=" + gcsEscape(in(tok))
			})
		}
	}
	p.ModifyResponse = func(resp *http.Response) error {
		if out == nil {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		body = out(body)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", fmt.Sprint(len(body)))
		return nil
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv.URL
}

// editTokens passes every nextPageToken value of a body through edit.
func editTokens(edit func(string) string) func([]byte) []byte {
	return func(b []byte) []byte {
		return tokenValue.ReplaceAllFunc(b, func(m []byte) []byte {
			v := m[len(`"nextPageToken":"`) : len(m)-1]
			return []byte(`"nextPageToken":"` + edit(string(v)) + `"`)
		})
	}
}

// editRaw applies f to a token's decoded bytes; a value that does not
// decode is returned as it is.
func editRaw(f func([]byte) []byte) func(string) string {
	return func(tok string) string {
		body, ok := strings.CutPrefix(tok, "gcs1.")
		raw, err := base64.RawURLEncoding.DecodeString(body)
		if !ok || err != nil || len(raw) < 2+gcsBindingBytes {
			return tok
		}
		return "gcs1." + base64.RawURLEncoding.EncodeToString(f(raw))
	}
}

// TestGCSDifferentFixtures runs the GCS request set against two Go
// servers over the same keys under different fixture identities, as the
// Java server on a Java-era fixture and the Go server on its SeekParquet
// conversion are: every token's bytes differ, and with the token-effect
// mode the run passes, each server resuming from its own tokens, with the
// foreign-token cases refused by both. A Go side whose tokens resume
// elsewhere, that issues no token, that refuses its own tokens or that
// accepts a foreign one fails.
func TestGCSDifferentFixtures(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("d%d/s%d/k%03d", i%3, i%7, i), fmt.Sprintf("top%02d", i%40), fmt.Sprintf("x/%c y", 'a'+i%26))
	}
	keys = dedupe(keys)
	client := &http.Client{}
	ref := &target{name: "java", base: gcsServer(t, "java-era", keys), client: client}
	all, err := gcsListAll(ref, "bkt")
	if err != nil || len(all) != len(keys) {
		t.Fatalf("%d keys, %v", len(all), err)
	}
	o := options{bucket: "bkt", parallel: 4, maxPrefixes: 50, maxTop: 10, walkSizes: "1,7,1000", random: 200, seed: 9}
	run := func(base string, effect bool) *report {
		rep := &report{}
		comp := gcsComparator{tokenEffect: effect}
		if effect {
			comp.effectOnly = new(atomic.Int64)
		}
		d := &driver{o: o, ref: ref, got: &target{name: "go", base: base, client: client}, comp: comp, rep: rep}
		d.runGCSAll(all, rand.New(rand.NewPCG(9, 9)))
		return rep
	}
	failed := func(rep *report) []string {
		var out []string
		for _, r := range rep.Results {
			if !r.Pass {
				out = append(out, r.Name)
			}
		}
		return out
	}
	goBase := gcsServer(t, "seekparquet", keys)

	rep := run(goBase, true)
	if rep.Failed != 0 || rep.Passed < 700 || rep.SkippedCount != 0 || rep.TokenEffectOnly == 0 {
		for _, r := range rep.Results {
			if !r.Pass {
				t.Errorf("%s %q: %+v %s", r.Kind, r.Name, r.diff, r.Error)
				break
			}
		}
		t.Fatalf("%d passed, %d failed, skipped %v, %d by effect", rep.Passed, rep.Failed, rep.Skipped, rep.TokenEffectOnly)
	}
	foreign := 0
	for _, r := range rep.Results {
		if strings.HasPrefix(r.Name, gcsForeignPrefix) && r.Pass {
			foreign++
		}
	}
	if foreign != 3 {
		t.Errorf("%d foreign-token cases passed, want 3", foreign)
	}
	if len(rep.TokenBytes) != 0 {
		t.Errorf("walk pages listed as token byte differences in the effect mode: %d", len(rep.TokenBytes))
	}

	// Byte for byte, the same pair fails: every case that resumes from a
	// token is refused by the Go server.
	if rep := run(goBase, false); rep.Failed == 0 {
		t.Error("different fixtures passed byte for byte")
	}

	// One identity on both sides: no token is foreign, so the
	// foreign-token cases are skipped, never passed.
	rep = run(gcsServer(t, "java-era", keys), true)
	if rep.Failed != 0 || rep.SkippedCount != 3 || rep.TokenEffectOnly != 0 {
		t.Errorf("one identity: %d failed (%q), skipped %v, %d by effect", rep.Failed, failed(rep), rep.Skipped, rep.TokenEffectOnly)
	}

	for _, c := range []struct {
		name    string
		in      func(string) string
		out     func([]byte) []byte
		mention string // a failure's error or detail must say this
	}{
		{"a token that resumes elsewhere", nil, editTokens(editRaw(func(b []byte) []byte { return append(b, 0) })), "differ in effect"},
		{"no token", nil, func(b []byte) []byte { return tokenValue.ReplaceAll(b, []byte(`"x":""`)) }, "issued no token"},
		{"its own token refused", func(string) string { return "gcs1.AAAA" }, nil, ""},
	} {
		rep := run(gcsEditingProxy(t, goBase, c.in, c.out), true)
		walks := 0
		for _, r := range rep.Results {
			if r.Kind == "walk" && !r.Pass {
				walks++
			}
		}
		if rep.Failed == 0 || walks == 0 {
			t.Errorf("%s: %d failed, %d walks", c.name, rep.Failed, walks)
			continue
		}
		mentioned := c.mention == ""
		for _, r := range rep.Results {
			if !r.Pass && (strings.Contains(r.Error, c.mention) || r.diff != nil && strings.Contains(r.Detail, c.mention)) {
				mentioned = true
			}
		}
		if !mentioned {
			t.Errorf("%s: no failure says %q: %q", c.name, c.mention, failed(rep))
		}
	}

	// A Go side that accepts the reference's token: a proxy in front of
	// a server of the reference's identity that issues its tokens with
	// another binding, turns its own back and passes any other through.
	var mu sync.Mutex
	issued := map[string]string{}
	flip := editRaw(func(b []byte) []byte {
		for i := 2; i < 2+gcsBindingBytes; i++ {
			b[i] ^= 0xff
		}
		return b
	})
	in := func(tok string) string {
		mu.Lock()
		defer mu.Unlock()
		if orig, ok := issued[tok]; ok {
			return orig
		}
		return tok
	}
	out := editTokens(func(tok string) string {
		f := flip(tok)
		mu.Lock()
		issued[f] = tok
		mu.Unlock()
		return f
	})
	rep = run(gcsEditingProxy(t, gcsServer(t, "java-era", keys), in, out), true)
	accepted := 0
	for _, r := range rep.Results {
		if strings.HasPrefix(r.Name, gcsForeignPrefix) && !r.Pass && strings.Contains(r.Error, "token of the other fixture was served") {
			accepted++
		}
	}
	if accepted != 3 {
		t.Errorf("foreign tokens accepted: %d of 3 cases failed for it (%q)", accepted, failed(rep))
	}
}
