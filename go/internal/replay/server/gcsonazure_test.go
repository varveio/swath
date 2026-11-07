// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/store"
)

// nativeProperties is the Properties element order of every Blob in the
// native Azure evidence captures (docs/replay-provider-evidence.tar.gz,
// azure/captures, service version 2026-06-06): the replay writes a
// subsequence of it.
var nativeProperties = []string{"Creation-Time", "Last-Modified", "Etag", "Content-Length", "Content-Type", "Content-Encoding",
	"Content-Language", "Content-CRC64", "Content-MD5", "Cache-Control", "Content-Disposition", "BlobType", "AccessTier",
	"AccessTierInferred", "LeaseStatus", "LeaseState", "ServerEncrypted"}

var (
	blobRe       = regexp.MustCompile(`<Blob><Name>.*?</Blob>`)
	propertyRe   = regexp.MustCompile(`<([A-Za-z0-9-]+)>[^<]*</[A-Za-z0-9-]+>`)
	nextMarkerRe = regexp.MustCompile(`<NextMarker>([^<]*)</NextMarker>`)
)

// azureWalk lists the container with query on the Azure route, following
// the markers, and returns every Blob element.
func azureWalk(t *testing.T, s *Server, query string) []string {
	t.Helper()
	var got []string
	for marker := ""; ; {
		q := "/replay/bucket?restype=container&comp=list" + query
		if marker != "" {
			q += "&marker=" + url.QueryEscape(marker)
		}
		r := azureDo(t, s, "GET", q, "x-ms-version", "2026-06-06")
		if r.status != 200 {
			t.Fatalf("%s: %d %s", q, r.status, r.body)
		}
		got = append(got, blobRe.FindAllString(r.body, -1)...)
		m := nextMarkerRe.FindStringSubmatch(r.body)
		if m == nil || m[1] == "" {
			return got
		}
		marker = m[1]
	}
}

// TestGCSRecordingOnTheAzureRoute serves a swath/gcs-objects/v1 recording
// on the Azure route and requires each Blob's Content-Type to be the
// recorded content type and its Content-MD5 the recorded MD5 (GCS lists
// it as the standard base64 of 16 bytes, the form Content-MD5 takes),
// placed where native listings place them; the placeholder type and no
// Content-MD5 where the recording holds none, an MD5 in another form, or
// a content type Azure could not hold (a control character); flat, paged
// and rolled up, with the window cache and without it. Before, every
// Blob had the placeholder type and no Content-MD5.
func TestGCSRecordingOnTheAzureRoute(t *testing.T) {
	base := func(name string) map[string]any {
		return map[string]any{"name": name, "size": "5", "generation": "1762400000000001", "metageneration": "1",
			"etag": "CAEQAQ==", "storageClass": "STANDARD", "updated": "2025-11-06T03:33:20.123456Z"}
	}
	whole := base("a/whole.txt")
	whole["md5Hash"], whole["contentType"] = "Hn9gS4ZBWt6U4V/vhidgmw==", "text/plain; charset=utf-8"
	composite := base("b/composite.gz")
	composite["contentType"] = "application/gzip"
	none := base("c/none")
	badMD5 := base("d/bad-md5")
	badMD5["md5Hash"], badMD5["contentType"] = "bm90IGFuIE1ENQ==", `a&b<c>"d'`
	control := base("e/control")
	control["md5Hash"], control["contentType"] = "1B2M2Y8AsgTpgAmY7PhCfg==", "text/plain\x01"
	dir := recordGCS(t, map[string]map[string]any{"a/whole.txt": whole, "b/composite.gz": composite, "c/none": none,
		"d/bad-md5": badMD5, "e/control": control})
	blob := func(name, content string) string {
		return "<Blob><Name>" + name + "</Name><Properties><Last-Modified>Thu, 06 Nov 2025 03:33:20 GMT</Last-Modified>" +
			"<Etag>0x000000000000001</Etag><Content-Length>5</Content-Length>" + content +
			"<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>" +
			"<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob>"
	}
	all := []string{
		blob("a/whole.txt", "<Content-Type>text/plain; charset=utf-8</Content-Type><Content-MD5>Hn9gS4ZBWt6U4V/vhidgmw==</Content-MD5>"),
		blob("b/composite.gz", "<Content-Type>application/gzip</Content-Type>"),
		blob("c/none", "<Content-Type>application/octet-stream</Content-Type>"),
		blob("d/bad-md5", "<Content-Type>a&amp;b&lt;c&gt;&quot;d&apos;</Content-Type>"),
		blob("e/control", "<Content-Type>application/octet-stream</Content-Type><Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5>"),
	}
	for _, w := range all {
		var names []string
		for _, m := range propertyRe.FindAllStringSubmatch(w, -1) {
			if m[1] != "Name" {
				names = append(names, m[1])
			}
		}
		for i := 1; i < len(names); i++ {
			if slices.Index(nativeProperties, names[i-1]) >= slices.Index(nativeProperties, names[i]) || !slices.Contains(nativeProperties, names[i]) {
				t.Fatalf("%v is not in the native order %v", names, nativeProperties)
			}
		}
	}
	for _, opts := range []store.Options{{}, {Prefetch: store.Prefetch{Off: true}}} {
		s := serveRecording(t, dir, "swath/gcs-objects/v1", opts)
		for _, c := range []struct {
			query string
			want  []string
		}{
			{"", all},
			{"&maxresults=2", all},
			{"&delimiter=/", nil},
			{"&prefix=a/&delimiter=/", all[:1]},
			{"&prefix=d/", all[3:4]},
		} {
			if got := azureWalk(t, s, c.query); strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Errorf("prefetch off %v, query %q:\n%s\nwant\n%s", opts.Prefetch.Off, c.query, strings.Join(got, "\n"), strings.Join(c.want, "\n"))
			}
		}
		// The S3 route's view of the same rows is unchanged: no content
		// type, the ETag from the MD5 (TestGCSRecordingOnTheS3Route).
		if got := s3Walk(t, s, "&prefix=a/"); len(got) != 1 || strings.Contains(got[0], "text/plain") ||
			!strings.Contains(got[0], "<ETag>&quot;1e7f604b86415ade94e15fef8627609b&quot;</ETag>") {
			t.Errorf("S3: %v", got)
		}
	}
}
