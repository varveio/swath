// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/replay/store"
	sourcegcs "github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// gcsProbe is one object of the probes of 2026-11-07 against Cloud
// Storage (anonymous, public buckets): its JSON API resource and the
// Contents element the XML API's S3-compatible ListObjectsV2 sent for it,
// the reference for a GCS object seen by an S3 client. Names, hashes,
// generations and times are transcribed onto synthetic values of the same
// shape (an etag is base64 of the generation and metageneration as
// protobuf varints, as the probed ones are).
type gcsProbe struct {
	resource map[string]any
	xmlAPI   string
}

var gcsProbes = []gcsProbe{
	{ // An object uploaded whole: the XML API's ETag is its MD5 in hex.
		map[string]any{"name": "a/scene_ANG.txt", "size": "1170", "generation": "1762400000123456", "metageneration": "1",
			"etag": "CMDE1s/L3JADEAE=", "md5Hash": "Hn9gS4ZBWt6U4V/vhidgmw==", "crc32c": "cimweA==", "storageClass": "STANDARD",
			"contentType": "application/octet-stream", "timeCreated": "2025-11-06T03:33:20.122Z", "updated": "2025-11-06T03:33:20.122Z"},
		`<Contents><Key>a/scene_ANG.txt</Key><Generation>1762400000123456</Generation><MetaGeneration>1</MetaGeneration><LastModified>2025-11-06T03:33:20.122Z</LastModified><ETag>"1e7f604b86415ade94e15fef8627609b"</ETag><Size>1170</Size></Contents>`,
	},
	{ // NEARLINE: the XML API names no StorageClass, whatever the class.
		map[string]any{"name": "b/radar.tar", "size": "130174464", "generation": "1762400100234567", "metageneration": "1",
			"etag": "CMfqtP/L3JADEAE=", "md5Hash": "ULf+TaZHICMsJbx8bWb2xQ==", "crc32c": "s4owng==", "storageClass": "NEARLINE",
			"contentType": "application/x-tar", "timeCreated": "2025-11-06T03:35:00.233Z", "updated": "2025-11-06T03:35:00.233Z"},
		`<Contents><Key>b/radar.tar</Key><Generation>1762400100234567</Generation><MetaGeneration>1</MetaGeneration><LastModified>2025-11-06T03:35:00.233Z</LastModified><ETag>"50b7fe4da64720232c25bc7c6d66f6c5"</ETag><Size>130174464</Size></Contents>`,
	},
	{ // A composite object has no MD5: its ETag is "-" and the JSON etag.
		map[string]any{"name": "c/sites.vcf.gz", "size": "2147424232", "generation": "1762400200345678", "metageneration": "1",
			"etag": "CM6Qk6/M3JADEAE=", "crc32c": "VQpMxQ==", "componentCount": 32, "storageClass": "MULTI_REGIONAL",
			"contentType": "text/x-vcard", "timeCreated": "2025-11-06T03:36:40.344Z", "updated": "2025-11-06T03:36:40.344Z"},
		`<Contents><Key>c/sites.vcf.gz</Key><Generation>1762400200345678</Generation><MetaGeneration>1</MetaGeneration><LastModified>2025-11-06T03:36:40.344Z</LastModified><ETag>"-CM6Qk6/M3JADEAE="</ETag><Size>2147424232</Size></Contents>`,
	},
	{ // A composite object whose metadata changed: the etag carries
		// metageneration 2, and so does the ETag.
		map[string]any{"name": "d/tool.jar", "size": "280287512", "generation": "1762400300456789", "metageneration": "2",
			"etag": "CNW28d7M3JADEAI=", "crc32c": "joK+mA==", "componentCount": 6, "storageClass": "MULTI_REGIONAL",
			"contentType": "application/java-archive", "timeCreated": "2025-11-06T03:38:20.455Z", "updated": "2025-11-06T03:38:20.455Z"},
		`<Contents><Key>d/tool.jar</Key><Generation>1762400300456789</Generation><MetaGeneration>2</MetaGeneration><LastModified>2025-11-06T03:38:20.455Z</LastModified><ETag>"-CNW28d7M3JADEAI="</ETag><Size>280287512</Size></Contents>`,
	},
	{ // An object whose metadata changed after upload: the XML API's
		// LastModified is timeCreated, not updated.
		map[string]any{"name": "e/reads.cram", "size": "15953191127", "generation": "1762400400567890", "metageneration": "2",
			"etag": "CNLcz47N3JADEAI=", "md5Hash": "5Y9eT3FvvB3ZI/fcWL8DFA==", "crc32c": "imEqqA==", "storageClass": "MULTI_REGIONAL",
			"contentType": "application/octet-stream", "timeCreated": "2025-11-06T03:40:00.566Z", "updated": "2025-11-06T08:10:43.105Z"},
		`<Contents><Key>e/reads.cram</Key><Generation>1762400400567890</Generation><MetaGeneration>2</MetaGeneration><LastModified>2025-11-06T03:40:00.566Z</LastModified><ETag>"e58f5e4f716fbc1dd923f7dc58bf0314"</ETag><Size>15953191127</Size></Contents>`,
	},
}

// recordGCS lists objects (resources by name) from the strict GCS fake
// with the Go lister and returns the swath/gcs-objects/v1 dataset it
// wrote.
func recordGCS(t *testing.T, objects map[string]map[string]any) string {
	t.Helper()
	var keys []string
	for k := range objects {
		keys = append(keys, k)
	}
	fake := gcstest.NewStrict(keys)
	fake.Resource = func(_ int, k string) map[string]any {
		o := map[string]any{"kind": "storage#object", "bucket": "bkt"}
		for name, v := range objects[k] {
			o[name] = v
		}
		return o
	}
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := listing.Run(context.Background(), listing.Config{
		Source: listing.SourceConfig{URI: "gs://bkt", Options: &sourcegcs.Options{Endpoint: fake.Serve(t), Anonymous: true}},
		// A dataset is all these tests need: unjournaled where a journaled run is refused (Windows).
		Output: listing.OutputConfig{Dir: out, NoJournal: !dataset.DirSyncEnforced},
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// serveGCSRecording serves dir on the S3 and GCS routes.
func serveGCSRecording(t *testing.T, dir string, opts store.Options) *Server {
	t.Helper()
	st, meta, err := store.Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if meta.SchemaID != "swath/gcs-objects/v1" {
		t.Fatalf("schema %q", meta.SchemaID)
	}
	cfg := config(st)
	cfg.Protocols = []string{"s3", "gcs"}
	return start(t, cfg)
}

var contentsRe = regexp.MustCompile(`<Contents>.*?</Contents>`)

// TestLiveGCSXMLAPIContents serves a recording of the probed objects on
// the S3 route and requires each Contents element to be the one the XML
// API sent, with and without fetch-owner=true, flat and rolled up, read
// through the window cache and without it: the ETag the MD5 in hex, or
// for an object without one (composite) "-" and the etag; no
// StorageClass (STANDARD, NEARLINE or MULTI_REGIONAL), Owner or checksum
// element. Two differences are the S3 route's own: it writes no
// Generation or MetaGeneration (elements S3 does not have), and it writes
// the ETag's quotation marks as &quot;, as live S3 does. LastModified is
// the recorded timeCreated, as the XML API's, also for the last probe,
// whose updated is later. Before, the route sent the GCS etag as the ETag
// and the GCS storage class as StorageClass, and before the lister
// recorded timeCreated, the last probe's LastModified was its updated.
func TestLiveGCSXMLAPIContents(t *testing.T) {
	objects := map[string]map[string]any{}
	var want []string
	for _, p := range gcsProbes {
		objects[p.resource["name"].(string)] = p.resource
		w := regexp.MustCompile(`<Generation>[0-9]+</Generation><MetaGeneration>[0-9]+</MetaGeneration>`).ReplaceAllString(p.xmlAPI, "")
		w = strings.ReplaceAll(w, `"`, "&quot;")
		want = append(want, w)
	}
	dir := recordGCS(t, objects)
	for _, opts := range []store.Options{{}, {Prefetch: store.Prefetch{Off: true}}} {
		s := serveGCSRecording(t, dir, opts)
		for _, query := range []string{"", "&fetch-owner=true", "&max-keys=2", "&prefix=c/&delimiter=/"} {
			var got []string
			for token := ""; ; {
				q := "/bucket?list-type=2" + query
				if token != "" {
					q += "&continuation-token=" + token
				}
				r := get(t, s, q)
				if r.status != 200 {
					t.Fatalf("%s: %d %s", q, r.status, r.body)
				}
				got = append(got, contentsRe.FindAllString(r.body, -1)...)
				m := regexp.MustCompile(`<NextContinuationToken>([^<]*)<`).FindStringSubmatch(r.body)
				if m == nil {
					break
				}
				token = m[1]
			}
			wantHere := want
			if strings.Contains(query, "prefix=c/") {
				wantHere = want[2:3]
			}
			if strings.Join(got, "\n") != strings.Join(wantHere, "\n") {
				t.Errorf("prefetch off %v, query %q:\n%s\nwant\n%s", opts.Prefetch.Off, query, strings.Join(got, "\n"), strings.Join(wantHere, "\n"))
			}
		}
	}
}

// TestGCSRecordingOnTheS3Route serves a swath/gcs-objects/v1 recording
// whose rows have no source value for the S3 ETag (neither an MD5 nor an
// etag), an MD5 that is not one (it does not decode to 16 bytes, so the
// etag answers), an MD5 and no etag, and a non-STANDARD class, and
// requires the S3 route's ETag and StorageClass as docs/internals/swath-replay.md's
// route table states them, while the GCS route answers the same rows
// exactly as stored. Before, the S3 route sent the GCS etag and storage
// class, and no ETag for a row without an etag.
func TestGCSRecordingOnTheS3Route(t *testing.T) {
	base := func(name string) map[string]any {
		return map[string]any{"name": name, "size": "5", "generation": "1762400000000001", "metageneration": "1",
			"storageClass": "COLDLINE", "updated": "2025-11-06T03:33:20.123456Z"}
	}
	none := base("none")
	badMD5 := base("bad-md5")
	badMD5["md5Hash"], badMD5["etag"] = "bm90IGFuIE1ENQ==", "CAEQAQ=="
	md5Only := base("md5-only")
	md5Only["md5Hash"] = "1B2M2Y8AsgTpgAmY7PhCfg=="
	archive := base("archive")
	archive["md5Hash"], archive["etag"], archive["storageClass"] = "Hn9gS4ZBWt6U4V/vhidgmw==", "CAEQAQ==", "ARCHIVE"
	dir := recordGCS(t, map[string]map[string]any{"none": none, "bad-md5": badMD5, "md5-only": md5Only, "archive": archive})
	s := serveGCSRecording(t, dir, store.Options{})
	want := strings.Join([]string{
		`<Contents><Key>archive</Key><LastModified>2025-11-06T03:33:20.123Z</LastModified><ETag>&quot;1e7f604b86415ade94e15fef8627609b&quot;</ETag><Size>5</Size></Contents>`,
		`<Contents><Key>bad-md5</Key><LastModified>2025-11-06T03:33:20.123Z</LastModified><ETag>&quot;-CAEQAQ==&quot;</ETag><Size>5</Size></Contents>`,
		`<Contents><Key>md5-only</Key><LastModified>2025-11-06T03:33:20.123Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>5</Size></Contents>`,
		`<Contents><Key>none</Key><LastModified>2025-11-06T03:33:20.123Z</LastModified><ETag>&quot;swath-s3-synthetic-v1&quot;</ETag><Size>5</Size></Contents>`,
	}, "\n")
	for _, query := range []string{"", "&fetch-owner=true", "&delimiter=/"} {
		r := get(t, s, "/bucket?list-type=2"+query)
		if got := strings.Join(contentsRe.FindAllString(r.body, -1), "\n"); r.status != 200 || got != want {
			t.Errorf("S3 %q: %d\n%s\nwant\n%s", query, r.status, got, want)
		}
	}
	r := get(t, s, "/storage/v1/b/bucket/o")
	for _, field := range []string{`"etag":"swath-gcs-synthetic-v1","generation":"1762400000000001","metageneration":"1","storageClass":"COLDLINE"`,
		`"etag":"CAEQAQ==","generation":"1762400000000001","metageneration":"1","storageClass":"ARCHIVE"`,
		`"md5Hash":"bm90IGFuIE1ENQ=="`, `"md5Hash":"1B2M2Y8AsgTpgAmY7PhCfg=="`, `"updated":"2025-11-06T03:33:20.123456Z"`} {
		if r.status != 200 || !strings.Contains(r.body, field) {
			t.Errorf("GCS: %d, no %s in\n%s", r.status, field, r.body)
		}
	}
}
