// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/azure"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The reference comparator's own tests, ported to the Go comparator.

const nativeXML = `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://live.blob.core.windows.net/" ContainerName="live">
  <Prefix>a/</Prefix><Blobs>
    <BlobPrefix><Name Encoded="true">a%EF%BF%BE/</Name></BlobPrefix>
    <Blob><Name> leading &amp; trailing </Name><Properties>
      <Creation-Time>Thu, 01 Jan 2026 00:00:00 GMT</Creation-Time>
      <Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>
      <Etag>native</Etag><Content-Length>12</Content-Length><Content-Type>text/plain</Content-Type>
      <BlobType>BlockBlob</BlobType><AccessTier>Cool</AccessTier><LeaseStatus>unlocked</LeaseStatus>
    </Properties></Blob>
  </Blobs><NextMarker></NextMarker>
</EnumerationResults>
`

const replayXML = `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="http://127.0.0.1:1234/replay/" ContainerName="bucket">
  <Prefix>a/</Prefix><Blobs>
    <BlobPrefix><Name Encoded="true">a%EF%BF%BE/</Name></BlobPrefix>
    <Blob><Name> leading &amp; trailing </Name><Properties>
      <Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>
      <Etag>0x000000000000001</Etag><Content-Length>12</Content-Length>
      <Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>
      <AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>
      <LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState>
    </Properties></Blob>
  </Blobs><NextMarker></NextMarker>
</EnumerationResults>
`

func comparePages(n, r string) error {
	return catch(func() {
		compareParsed(parsePage([]byte(n), false), parsePage([]byte(r), true), "https://live.blob.core.windows.net/",
			"http://127.0.0.1:1234/replay/", "live", "bucket", nil, nil, true)
	})
}

func wantMismatch(t *testing.T, err error, part string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), part) {
		t.Errorf("got %v, want a mismatch naming %q", err, part)
	}
}

// AzureProfileComparatorTest.comparesExactMembershipTextOrderAndTypedSyntheticFields.
func TestComparatorAcceptsTheProfile(t *testing.T) {
	if err := comparePages(nativeXML, replayXML); err != nil {
		t.Fatal(err)
	}
}

// AzureProfileComparatorTest.nativeOnlyEmptyOrMetadataAfterPropertiesIsProfileOmitted.
func TestComparatorOmitsNativeOrMetadata(t *testing.T) {
	with := strings.Replace(nativeXML, "</Properties></Blob>", "<Content-CRC64 /><Content-MD5>bWQ1</Content-MD5>"+
		"<ServerEncrypted>true</ServerEncrypted></Properties><OrMetadata /></Blob>", 1)
	if err := comparePages(with, replayXML); err != nil {
		t.Fatal(err)
	}
	wantMismatch(t, comparePages(strings.Replace(with, "<OrMetadata />", "<OrMetadata><Rule/></OrMetadata>", 1), replayXML), "unexpected field after Properties")
	if comparePages(strings.Replace(with, "<OrMetadata />", "<OrMetadata>x</OrMetadata>", 1), replayXML) == nil {
		t.Error("non-empty OrMetadata accepted")
	}
	wantMismatch(t, comparePages(with, strings.Replace(replayXML, "</Properties></Blob>", "</Properties><OrMetadata /></Blob>", 1)), "unexpected field after Properties")
}

// AzureProfileComparatorTest.strictReplayPolicyAcceptsActualAzureRendererOutput.
func TestComparatorAcceptsTheRenderer(t *testing.T) {
	size := int64(12)
	tm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := store.Object{Key: []byte(" leading & trailing "), Size: &size, LastModified: &tm}
	res := &azure.Result{Request: &azure.Request{Account: "replay", Container: "bucket", Version: azure.Version20260606,
		Prefix: "a/", PrefixSupplied: true, PageSize: 2},
		Entries: []azure.Entry{{Prefix: []byte("a\ufffe/")}, {Object: &o}}}
	body, err := azure.AppendList(nil, res, "http://127.0.0.1:1234/replay/")
	if err != nil {
		t.Fatal(err)
	}
	if err := comparePages(strings.Replace(nativeXML, "a%EF%BF%BE/", "a%EF%BF%BE%2F", 1), string(body)); err != nil {
		t.Fatal(err)
	}
}

// AzureProfileComparatorTest.refusesSizeTextAttributeAndElementOrderChanges.
func TestComparatorRefusesChanges(t *testing.T) {
	for _, c := range []struct{ from, to, part string }{
		{"<Content-Length>12</Content-Length>", "<Content-Length>13</Content-Length>", "Content-Length"},
		{" leading &amp; trailing ", "leading &amp; trailing", "Name text"},
		{`Encoded="true"`, `Encoded="true" Extra="x"`, "attribute"},
		{"<Etag>0x000000000000001</Etag>", "<Etag>0x000000000000001</Etag><Future>bad</Future>", "unknown Azure Properties field"},
		{"<AccessTier>Hot</AccessTier>", "<AccessTier>Cool</AccessTier>", "AccessTier"},
		{"00:00:00 GMT", "00:00:01 GMT", "Last-Modified"},
		{`ContainerName="bucket"`, `ContainerName="wrong"`, "replay container"},
		{`/replay/"`, `/replay"`, "trailing slash"},
		{"<Prefix>a/</Prefix>", "<Prefix>wrong/</Prefix>", "Prefix echo"},
		{"<Last-Modified>", "<Creation-Time>Thu, 01 Jan 2026 00:00:00 GMT</Creation-Time><Last-Modified>", "unknown Azure Properties field"},
	} {
		wantMismatch(t, comparePages(nativeXML, strings.Replace(replayXML, c.from, c.to, 1)), c.part)
	}
	if comparePages(nativeXML, strings.Replace(replayXML, "<BlobPrefix>", "<Blob>", 1)) == nil {
		t.Error("a BlobPrefix turned Blob accepted")
	}
	prefix := `<BlobPrefix><Name Encoded="true">a%EF%BF%BE/</Name></BlobPrefix>`
	start := strings.Index(replayXML, prefix)
	end := strings.Index(replayXML[start:], "</Blob>\n") + start + len("</Blob>")
	swapped := replayXML[:start] + replayXML[start+len(prefix):end] + prefix + replayXML[end:]
	wantMismatch(t, comparePages(nativeXML, swapped), "entry kind")
	wantMismatch(t, comparePages(strings.Replace(nativeXML, "<BlobType>BlockBlob</BlobType>", "<BlobType>Unknown</BlobType>", 1), replayXML), "native BlobType")
}

func walkOf(pages ...capturedPage) []capturedPage { return pages }

func xmlPage(marker, prefix *string, body string) capturedPage {
	return capturedPage{marker: marker, prefix: prefix, exchange: exchange{200, nil, []byte(body)}}
}

// AzureProfileComparatorTest.completedWalkAllowsDifferentPageBoundariesButChecksOwnMarkers.
func TestComparatorWalkBoundaries(t *testing.T) {
	first := regexp.MustCompile(`(?s)<Blob><Name>.*?</Blob>`).ReplaceAllString(nativeXML, "")
	first = strings.Replace(first, "<NextMarker></NextMarker>", "<NextMarker>native-1</NextMarker>", 1)
	second := regexp.MustCompile(`(?s)<BlobPrefix>.*?</BlobPrefix>`).ReplaceAllString(nativeXML, "")
	second = strings.Replace(second, "<Prefix>a/</Prefix>", "<Prefix>a/</Prefix><Marker>native-1</Marker>", 1)
	walk := func(native []capturedPage) error {
		return catch(func() {
			compareParsed(aggregate(native, false, 3, 10), aggregate(walkOf(xmlPage(nil, sp("a/"), replayXML)), true, 3, 10),
				"https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/", "live", "bucket", nil, nil, false)
		})
	}
	if err := walk(walkOf(xmlPage(nil, sp("a/"), first), xmlPage(sp("native-1"), sp("a/"), second))); err != nil {
		t.Fatal(err)
	}
	wantMismatch(t, walk(walkOf(xmlPage(nil, sp("a/"), first), xmlPage(sp("wrong"), sp("a/"), second))), "request marker")
	wantMismatch(t, walk(walkOf(xmlPage(nil, sp("wrong/"), first))), "Prefix echo")
	wrongEcho := strings.Replace(second, "<Marker>native-1</Marker>", "<Marker>wrong</Marker>", 1)
	wantMismatch(t, walk(walkOf(xmlPage(nil, sp("a/"), first), xmlPage(sp("native-1"), sp("a/"), wrongEcho))), "Marker echo")
}

// AzureProfileComparatorTest.crossPageBlobPrefixRepeatIsExplicitlyUnsupported.
func TestComparatorRefusesCrossPagePrefixRepeat(t *testing.T) {
	first := `<?xml version="1.0"?><EnumerationResults ServiceEndpoint="https://live.blob.core.windows.net/" ContainerName="live">` +
		`<Blobs><BlobPrefix><Name>a/</Name></BlobPrefix></Blobs><NextMarker>native-1</NextMarker></EnumerationResults>`
	second := strings.Replace(strings.Replace(first, "<Blobs>", "<Marker>native-1</Marker><Blobs>", 1),
		"<NextMarker>native-1</NextMarker>", "<NextMarker></NextMarker>", 1)
	wantMismatch(t, catch(func() {
		aggregate(walkOf(xmlPage(nil, nil, first), xmlPage(sp("native-1"), nil, second)), false, 3, 10)
	}),
		"cross-page repeated Azure BlobPrefix")
}

// AzureNamespaceMappingTest.nativeHostAndReplayAccountPathAreMappedExplicitly.
func TestNamespaceMapping(t *testing.T) {
	const native, local = "https://{account}.blob.core.windows.net/", "http://127.0.0.1:19090/replay/"
	if err := catch(func() { assertMapped("/{container}", "/replay/replay-bucket", native, local) }); err != nil {
		t.Fatal(err)
	}
	wantMismatch(t, catch(func() { assertMapped("/{account}/{container}", "/replay/replay-bucket", native, local) }), "mapping differs")
	wantMismatch(t, catch(func() {
		assertMapped("/{container}", "/replay/replay-bucket", "http://{account}.blob.core.windows.net/", local)
	}), "native ServiceEndpoint")
	wantMismatch(t, catch(func() { assertMapped("/{container}", "/replay/replay-bucket", native, "http://127.0.0.1:19090/wrong/") }), "replay ServiceEndpoint")
}

// ProviderExchangeComparatorTest.azureVersionClientRequestIdAndErrorCodeAreChecked.
func TestExchangeChecksVersionClientIDAndErrorCode(t *testing.T) {
	nb := []byte(`<?xml version="1.0"?><Error><Code>InvalidHeaderValue</Code><Message>native</Message></Error>`)
	rb := []byte(`<?xml version="1.0"?><Error><Code>InvalidHeaderValue</Code><Message>replay</Message></Error>`)
	h := func(version, code string) map[string][]string {
		return map[string][]string{"content-type": {"application/xml"}, "x-ms-version": {version}, "x-ms-error-code": {code},
			"x-ms-client-request-id": {"probe-1"}, "x-ms-request-id": {"id"}}
	}
	native := exchange{400, h("2026-06-06", "InvalidHeaderValue"), nb}
	run := func(replay exchange) error {
		return catch(func() {
			assertAzure(native, replay, "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/", "live", "bucket",
				sp("2026-06-06"), sp("probe-1"), nil, nil, "GET")
		})
	}
	if err := run(exchange{400, h("2026-06-06", "InvalidHeaderValue"), rb}); err != nil {
		t.Fatal(err)
	}
	wantMismatch(t, run(exchange{400, h("2026-10-06", "InvalidHeaderValue"), rb}), "replay x-ms-version")
	wantMismatch(t, run(exchange{400, h("2026-06-06", "ContainerNotFound"), rb}), "replay x-ms-error-code")
}

// ProviderExchangeComparatorTest.azureWalkChecksVersionAndRawXmlPreamble.
func TestWalkChecksVersionAndPreamble(t *testing.T) {
	nx := `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="https://live.blob.core.windows.net/"` +
		` ContainerName="live"><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`
	rx := strings.Replace(strings.Replace(nx, "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/", 1),
		`ContainerName="live"`, `ContainerName="bucket"`, 1)
	h := map[string][]string{"content-type": {"application/xml"}, "x-ms-version": {"2026-06-06"}}
	run := func(replay string) error {
		return catch(func() {
			assertAzureWalk([]capturedPage{{exchange: exchange{200, h, []byte(nx)}}}, []capturedPage{{exchange: exchange{200, h, []byte(replay)}}},
				"https://live.blob.core.windows.net/", "http://127.0.0.1/replay/", "live", "bucket", sp("2026-06-06"), 2, 10)
		})
	}
	if err := run(rx); err != nil {
		t.Fatal(err)
	}
	wantMismatch(t, run("\ufeff"+rx), "BOM/prolog")
}

// ProviderMatchReceiptRunTest.azureWalkReceiptPreservesClientIdAndRejectsUnexpectedDuplicate.
func TestWalkRequestsCarryClientIDAndRefuseDuplicates(t *testing.T) {
	raw := "sdk-probe-1"
	nativeStep := func(query string) step {
		return step{phase: "walk", request: newRequest("GET", "/{container}", query, [][2]string{{"x-ms-version", "2026-06-06"},
			{"x-ms-client-request-id", opaqueDigest(raw)}}),
			exchange: exchange{200, map[string][]string{"x-ms-client-request-id": {opaqueDigest(raw)}}, nil}}
	}
	replay := func(query string) replayStep {
		return replayStep{newRequest("GET", "/replay/replay-bucket", query, [][2]string{{"x-ms-version", "2026-06-06"},
			{"x-ms-client-request-id", raw}}), exchange{200, map[string][]string{"x-ms-client-request-id": {raw}}, nil}}
	}
	base := "restype=container&comp=list&maxresults=1"
	ok := capture{kind: "walk", probeID: "azure-09", steps: []step{nativeStep(base), nativeStep(base + "&marker=n")}}
	if err := catch(func() {
		assertWalkRequests(ok, []replayStep{replay(base), replay(base + "&marker=l")}, "/replay/replay-bucket")
	}); err != nil {
		t.Fatal(err)
	}
	dup := base + "&prefix=a&prefix=a"
	bad := capture{kind: "walk", probeID: "azure-09", steps: []step{nativeStep(dup), nativeStep(dup + "&marker=n")}}
	wantMismatch(t, catch(func() {
		assertWalkRequests(bad, []replayStep{replay(dup), replay(dup + "&marker=l")}, "/replay/replay-bucket")
	}), "duplicate query field outside duplicate-parameter probe")
	wantMismatch(t, catch(func() {
		r := replay(base)
		r.request = newRequest("GET", "/replay/replay-bucket", base, [][2]string{{"x-ms-version", "2026-06-06"}, {"x-ms-client-request-id", "other"}})
		assertWalkRequests(ok, []replayStep{r}, "/replay/replay-bucket")
	}), "replay request client ID differs")
}

// ProviderEvidenceTest.azureKeepsElementOrderExactNameTextAndEncodedAttribute
// and azureRefusesDoctypeAndUnexpectedEntryKinds.
func TestPageParsing(t *testing.T) {
	p := parsePage([]byte(`<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://account.blob.core.windows.net/" ContainerName="test">
  <Blobs><BlobPrefix><Name Encoded="true">a%2Bb/</Name></BlobPrefix>
    <Blob><Name> leading and trailing </Name><Properties/></Blob></Blobs><NextMarker>opaque</NextMarker>
</EnumerationResults>`), false)
	if len(p.entries) != 2 || p.entries[0].kind != "BlobPrefix" || p.entries[0].name != "a%2Bb/" || show(p.entries[0].encoded) != "true" ||
		p.entries[1].name != " leading and trailing " || p.entries[1].encoded != nil || p.nextMarker != "opaque" ||
		p.endpoint != "https://account.blob.core.windows.net/" || p.container != "test" {
		t.Errorf("%+v", p)
	}
	if catch(func() {
		parsePage([]byte(`<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<EnumerationResults><Blobs><Blob><Name>&xxe;</Name></Blob></Blobs></EnumerationResults>`), false)
	}) == nil {
		t.Error("DOCTYPE accepted")
	}
	wantMismatch(t, catch(func() {
		parsePage([]byte(`<EnumerationResults ServiceEndpoint="https://a.blob.core.windows.net/" ContainerName="c"><Blobs><Deleted><Name>x</Name></Deleted></Blobs><NextMarker></NextMarker></EnumerationResults>`), false)
	}), "unknown Azure entry element")
}
