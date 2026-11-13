// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	amem "github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/varveio/swath/internal/source/objhttp"
)

// listWith lists one page with in's include from a server answering body
// with Content-Type ct, and reports the query it was sent.
func listWith(t *testing.T, ct string, body []byte, include Include) (*ListOutput, string, error) {
	t.Helper()
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("include")
		w.Header().Set("Content-Type", ct)
		w.Write(body)
	}))
	defer srv.Close()
	cl, err := newClient(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Anonymous: true,
		Retry: objhttp.Retry{MaxAttempts: 1}, RequestTimeout: time.Minute, HTTPClient: &http.Client{}}, build{bodyCap: MaxBody, entra: defaultChain})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cl.List(context.Background(), &ListInput{Container: "c", Include: include})
	return out, query, err
}

// TestDeletedEquivalence: a page recorded live with include=deleted (NOAA
// GFS, anonymous, 2026-11-11: two soft-deleted blobs, then two live ones,
// in name order) converts to the same items from Arrow and XML, deleted
// time and remaining retention on the deleted ones only, and the
// properties every listing carries on each.
func TestDeletedEquivalence(t *testing.T) {
	ab, err := os.ReadFile("testdata/deleted.arrow")
	if err != nil {
		t.Fatal(err)
	}
	xb, err := os.ReadFile("testdata/deleted.xml")
	if err != nil {
		t.Fatal(err)
	}
	a, q, err := listWith(t, arrowType, ab, Include{Deleted: true})
	if err != nil {
		t.Fatal(err)
	}
	if q != "deleted" {
		t.Errorf("include=%q, want deleted", q)
	}
	x, _, err := listWith(t, "application/xml", xb, Include{Deleted: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, x) {
		t.Fatalf("Arrow %+v\nXML   %+v", a, x)
	}
	if len(a.Items) != 4 {
		t.Fatalf("%d items, want 4", len(a.Items))
	}
	deletedAt := time.Date(2026, 11, 9, 3, 18, 41, 0, time.UTC).UnixMicro()
	for i, it := range a.Items {
		deleted := i < 2
		if it.Deleted != deleted || (it.DeletedTime == deletedAt) != deleted || it.HasRetention != deleted ||
			deleted && it.RetentionDays != 4 {
			t.Errorf("item %d %q: entry %+v, want deleted %v", i, it.Name, it.Entry, deleted)
		}
		if it.AccessTierInferred != True || it.ServerEncrypted != True || it.LastAccessTime == 0 {
			t.Errorf("item %d %q: properties %+v", i, it.Name, it.Props)
		}
		if lease := map[bool]string{true: "", false: "available"}[deleted]; it.LeaseState != lease {
			t.Errorf("item %d %q: lease state %q, want %q", i, it.Name, it.LeaseState, lease)
		}
	}
}

// TestZeroSDKTimesAbsent: the SDK's Arrow decoder returns a non-nil zero
// time for timestamp[s] -62135596800. Its XML decoder drops zero times,
// including empty elements. Both must leave the timestamp fields absent,
// while retaining ordinary times, including those before the Unix epoch.
func TestZeroSDKTimesAbsent(t *testing.T) {
	str := arrow.BinaryTypes.String
	cols := []column{
		{"Name", str, `["absent", "before", "dated", "zero"]`},
		{"ResourceType", str, `["blob", "blob", "blob", "blob"]`},
	}
	var before, dated, empty strings.Builder
	for _, name := range []string{"Last-Modified", "Creation-Time", "DeletedTime", "AccessTierChangeTime", "LastAccessTime", "ImmutabilityPolicyUntilDate"} {
		cols = append(cols, column{name, &arrow.TimestampType{Unit: arrow.Second}, `[null, -1, 1767225600, -62135596800]`})
		before.WriteString("<" + name + ">Wed, 31 Dec 1969 23:59:59 GMT</" + name + ">")
		dated.WriteString("<" + name + ">Thu, 01 Jan 2026 00:00:00 GMT</" + name + ">")
		empty.WriteString("<" + name + "/>")
	}
	mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ab := columnsPage(t, mem, cols)
	xb := []byte(listHead + `<Blob><Name>absent</Name><Properties/></Blob>` +
		`<Blob><Name>before</Name><Properties>` + before.String() + `</Properties></Blob>` +
		`<Blob><Name>dated</Name><Properties>` + dated.String() + `</Properties></Blob>` +
		`<Blob><Name>zero</Name><Properties>` + empty.String() + `</Properties></Blob>` + listTail)
	want := []Item{{Name: "absent"}, {Name: "before", LastModified: -1000000, CreationTime: -1000000,
		Entry: Entry{DeletedTime: -1000000}, Props: Props{AccessTierChangeTime: -1000000, LastAccessTime: -1000000, ImmutabilityUntil: -1000000}},
		{Name: "dated", LastModified: 1767225600000000, CreationTime: 1767225600000000,
			Entry: Entry{DeletedTime: 1767225600000000}, Props: Props{AccessTierChangeTime: 1767225600000000, LastAccessTime: 1767225600000000, ImmutabilityUntil: 1767225600000000}},
		{Name: "zero"}}
	for _, f := range []struct {
		name, ct string
		body     []byte
	}{{"Arrow", arrowType, ab}, {"XML", "application/xml", xb}} {
		t.Run(f.name, func(t *testing.T) {
			got, _, err := listWith(t, f.ct, f.body, Include{Deleted: true, Immutability: true})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Items, want) {
				t.Fatalf("items %+v, want %+v", got.Items, want)
			}
		})
	}
}

// TestIncludeRequest: each include is sent as the SDK spells it, and none
// without one (the request every other listing sends).
func TestIncludeRequest(t *testing.T) {
	body := []byte("\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs /><NextMarker /></EnumerationResults>")
	for _, c := range []struct {
		in   Include
		want string
	}{
		{Include{}, ""},
		{Include{Versions: true, Snapshots: true, Deleted: true}, "deleted,snapshots,versions"},
		{Include{Metadata: true}, "metadata"},
		{Include{Tags: true}, "tags"},
		{Include{Immutability: true}, "immutabilitypolicy,legalhold"},
	} {
		_, q, err := listWith(t, "application/xml", body, c.in)
		if err != nil {
			t.Fatal(err)
		}
		if q != c.want {
			t.Errorf("%+v: include=%q, want %q", c.in, q, c.want)
		}
	}
}

// TestGroupedPage: a page with versions, snapshots, a soft-deleted entry,
// metadata, tags and immutability converts to the same items from Arrow
// and XML, a name's entries in wire order; a repeated name is refused
// unless the request was grouped.
func TestGroupedPage(t *testing.T) {
	str := arrow.BinaryTypes.String
	ts := &arrow.TimestampType{Unit: arrow.Second}
	strMap := arrow.MapOf(str, str)
	mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ab := columnsPage(t, mem, []column{
		{"Name", str, `["a", "a", "a", "b"]`},
		{"ResourceType", str, `["blob", "blob", "blob", "blob"]`},
		{"Snapshot", str, `["2026-01-01T00:00:00.0000000Z", null, null, null]`},
		{"VersionId", str, `[null, "2026-01-02T00:00:00.0000000Z", "2026-01-03T00:00:00.0000000Z", null]`},
		{"IsCurrentVersion", arrow.FixedWidthTypes.Boolean, `[null, null, true, null]`},
		{"Deleted", arrow.FixedWidthTypes.Boolean, `[null, null, null, true]`},
		{"DeletedTime", ts, `[null, null, null, 1767225600]`},
		{"RemainingRetentionDays", arrow.PrimitiveTypes.Uint64, `[null, null, null, 0]`},
		{"Content-Length", arrow.PrimitiveTypes.Uint64, `[1, 2, 3, 4]`},
		{"AccessTierChangeTime", ts, `[null, null, 1767225600, null]`},
		{"ArchiveStatus", str, `[null, null, "rehydrate-pending-to-hot", null]`},
		{"RehydratePriority", str, `[null, null, "High", null]`},
		{"Content-Encoding", str, `[null, null, "gzip", null]`},
		{"EncryptionScope", str, `[null, null, "scope1", null]`},
		{"CustomerProvidedKeySha256", str, `[null, "abc=", null, null]`},
		{"LeaseState", str, `[null, null, "leased", null]`},
		{"TagCount", arrow.PrimitiveTypes.Uint64, `[null, null, 2, null]`},
		{"Metadata", strMap, `[null, null, [{"key": "b", "value": "2"}, {"key": "a", "value": "1"}], null]`},
		{"Tags", strMap, `[null, null, [{"key": "project", "value": "x"}, {"key": "cost", "value": "y"}], null]`},
		{"ImmutabilityPolicyUntilDate", ts, `[null, null, 1767225600, null]`},
		{"ImmutabilityPolicyMode", str, `[null, null, "Locked", null]`},
		{"LegalHold", arrow.FixedWidthTypes.Boolean, `[null, null, true, null]`},
	})
	blob := func(name, entry, props string) string {
		return "<Blob><Name>" + name + "</Name>" + entry + "<Properties>" + props + "</Properties></Blob>"
	}
	const day = "Thu, 01 Jan 2026 00:00:00 GMT"
	xb := []byte("\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs>" +
		blob("a", "<Snapshot>2026-01-01T00:00:00.0000000Z</Snapshot>", "<Content-Length>1</Content-Length>") +
		blob("a", "<VersionId>2026-01-02T00:00:00.0000000Z</VersionId>", "<Content-Length>2</Content-Length><CustomerProvidedKeySha256>abc=</CustomerProvidedKeySha256>") +
		strings.TrimSuffix(blob("a", "<VersionId>2026-01-03T00:00:00.0000000Z</VersionId><IsCurrentVersion>true</IsCurrentVersion>",
			"<Content-Length>3</Content-Length><Content-Encoding>gzip</Content-Encoding><AccessTierChangeTime>"+day+"</AccessTierChangeTime>"+
				"<ArchiveStatus>rehydrate-pending-to-hot</ArchiveStatus><RehydratePriority>High</RehydratePriority>"+
				"<EncryptionScope>scope1</EncryptionScope><LeaseState>leased</LeaseState><TagCount>2</TagCount>"+
				"<ImmutabilityPolicyUntilDate>"+day+"</ImmutabilityPolicyUntilDate><ImmutabilityPolicyMode>Locked</ImmutabilityPolicyMode>"+
				"<LegalHold>true</LegalHold>"), "</Blob>") +
		"<Metadata><b>2</b><a>1</a></Metadata><Tags><TagSet><Tag><Key>project</Key><Value>x</Value></Tag><Tag><Key>cost</Key><Value>y</Value></Tag></TagSet></Tags></Blob>" +
		blob("b", "<Deleted>true</Deleted>", "<Content-Length>4</Content-Length><DeletedTime>"+day+"</DeletedTime><RemainingRetentionDays>0</RemainingRetentionDays>") +
		"</Blobs><NextMarker /></EnumerationResults>")
	all := Include{Versions: true, Snapshots: true, Deleted: true, Metadata: true, Tags: true, Immutability: true}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	want := &ListOutput{Items: []Item{
		{Name: "a", Size: 1, Entry: Entry{Snapshot: "2026-01-01T00:00:00.0000000Z"}},
		{Name: "a", Size: 2, Entry: Entry{VersionID: "2026-01-02T00:00:00.0000000Z"}, Props: Props{CustomerProvidedKeySHA256: "abc="}},
		{Name: "a", Size: 3, Entry: Entry{VersionID: "2026-01-03T00:00:00.0000000Z", IsCurrentVersion: true},
			Props: Props{AccessTierChangeTime: at, ArchiveStatus: "rehydrate-pending-to-hot", RehydratePriority: "High", ContentEncoding: "gzip",
				EncryptionScope: "scope1", LeaseState: "leased", TagCount: 2, Metadata: `{"a":"1","b":"2"}`, Tags: `{"cost":"y","project":"x"}`,
				ImmutabilityUntil: at, ImmutabilityMode: "Locked", LegalHold: True}},
		{Name: "b", Size: 4, Entry: Entry{Deleted: true, DeletedTime: at, HasRetention: true}},
	}}
	for _, f := range []struct {
		name, ct string
		body     []byte
	}{{"Arrow", arrowType, ab}, {"XML", "application/xml", xb}} {
		got, _, err := listWith(t, f.ct, f.body, all)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %+v, %v\nwant %+v", f.name, got, err, want)
		}
		if _, _, err := listWith(t, f.ct, f.body, Include{Metadata: true}); !strings.Contains(err.Error(), `"a" listed after "a"`) {
			t.Errorf("%s: a repeated name in an ungrouped listing: %v", f.name, err)
		}
	}
}

// TestIncludeColumnTypes: a column behind an entry or property swath
// writes, in a type the SDK's decoder reads as nulls, refuses the page as
// any column behind a written value does.
func TestIncludeColumnTypes(t *testing.T) {
	str := arrow.BinaryTypes.String
	for _, c := range []column{
		{"Deleted", str, `["true"]`},
		{"RemainingRetentionDays", arrow.PrimitiveTypes.Int64, `[3]`},
		{"Metadata", arrow.ListOf(str), `[["a"]]`},
		{"VersionId", arrow.PrimitiveTypes.Int64, `[1]`},
	} {
		mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
		body := columnsPage(t, mem, []column{{"Name", str, `["a"]`}, {"ResourceType", str, `["blob"]`}, c})
		if _, _, err := listWith(t, arrowType, body, Include{Deleted: true, Metadata: true}); err == nil || !strings.Contains(err.Error(), c.name) {
			t.Errorf("column %s %s: %v", c.name, c.typ, err)
		}
		mem.AssertSize(t, 0)
	}
}
