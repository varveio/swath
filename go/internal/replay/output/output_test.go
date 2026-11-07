// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package output

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// The tests below port the replay wire contract's own sink tests
// (BudgetedOutputTest), one per case, under the same names.

func mustNew(t *testing.T, b *Budget, initial, cap, chunk int) *Output {
	t.Helper()
	o, err := NewChunked(b, initial, cap, chunk, nil)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func flat(views [][]byte) []byte { return bytes.Join(views, nil) }

func sizes(views [][]byte) []int {
	var out []int
	for _, v := range views {
		out = append(out, len(v))
	}
	return out
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), what) {
			t.Errorf("panic %v, want %q", r, what)
		}
	}()
	f()
}

func TestProductionChunkSizeIsFixed(t *testing.T) {
	if ChunkBytes != 256<<10 {
		t.Errorf("ChunkBytes %d", ChunkBytes)
	}
	o := mustNew(t, NewBudget(4096, nil), 1, 4096, 7)
	o.Write(make([]byte, 8))
	if got := sizes(o.Body()); !slices.Equal(got, []int{7, 1}) || o.Capacity() != 14 {
		t.Errorf("chunks %v capacity %d", got, o.Capacity())
	}
	o.Close()
}

func TestInitialChunkReasonNamesOnlyActualCapClamping(t *testing.T) {
	var reasons []Reason
	b := NewBudget(128<<10, func(r Reason, _ int) { reasons = append(reasons, r) })
	sparse := mustNew(t, b, 4096, 64<<10, 256<<10)
	sparse.SetFirstChunkHint(4096)
	sparse.Byte(1)
	if sparse.Capacity() != 4096 {
		t.Errorf("sparse capacity %d", sparse.Capacity())
	}
	sparse.Close()
	if !slices.Equal(reasons, []Reason{Initial}) {
		t.Errorf("reasons %v", reasons)
	}
	reasons = nil
	capped := mustNew(t, b, 4096, 5000, 256<<10)
	capped.SetFirstChunkHint(8192)
	capped.Byte(1)
	if capped.Capacity() != 5000 {
		t.Errorf("capped capacity %d", capped.Capacity())
	}
	capped.Close()
	if !slices.Equal(reasons, []Reason{InitialCapPartial}) || b.Charged() != 0 {
		t.Errorf("reasons %v charged %d", reasons, b.Charged())
	}
}

func TestConcurrentChunkGrowthNeverOverchargesAndRecoversAfterRelease(t *testing.T) {
	b := NewBudget(1536, nil)
	outputs := make([]*Output, 8)
	for i := range outputs {
		outputs[i] = mustNew(t, b, 128, 256, 128)
	}
	if b.Charged() != 1024 {
		t.Fatalf("charged %d", b.Charged())
	}
	var succeeded, refused atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, o := range outputs {
		wg.Go(func() {
			<-start
			o.Write(make([]byte, 129))
			if o.Err() == nil {
				succeeded.Add(1)
			} else if errors.Is(o.Err(), ErrBudget) {
				refused.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if succeeded.Load() != 4 || refused.Load() != 4 || b.Charged() != 1536 || b.Peak() != 1536 {
		t.Errorf("succeeded %d refused %d charged %d peak %d", succeeded.Load(), refused.Load(), b.Charged(), b.Peak())
	}
	for _, o := range outputs {
		o.Close()
	}
	if b.Charged() != 0 {
		t.Errorf("charged %d after close", b.Charged())
	}
}

func TestReservesBeforePagingThenReturnsUnusedCreditAfterSparseOrEmptyRender(t *testing.T) {
	b := NewBudget(1<<20, nil)
	sparse := mustNew(t, b, 320_512, 64<<20, ChunkBytes)
	if b.Charged() != 320_512 || sparse.Capacity() != 0 {
		t.Fatalf("charged %d capacity %d", b.Charged(), sparse.Capacity())
	}
	sparse.SetFirstChunkHint(4096)
	sparse.Write(make([]byte, 100))
	if sparse.Capacity() != 4096 || b.Charged() != 320_512 {
		t.Errorf("capacity %d charged %d", sparse.Capacity(), b.Charged())
	}
	if n := len(flat(sparse.Body())); n != 100 || b.Charged() != 4096 {
		t.Errorf("body %d charged %d", n, b.Charged())
	}
	mustPanic(t, "frozen", func() { sparse.Byte(1) })
	sparse.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
	empty := mustNew(t, b, 320_512, 64<<20, ChunkBytes)
	if empty.Capacity() != 0 || len(flat(empty.Body())) != 0 || b.Charged() != 0 {
		t.Errorf("empty: capacity %d charged %d", empty.Capacity(), b.Charged())
	}
	empty.Close()
}

func TestChunksConsumeCreditThenRefuseAggregateGrowthWithoutLeaking(t *testing.T) {
	b := NewBudget(12, nil)
	first := mustNew(t, b, 4, 12, 4)
	first.Write(make([]byte, 8))
	if first.Capacity() != 8 || b.Charged() != 8 {
		t.Fatalf("capacity %d charged %d", first.Capacity(), b.Charged())
	}
	second := mustNew(t, b, 4, 12, 4)
	first.Byte(1)
	if first.Err() != ErrBudget || first.Len() != 8 || first.Capacity() != 8 || b.Charged() != 12 {
		t.Errorf("err %v len %d capacity %d charged %d", first.Err(), first.Len(), first.Capacity(), b.Charged())
	}
	first.Close()
	first.Close()
	second.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
	if _, err := NewChunked(NewBudget(3, nil), 4, 12, 4, nil); err != ErrBudget {
		t.Errorf("reservation beyond the budget: %v", err)
	}
}

func TestFinalPartialChunkHonorsNonAlignedCapacityCapWithoutPadding(t *testing.T) {
	b := NewBudget(32, nil)
	o := mustNew(t, b, 4, 10, 4)
	want := []byte("0123456789")
	o.Write(want)
	body := o.Body()
	if !slices.Equal(sizes(body), []int{4, 4, 2}) || !bytes.Equal(flat(body), want) || o.Capacity() != 10 || b.Charged() != 10 {
		t.Errorf("views %v capacity %d charged %d", sizes(body), o.Capacity(), b.Charged())
	}
	mustPanic(t, "frozen", func() { o.Byte(1) })
	o.Close()
	capped := mustNew(t, b, 4, 10, 4)
	capped.Write(make([]byte, 11))
	if capped.Err() != ErrTooLarge || capped.Capacity() != 0 {
		t.Errorf("err %v capacity %d", capped.Err(), capped.Capacity())
	}
	capped.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
}

func TestEntityUTF8AndPercentEscapesCrossChunkEdgesByteExactly(t *testing.T) {
	b := NewBudget(64, nil)
	xml := mustNew(t, b, 4, 32, 4)
	xml.ASCII("abc")
	xml.Escaped("é&d")
	if got := string(flat(xml.Body())); got != "abcé&amp;d" || len(xml.Body()) < 2 {
		t.Errorf("%q in %d views", got, len(xml.Body()))
	}
	xml.Close()
	safe := []byte("abc/XYZ-_.09")
	o := mustNew(t, NewBudget(128, nil), 1, len(safe), 4)
	o.PercentEncoded(safe)
	if !bytes.Equal(flat(o.Body()), safe) {
		t.Errorf("%q", flat(o.Body()))
	}
	rng := rand.New(rand.NewPCG(220, 0))
	for n := 1; n <= 128; n++ {
		input := make([]byte, n)
		want := 0
		for i := range input {
			if rng.IntN(2) == 0 {
				input[i], want = 'a', want+1
			} else {
				input[i], want = '~', want+3
			}
		}
		o := mustNew(t, NewBudget(int64(4*want), nil), 1, want, 7)
		o.PercentEncoded(input)
		got := flat(o.Body())
		decoded, err := url.PathUnescape(string(got))
		if len(got) != want || err != nil || decoded != string(input) {
			t.Fatalf("n=%d: %q", n, got)
		}
	}
}

func TestPercentEncodingReusesChargedChunkWithoutAllocatingAndRejectsFrozenOrClosedOutput(t *testing.T) {
	b := NewBudget(32, nil)
	allocations := 0
	o, err := NewChunked(b, 8, 32, 32, func(n int) []byte { allocations++; return make([]byte, 0, n) })
	if err != nil {
		t.Fatal(err)
	}
	o.ASCII("prefix")
	if allocations != 1 || b.Charged() != 32 {
		t.Fatalf("allocations %d charged %d", allocations, b.Charged())
	}
	o.PercentEncoded([]byte{'a', ' ', 0xe9})
	onePass, fallback := o.PercentPaths()
	if allocations != 1 || o.Capacity() != 32 || onePass != 1 || fallback != 0 {
		t.Errorf("allocations %d capacity %d paths %d/%d", allocations, o.Capacity(), onePass, fallback)
	}
	if got := string(flat(o.Body())); got != "prefixa+%E9" {
		t.Errorf("%q", got)
	}
	mustPanic(t, "frozen", func() { o.PercentEncoded(nil) })
	o.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
	mustPanic(t, "closed", func() { o.PercentEncoded(nil) })
}

// percentOracle is live S3's encoding-type=url table (probed 2026-11-07):
// a space is '+', A-Z a-z 0-9 - _ . / * stay, every other byte is %XX.
func percentOracle(v []byte) string {
	var s strings.Builder
	for _, c := range v {
		if c == ' ' {
			s.WriteByte('+')
		} else if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte("-_./*", c) >= 0 {
			s.WriteByte(c)
		} else {
			s.WriteString("%" + strings.ToUpper(string("0123456789abcdef"[c>>4])+string("0123456789abcdef"[c&15])))
		}
	}
	return s.String()
}

func TestPrefixedFastAndExactFallbackMatchIndependentPercentOracleForAllBytesAndRandomMix(t *testing.T) {
	every := make([]byte, 256)
	for i := range every {
		every[i] = byte(i)
	}
	mixed := make([]byte, 512)
	rng := rand.New(rand.NewPCG(220, 0))
	for i := range mixed {
		mixed[i] = byte(rng.IntN(256))
	}
	for _, v := range [][]byte{every, mixed} {
		encoded := percentOracle(v)
		fastPrefix := strings.Repeat("p", 50)
		fast := mustNew(t, NewBudget(4096, nil), 64, 4096, 4096)
		fast.ASCII(fastPrefix)
		fast.PercentEncoded(v)
		body := fast.Body()
		onePass, fallback := fast.PercentPaths()
		if string(flat(body)) != fastPrefix+encoded || !slices.Equal(sizes(body), []int{len(fastPrefix) + len(encoded)}) || onePass != 1 || fallback != 0 {
			t.Errorf("fast: views %v paths %d/%d", sizes(body), onePass, fallback)
		}
		exactPrefix := strings.Repeat("p", 4096-len(encoded))
		slow := mustNew(t, NewBudget(4096, nil), 64, 4096, 4096)
		slow.ASCII(exactPrefix)
		slow.PercentEncoded(v)
		body = slow.Body()
		onePass, fallback = slow.PercentPaths()
		if string(flat(body)) != exactPrefix+encoded || !slices.Equal(sizes(body), []int{4096}) || onePass != 0 || fallback != 1 {
			t.Errorf("fallback: views %v paths %d/%d", sizes(body), onePass, fallback)
		}
	}
}

func TestPercentFastPathEngagesAtExactlyOneThirdRoomAndFallsBackOneByteBeyond(t *testing.T) {
	b := NewBudget(32, nil)
	exact := mustNew(t, b, 8, 32, 32)
	exact.ASCII(strings.Repeat("p", 23))
	exact.PercentEncoded([]byte{0, 0x7f, 0xff})
	onePass, _ := exact.PercentPaths()
	if string(flat(exact.Body())) != strings.Repeat("p", 23)+"%00%7F%FF" || onePass != 1 || exact.Capacity() != 32 {
		t.Errorf("exact boundary: %q %d", flat(exact.Body()), onePass)
	}
	exact.Close()
	beyond := mustNew(t, b, 8, 32, 32)
	beyond.ASCII(strings.Repeat("p", 23))
	beyond.PercentEncoded([]byte("abcd"))
	onePass, fallback := beyond.PercentPaths()
	if string(flat(beyond.Body())) != strings.Repeat("p", 23)+"abcd" || onePass != 0 || fallback != 1 || beyond.Capacity() != 32 {
		t.Errorf("one beyond: %q %d/%d", flat(beyond.Body()), onePass, fallback)
	}
	beyond.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
}

func TestConservativePercentFastPathFallsBackToExactCapAndBudgetDecisions(t *testing.T) {
	b := NewBudget(14, nil)
	fit := mustNew(t, b, 5, 10, 10)
	fit.ASCII("xxxxx")
	fit.PercentEncoded([]byte("aaaa")) // fits, though the 3x bound does not
	onePass, fallback := fit.PercentPaths()
	if onePass != 0 || fallback != 1 || string(flat(fit.Body())) != "xxxxxaaaa" {
		t.Errorf("exact fit: %d/%d %q", onePass, fallback, flat(fit.Body()))
	}
	fit.Close()
	capped := mustNew(t, b, 5, 10, 10)
	capped.ASCII("xxxxxxx")
	capped.PercentEncoded([]byte("a~b"))
	_, fallback = capped.PercentPaths()
	if capped.Err() != ErrTooLarge || capped.Len() != 7 || capped.Capacity() != 10 || fallback != 0 {
		t.Errorf("capped: %v len %d capacity %d fallback %d", capped.Err(), capped.Len(), capped.Capacity(), fallback)
	}
	capped.Close()
	split := mustNew(t, b, 7, 14, 7)
	split.ASCII("xxxxx")
	split.PercentEncoded([]byte{'~'})
	_, fallback = split.PercentPaths()
	if fallback != 1 || string(flat(split.Body())) != "xxxxx%7E" || !slices.Equal(sizes(split.Body()), []int{7, 1}) {
		t.Errorf("split: %d %q %v", fallback, flat(split.Body()), sizes(split.Body()))
	}
	split.Close()
	if b.Charged() != 0 {
		t.Errorf("charged %d", b.Charged())
	}
	exhausted := NewBudget(6, nil)
	o := mustNew(t, exhausted, 4, 8, 4)
	o.ASCII("xxxx")
	o.PercentEncoded([]byte{'~'})
	_, fallback = o.PercentPaths()
	if o.Err() != ErrBudget || o.Len() != 4 || o.Capacity() != 4 || exhausted.Charged() != 4 || fallback != 0 {
		t.Errorf("exhausted: %v len %d capacity %d charged %d", o.Err(), o.Len(), o.Capacity(), exhausted.Charged())
	}
	o.Close()
	if exhausted.Charged() != 0 {
		t.Errorf("charged %d", exhausted.Charged())
	}
}

// TestBoundedGrowthPreservesEveryByte writes more than six hundred KiB in
// pieces of every size across chunk edges, as S3XmlTest's
// boundedBufferGrowthPreservesEveryResponseByte does, and checks the bytes
// and that the capacity never ran more than one chunk ahead of the length.
func TestBoundedGrowthPreservesEveryByte(t *testing.T) {
	b := NewBudget(1<<30, nil)
	o := mustNew(t, b, 4096, 1<<30, ChunkBytes)
	var want bytes.Buffer
	rng := rand.New(rand.NewPCG(7, 7))
	for want.Len() < 600<<10 {
		piece := make([]byte, rng.IntN(5000))
		for i := range piece {
			piece[i] = byte(rng.IntN(256))
		}
		switch rng.IntN(3) {
		case 0:
			o.Write(piece)
			want.Write(piece)
		case 1:
			o.PercentEncoded(piece)
			want.WriteString(percentOracle(piece))
		default:
			o.Escaped(string(piece))
			for _, c := range piece {
				switch c {
				case '&':
					want.WriteString("&amp;")
				case '<':
					want.WriteString("&lt;")
				case '>':
					want.WriteString("&gt;")
				case '"':
					want.WriteString("&quot;")
				case '\'':
					want.WriteString("&apos;")
				default:
					want.WriteByte(c)
				}
			}
		}
		if o.Capacity()-o.Len() > ChunkBytes {
			t.Fatalf("capacity %d ran ahead of length %d", o.Capacity(), o.Len())
		}
	}
	if got := flat(o.Body()); !bytes.Equal(got, want.Bytes()) || int64(o.Capacity()) != b.Charged() {
		t.Fatalf("%d bytes, want %d; charged %d capacity %d", len(got), want.Len(), b.Charged(), o.Capacity())
	}
	o.Close()
}
