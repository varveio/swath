// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// versioned is a fake versions listing (S3 ListObjectVersions-shaped):
// rows sorted by key, a key's versions consecutive in wire order; a
// physical seek lists the keys after After (or from After, inclusive),
// a continuation resumes at its entry. Each row's Size is its index in
// rows, so a test sees within-key order and duplicates.
type versioned struct {
	rows                 []Row
	short                bool // pages of 1..Max entries
	empties              bool // some continuing pages hold nothing
	inclusive            bool // the start bound lists After's own rows again
	latency, slowLatency time.Duration
	slowPrefix           string

	mu    sync.Mutex
	rng   *rand.Rand
	nonce int
}

func newVersioned(keys []string, versions func(i int) int, seed uint64) *versioned {
	keys = slices.Compact(slices.Sorted(slices.Values(keys)))
	v := &versioned{rng: rand.New(rand.NewPCG(seed, 1))}
	for i, k := range keys {
		for range versions(i) {
			v.rows = append(v.rows, Row{Key: k, Data: Object{Size: int64(len(v.rows))}})
		}
	}
	return v
}

// want is every row under prefix above after.
func (v *versioned) want(prefix, after string) []Row {
	var out []Row
	for _, r := range v.rows {
		if strings.HasPrefix(r.Key, prefix) && r.Key > after {
			out = append(out, r)
		}
	}
	return out
}

func (v *versioned) fetch(ctx context.Context, q *Query) (*Page[Object], error) {
	delay := v.latency
	if v.slowPrefix != "" && (strings.HasPrefix(q.Prefix, v.slowPrefix) || strings.HasPrefix(q.After, v.slowPrefix)) {
		delay = v.slowLatency
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	var i int
	if q.Token != "" {
		n, _, _ := strings.Cut(q.Token, "#")
		i, _ = strconv.Atoi(n)
	} else {
		start := q.After
		i = sort.Search(len(v.rows), func(j int) bool {
			k := v.rows[j].Key
			if v.inclusive {
				return k >= start
			}
			return k > start
		})
		if j := sort.Search(len(v.rows), func(j int) bool { return v.rows[j].Key >= q.Prefix }); j > i {
			i = j
		}
	}
	under := func(j int) bool { return j < len(v.rows) && strings.HasPrefix(v.rows[j].Key, q.Prefix) }
	pg := &Page[Object]{}
	if v.empties && q.Token != "" && v.rng.IntN(5) == 0 && under(i) {
		v.nonce++
		pg.Truncated, pg.Token = true, fmt.Sprintf("%d#%d", i, v.nonce)
		return pg, nil
	}
	max := q.Max
	if v.short && max > 1 {
		max = 1 + v.rng.IntN(max)
	}
	for n := 0; n < max && under(i); n++ {
		k := v.rows[i].Key
		if q.Delimiter != "" {
			if d := strings.Index(k[len(q.Prefix):], q.Delimiter); d >= 0 {
				cp := k[:len(q.Prefix)+d+len(q.Delimiter)]
				pg.Prefixes = append(pg.Prefixes, cp)
				for under(i) && strings.HasPrefix(v.rows[i].Key, cp) {
					i++
				}
				continue
			}
		}
		pg.Rows = append(pg.Rows, v.rows[i])
		i++
	}
	if under(i) {
		pg.Truncated, pg.Token = true, strconv.Itoa(i)
	}
	return pg, nil
}

// groupSchema is the test schema in group mode.
var groupSchema = func() schema.Schema[Object] {
	s := Schema
	s.Info.ID, s.Info.Groups = "swath/keyrange-groups-test/v1", true
	return s
}()

func newGroupEngine(t *testing.T, v *versioned, opt Options) *Engine[Object] {
	t.Helper()
	p, err := New(opt, Description[Object]{Target: "test", Listing: Listing, Groups: true, StartInclusive: v.inclusive,
		Detach: groupSchema.Detach, Size: func(o *Object) int64 { return o.Size }}, v.fetch)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// runGroups lists v through the order stage and through Count, and checks
// both against every row under the scope after the resume key, in order.
func runGroups(t *testing.T, v *versioned, opt Options) Stats {
	t.Helper()
	after := ""
	var oa *string
	if opt.Resume != nil {
		after = opt.Resume.After
		oa = &after
	}
	want := v.want(opt.Prefix, after)
	p := newGroupEngine(t, v, opt)
	c := &collect{}
	st, err := order.Run[Range, Object](context.Background(), p, c, &groupSchema,
		order.Options{Workers: max(1, opt.Workers), Budget: 1 << 22, Groups: true, After: oa})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.rows, want) {
		t.Fatalf("listed %d rows, want %d; first difference at %d", len(c.rows), len(want), firstDiff(c.rows, want))
	}
	names := int64(len(distinct(want)))
	if st.DistinctKeys != names {
		t.Fatalf("order: %d distinct keys, want %d", st.DistinctKeys, names)
	}
	cs, err := order.Count[Range, Object](context.Background(), newGroupEngine(t, v, opt), nil,
		order.Options{Workers: max(1, opt.Workers), Groups: true, After: oa})
	if err != nil {
		t.Fatal(err)
	}
	if cs.Rows != int64(len(want)) || cs.DistinctKeys != names {
		t.Fatalf("count: %d rows, %d keys; want %d, %d", cs.Rows, cs.DistinctKeys, len(want), names)
	}
	return p.Stats()
}

func firstDiff(a, b []Row) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// groupKeyspace is keys with versions: most keys one to four, every 97th
// a few pages' worth, and hot a hot key of 50 pages.
func groupKeyspace(n, page int, hot int) func(i int) int {
	return func(i int) int {
		switch {
		case i == hot:
			return 50 * page
		case i%97 == 13:
			return 2*page + i%5
		default:
			return 1 + i%4
		}
	}
}

func groupKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("d%02d/s%02d/k%05d", i/400, i/40%10, i)
	}
	return keys
}

// TestGroupsEveryRowOnceInOrder lists versioned keyspaces at small page
// sizes through every path (sequential, forced splits, the seed and
// structure probes, warm starts, latency-ranked splits, demand splits, resume,
// an inclusive start) and checks every row once, in order, with the group
// counters engaged.
func TestGroupsEveryRowOnceInOrder(t *testing.T) {
	type cfg struct {
		name string
		opt  Options
	}
	coin := func(seed uint64) func() bool {
		var mu sync.Mutex
		rng := rand.New(rand.NewPCG(seed, 2))
		return func() bool { mu.Lock(); defer mu.Unlock(); return rng.IntN(2) == 0 }
	}
	keys := groupKeys(1200)
	prior := &source.Prior{Source: "cuts", Unscoped: true, Counted: true}
	// Include the hot key itself to exercise an exact group boundary.
	for i := 60; i < len(keys); i += 60 {
		prior.Cuts = append(prior.Cuts, source.Cut{Key: keys[i]})
	}
	for _, page := range []int{1, 2, 3, 7, 50} {
		// Cuts carry row mass, as a versioned --seed-from dataset does.
		mass, cut := int64(0), 0
		versions := groupKeyspace(len(keys), page, 300)
		for i, key := range keys {
			mass += int64(versions(i))
			if cut < len(prior.Cuts) && key == prior.Cuts[cut].Key {
				prior.Cuts[cut].Rows = mass
				mass, cut = 0, cut+1
			}
		}
		for _, short := range []bool{false, true} {
			for seed := range uint64(3) {
				cfgs := []cfg{
					{"sequential", Options{Sequential: true}},
					{"forced", Options{Workers: 8, SplitCoin: coin(seed)}},
					{"seed+structure", Options{Workers: 16, Seed: true, SeedFirstPage: true, Structure: StructureOn}},
					{"demand", Options{Workers: 16}},
					{"warm-start", Options{Workers: 16, Prior: prior}},
					{"slow-split", Options{Workers: 16, Seed: true, SeedFirstPage: true, SlowSplit: true}},
					{"resume", Options{Workers: 8, SplitCoin: coin(seed), Resume: &Resume{After: keys[600]}}},
					{"resume-hot", Options{Workers: 8, Seed: true, Resume: &Resume{After: keys[300]}}},
					{"prefix", Options{Workers: 8, Prefix: "d01/", SplitCoin: coin(seed)}},
				}
				for _, inclusive := range []bool{false, true} {
					for _, c := range cfgs {
						name := fmt.Sprintf("page%d/short=%v/seed%d/inclusive=%v/%s", page, short, seed, inclusive, c.name)
						t.Run(name, func(t *testing.T) {
							v := newVersioned(keys, groupKeyspace(len(keys), page, 300), seed)
							v.short, v.empties, v.inclusive = short, short, inclusive
							opt := c.opt
							opt.PageSize = page
							st := runGroups(t, v, opt)
							switch c.name {
							case "warm-start":
								if st.Get(cSeedFromCuts) != int64(len(prior.Cuts)) || st.Get(cSeedFromTiles) < 2 || st.Get(cSeedFromExpected) == 0 || st.Get(cSeedRequests) != 0 {
									t.Errorf("warm start: cuts=%d tiles=%d expected=%d seed_requests=%d", st.Get(cSeedFromCuts), st.Get(cSeedFromTiles), st.Get(cSeedFromExpected), st.Get(cSeedRequests))
								}
							case "slow-split":
								for _, id := range []counterID{cSlowMaxRangeLatency, cSlowRunLatency} {
									if st.Get(id) == 0 {
										t.Errorf("%s: 0, want engaged", Counters[id].Key)
									}
								}
							}
							if page <= 7 && st.Get(cGroupsMaxRows) < int64(50*page) && !strings.HasPrefix(c.name, "resume") && c.name != "prefix" && c.name != "sequential" {
								t.Errorf("groups.max_rows %d: the hot key's group was not seen whole", st.Get(cGroupsMaxRows))
							}
						})
					}
				}
			}
		}
	}
	// Latency ranking above is exercised throughout the matrix. This
	// outlier also requires an actual slow-range split while other workers
	// are busy, with groups crossing page boundaries in both Run and Count.
	t.Run("slow-range-engaged", func(t *testing.T) {
		var keys []string
		for d := range 100 {
			for i := range 100 {
				keys = append(keys, fmt.Sprintf("d/%03d/file-%05d", d, i))
			}
		}
		v := newVersioned(keys, func(int) int { return 3 }, 1)
		v.latency, v.slowPrefix, v.slowLatency = 4*time.Millisecond, "d/000/", 80*time.Millisecond
		st := runGroups(t, v, Options{Workers: 16, PageSize: 20, Seed: true, SeedFirstPage: true, SlowSplit: true})
		for _, id := range []counterID{cSplitReasonSlowRange, cSlowMaxRangeLatency, cSlowRunLatency, cGroupsHeldRows} {
			if st.Get(id) == 0 {
				t.Errorf("%s: 0, want engaged", Counters[id].Key)
			}
		}
	})
}

// TestGroupsSplitVetoAndRelist: forced splits around a hot key are vetoed
// while its group is open, and a split that drops held rows lists them
// again, once.
func TestGroupsSplitVetoAndRelist(t *testing.T) {
	keys := groupKeys(600)
	v := newVersioned(keys, groupKeyspace(len(keys), 3, 300), 1)
	st := runGroups(t, v, Options{Workers: 8, PageSize: 3, SplitCoin: func() bool { return true }})
	for _, id := range []counterID{cGroupsOpenPages, cSplitSkippedGroupOpen, cGroupsHeldRows, cGroupsRelistedRows} {
		if st.Get(id) == 0 {
			t.Errorf("%s: 0, want engaged", Counters[id].Key)
		}
	}
}

type counterID = counters.ID

// TestGroupsTake checks the hold-back transitions page by page.
func TestGroupsTake(t *testing.T) {
	p := newGroupEngine(t, &versioned{}, Options{})
	rows := func(keys string) []Row {
		var out []Row
		for _, k := range strings.Split(keys, ",") {
			if k != "" {
				out = append(out, Row{Key: k})
			}
		}
		return out
	}
	type step struct {
		page       string
		done       bool
		emit, held string
		open       bool
	}
	for _, tc := range []struct {
		name  string
		steps []step
	}{
		{"split-at-last-key", []step{{"A,B,B", false, "A", "B,B", false}, {"B,C", false, "B,B,B", "C", false}, {"C,D", true, "C,C,D", "", false}}},
		{"hot-key", []step{{"A,B", false, "A", "B", false}, {"B", false, "B,B", "", true}, {"B", false, "B", "", true}, {"B,C", false, "B", "C", false}}},
		{"one-key-pages-held", []step{{"B", false, "", "B", false}, {"C", false, "B", "C", false}, {"C", false, "C,C", "", true}, {"D", false, "", "D", false}}},
		{"empty-keeps-state", []step{{"B", false, "", "B", false}, {"", false, "", "B", false}, {"B", false, "B,B", "", true}, {"", false, "", "", true}, {"", true, "", "", false}}},
		{"held-then-one-key", []step{{"A,B", false, "A", "B", false}, {"C,C", false, "B", "C,C", false}, {"", true, "C,C", "", false}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g groups[Object]
			c := ""
			for i, s := range tc.steps {
				emit, last, _ := p.take(&g, rows(s.page), s.done, c)
				c = last
				if got := strings.Join(keysOf(emit), ","); got != s.emit {
					t.Fatalf("step %d: emitted %q, want %q", i, got, s.emit)
				}
				if got := strings.Join(keysOf(g.held), ","); got != s.held || g.open != s.open {
					t.Fatalf("step %d: held %q open %v, want %q %v", i, got, g.open, s.held, s.open)
				}
			}
		})
	}
}

// cycling answers one key's rows forever with tokens that cycle.
type cycling struct{ period int }

func (c cycling) fetch(_ context.Context, q *Query) (*Page[Object], error) {
	n, _ := strconv.Atoi(q.Token)
	return &Page[Object]{Rows: []Row{{Key: "hot"}}, Truncated: true, Token: strconv.Itoa((n + 1) % c.period)}, nil
}

// TestGroupsTokenCycle: a hot key's pages whose tokens cycle are an
// invalid page, in a range and in the sequential chain, not an endless
// stream of duplicate rows.
func TestGroupsTokenCycle(t *testing.T) {
	for _, seq := range []bool{false, true} {
		for _, period := range []int{2, 3, 100} {
			p, err := New(Options{Sequential: seq, Workers: 2, PageSize: 1}, Description[Object]{Target: "test", Listing: Listing,
				Groups: true, Detach: groupSchema.Detach}, cycling{period}.fetch)
			if err != nil {
				t.Fatal(err)
			}
			_, err = order.Run[Range, Object](context.Background(), p, &collect{}, &groupSchema, order.Options{Workers: 2, Budget: 1 << 22, Groups: true})
			if !errors.Is(err, ErrInvalidPage) {
				t.Fatalf("sequential=%v period %d: %v, want an invalid page", seq, period, err)
			}
		}
	}
}

// TestGroupsOutOfOrder: a key below the previous one is invalid in group
// mode; an equal one at a seam the engine did not leave open is too.
func TestGroupsOutOfOrder(t *testing.T) {
	pages := [][]Row{{{Key: "a"}, {Key: "b"}}, {{Key: "a"}}}
	fetch := func(_ context.Context, q *Query) (*Page[Object], error) {
		i, _ := strconv.Atoi(q.Token)
		pg := &Page[Object]{Rows: pages[i]}
		if i+1 < len(pages) {
			pg.Truncated, pg.Token = true, strconv.Itoa(i+1)
		}
		return pg, nil
	}
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach}, fetch)
	if err != nil {
		t.Fatal(err)
	}
	_, err = order.Run[Range, Object](context.Background(), p, &collect{}, &groupSchema, order.Options{Workers: 1, Budget: 1 << 22, Groups: true})
	if !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("%v, want an invalid page", err)
	}
}

// TestGroupsNeedDetach: a grouped description without Detach is refused.
func TestGroupsNeedDetach(t *testing.T) {
	if _, err := New(Options{}, Description[Object]{Target: "test", Listing: Listing, Groups: true}, nil); err == nil {
		t.Fatal("a grouped listing without Detach was accepted")
	}
}

type groupEmitter[P any] struct{ rows []schema.Row[P] }

func (e *groupEmitter[P]) Rows(rows []schema.Row[P]) error {
	e.rows = append(e.rows, rows...)
	return nil
}
func (e *groupEmitter[P]) Child(Range) error { return errors.New("unexpected split") }

// TestGroupsRejectSuppressedTokenCycle: A token cycle is invalid even when resume
// suppresses every returned row.
func TestGroupsRejectSuppressedTokenCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	p, err := New(Options{Workers: 1, Resume: &Resume{After: "kb%"}}, Description[Object]{
		Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach,
		AcceptBound: SafeKey,
	}, func(_ context.Context, q *Query) (*Page[Object], error) {
		calls++
		if calls > 8 {
			cancel()
			return nil, context.Canceled
		}
		token := "A"
		if q.Token == "A" {
			token = "B"
		}
		return &Page[Object]{Rows: []Row{{Key: "kb!"}}, Truncated: true, Token: token}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = p.expandRange(ctx, Range{lo: "kb", after: "kb%", open: true}, &groupEmitter[Object]{})
	if !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("suppressed A/B token cycle was not rejected: calls=%d error=%v", calls, err)
	}
}

type groupPayload struct{ Text string }

// TestGroupsEmittedRowsOwnTheirStorage: Emitted groups release the page arena and
// excluded tail row headers.
func TestGroupsEmittedRowsOwnTheirStorage(t *testing.T) {
	detach := func(rows []schema.Row[groupPayload]) []schema.Row[groupPayload] {
		out := make([]schema.Row[groupPayload], len(rows))
		for i, r := range rows {
			out[i] = schema.Row[groupPayload]{Key: strings.Clone(r.Key), Data: groupPayload{Text: strings.Clone(r.Data.Text)}}
		}
		return out
	}
	p, err := New(Options{}, Description[groupPayload]{Target: "test", Listing: Listing, Groups: true, Detach: detach}, nil)
	if err != nil {
		t.Fatal(err)
	}
	arena := "a" + strings.Repeat("b", 8<<20)
	page := []schema.Row[groupPayload]{{Key: arena[:1]}, {Key: arena[:1]}, {Key: arena[1:2], Data: groupPayload{Text: arena[1:]}}}
	var g groups[groupPayload]
	emit, _, _ := p.take(&g, page, false, "")
	if len(emit) != 2 || len(g.held) != 1 {
		t.Fatal("unexpected transition")
	}
	if unsafe.StringData(emit[0].Key) == unsafe.StringData(arena) {
		t.Errorf("two tiny emitted rows still pin the original %d-byte arena after the tail was detached", len(arena))
	}
	if unsafe.SliceData(emit) == unsafe.SliceData(page) {
		t.Errorf("emitted prefix still owns the page array, including its hidden tail header referencing %d bytes", len(page[2].Data.Text))
	}
	// Native-sized metadata also exposes the fromPage==0 && owned path:
	// a detached B is emitted while a page of C is held. The emitted B's
	// combined backing array still contains all of C's row headers.
	g = groups[groupPayload]{}
	p.take(&g, []schema.Row[groupPayload]{{Key: "a"}, {Key: "a"}, {Key: "b"}}, false, "")
	if !g.owned {
		t.Fatal("B was not detached")
	}
	held := g.held
	page = make([]schema.Row[groupPayload], 1000)
	arena = "c" + strings.Repeat("x", 1000*1024)
	for i := range page {
		page[i] = schema.Row[groupPayload]{Key: arena[:1], Data: groupPayload{Text: arena[1+i*1024 : 1+(i+1)*1024]}}
	}
	emit, _, _ = p.take(&g, page, false, "a")
	if len(emit) != 1 {
		t.Fatal("expected only the detached B")
	}
	if unsafe.SliceData(emit) != unsafe.SliceData(held) {
		t.Errorf("already detached B was combined with the excluded page's row headers")
	}
}

// TestGroupsMaximumIncludesMiddleGroup: The maximum group size includes complete
// interior groups, not just the held tail.
func TestGroupsMaximumIncludesMiddleGroup(t *testing.T) {
	p := newGroupEngine(t, &versioned{}, Options{})
	rows := []Row{{Key: "a"}}
	for range 100 {
		rows = append(rows, Row{Key: "b"})
	}
	rows = append(rows, Row{Key: "c"})
	var g groups[Object]
	p.take(&g, rows, true, "")
	st := p.Stats()
	if got := st.Get(cGroupsMaxRows); got != 100 {
		t.Fatalf("groups.max_rows=%d, want 100", got)
	}
}

func TestGroupRawProgressBounded(t *testing.T) {
	for _, sequential := range []bool{false, true} {
		t.Run(fmt.Sprint(sequential), func(t *testing.T) {
			calls := 0
			stop := errors.New("test end")
			p, err := New(Options{Workers: 1, Sequential: sequential, Resume: &Resume{After: "kb\x02"}}, Description[Object]{
				Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach, AcceptBound: SafeKey,
			}, func(context.Context, *Query) (*Page[Object], error) {
				calls++
				if calls > EmptyChainLive+101 {
					return nil, stop
				}
				return &Page[Object]{Rows: []Row{{Key: "kb\x01"}}, Truncated: true, Token: fmt.Sprint(calls)}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if sequential {
				err = p.expandSequential(context.Background(), &groupEmitter[Object]{})
			} else {
				err = p.expandRange(context.Background(), Range{lo: "kb", after: "kb\x02", open: true}, &groupEmitter[Object]{})
			}
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			advanced, chained := p.Frontier()
			if advanced != EmptyChainLive+1 || chained != 100 {
				t.Fatalf("calls=%d frontier=%d chained=%d", calls, advanced, chained)
			}
		})
	}
}

func TestGroupRawSuppressedOrder(t *testing.T) {
	calls := 0
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
		func(context.Context, *Query) (*Page[Object], error) {
			calls++
			k := "kb!"
			if calls > 1 {
				k = "ka"
			}
			return &Page[Object]{Rows: []Row{{Key: k}}, Truncated: true, Token: fmt.Sprint(calls)}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	err = p.expandRange(context.Background(), Range{lo: "k", after: "kb%", open: true}, &groupEmitter[Object]{})
	if !errors.Is(err, ErrInvalidPage) || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestGroupMaximumAllEmissionPaths(t *testing.T) {
	rows := []Row{{Key: "a"}}
	for range 100 {
		rows = append(rows, Row{Key: "b"})
	}
	rows = append(rows, Row{Key: "c"})
	for _, mode := range []string{"sequential", "seed-eof"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			p, err := New(Options{Workers: 1, Sequential: mode == "sequential"}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
				func(context.Context, *Query) (*Page[Object], error) {
					calls++
					return &Page[Object]{Rows: append([]Row(nil), rows...)}, nil
				})
			if err != nil {
				t.Fatal(err)
			}
			e := &groupEmitter[Object]{}
			if mode == "seed-eof" {
				_, err = p.seedEmit(&Page[Object]{Rows: rows}, "", "", e)
			} else {
				err = p.Expand(context.Background(), p.Root(), e)
			}
			if err != nil {
				t.Fatal(err)
			}
			st := p.Stats()
			if len(e.rows) != 102 || st.Get(cGroupsMaxRows) != 100 {
				t.Fatalf("rows=%d max=%d", len(e.rows), st.Get(cGroupsMaxRows))
			}
			if mode == "sequential" && calls != 1 {
				t.Fatal(calls)
			}
		})
	}
}

func TestGroupOverfetchRows(t *testing.T) {
	calls := 0
	p, err := New(Options{Workers: 1}, Description[Object]{Target: "test", Listing: Listing, Groups: true, Detach: groupSchema.Detach},
		func(context.Context, *Query) (*Page[Object], error) {
			calls++
			return &Page[Object]{Rows: []Row{{Key: "a"}, {Key: "b"}, {Key: "b"}, {Key: "c"}, {Key: "c"}}, Truncated: true, Token: "next"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	e := &groupEmitter[Object]{}
	if err := p.expandRange(context.Background(), Range{hi: "b"}, e); err != nil {
		t.Fatal(err)
	}
	st := p.Stats()
	if len(e.rows) != 3 || calls != 1 || st.Get(cGroupsOverfetchRows) != 2 || st.Get(cOverfetchKeys) != 2 {
		t.Fatalf("rows=%d calls=%d stats=%+v", len(e.rows), calls, p.Stats())
	}
}
