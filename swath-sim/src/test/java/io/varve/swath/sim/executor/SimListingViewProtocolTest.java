/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The simulated store call answers a listing request the way S3 answers it — checked against the one
 * place that owns those rules, {@link ListObjectsV2Pager}, over the same fixture rows.
 *
 * <p><b>Why a differential and not an expectation.</b> The policies this module drives are the
 * production ones: the seed planner reads a delimiter probe's common prefixes, the range scanner
 * reads {@code IsTruncated}. Both were written against S3's protocol, so a simulator that answers
 * those two questions even slightly differently mismodels the engine rather than the workload — and
 * the difference is invisible in a run's output, which is exactly why it needs pinning. The pager is
 * the module's fidelity authority (every replay-server store is differentialled through it), so
 * "agrees with the pager over the same rows" is the sharpest statement of correctness available
 * without a live bucket.
 *
 * <p>Each case asserts the pager's answer <em>and</em> the literal expected answer: the differential
 * localises a future regression, the literal keeps the test readable as a statement about S3.
 */
class SimListingViewProtocolTest {

    /** Small enough that the fixtures below straddle the boundary in one or two pages. */
    private static final int PROBE_LIMIT = 2;

    private static final int PAGE_SIZE = 10;

    /** Three full pages and nothing after them — the exact-multiple boundary. */
    private static final int EXACT_MULTIPLE_KEYS = 3 * PAGE_SIZE;

    private static final byte[] SLASH = {'/'};

    /**
     * A delimiter probe that stopped on a rolled-up directory resumes {@code startAfter} that
     * directory's own bytes — that is what the seed planner's bounded second page passes
     * ({@code HybridSeedPlanner} hands back {@code lastKey}, which for a capped structure probe is a
     * common prefix, not an object key). Every key beneath {@code d1/} sorts strictly after
     * {@code d1/}, so a resume that only advances the range floor rolls the same directory up a
     * second time: one duplicate cut, and one legitimate directory pushed off the far end of a page
     * that stays capped. S3 does not do that, and neither does the pager — it skips a common prefix
     * at or below the resume boundary and jumps the cursor to {@code successor(d1/)}.
     */
    @Test
    void aResumedDelimiterProbeDoesNotRollUpTheDirectoryItResumedAt() {
        ListingFixtureStore store = directoryFixture();
        SimListingView view = new SimListingView(store, null);

        SimListingView.Rollup first = view.rollup(null, null, PROBE_LIMIT);
        assertThat(names(first.commonPrefixes())).containsExactly("d0/", "d1/");
        assertThat(first.capped()).as("more directories remain").isTrue();
        assertThat(text(first.lastKey())).as("the boundary the planner resumes at").isEqualTo("d1/");

        SimListingView.Rollup resumed = view.rollup(null, first.lastKey(), PROBE_LIMIT);

        assertThat(names(resumed.commonPrefixes()))
                .isEqualTo(commonPrefixesFromPager(store, first.lastKey(), PROBE_LIMIT))
                .containsExactly("d2/", "d3/");
        assertThat(resumed.objectCount()).isZero();
    }

    /**
     * Page one of the same probe must also match the pager, so the resume case above cannot be passed
     * by a view that merely mangles both pages the same way.
     */
    @Test
    void theFirstDelimiterProbePageMatchesThePager() {
        ListingFixtureStore store = directoryFixture();
        SimListingView view = new SimListingView(store, null);

        SimListingView.Rollup first = view.rollup(null, null, PROBE_LIMIT);

        assertThat(names(first.commonPrefixes()))
                .isEqualTo(commonPrefixesFromPager(store, null, PROBE_LIMIT));
    }

    /**
     * A range holding an exact multiple of the page size is finished when its last full page arrives:
     * S3 sets {@code IsTruncated=false} on a page that consumed the last key, because the service
     * looks one key past the page it returns, and the pager models that by reading {@code maxKeys + 1}
     * rows. A view that instead calls a page truncated whenever it came back full cannot tell "the end
     * landed here" from "there is more", so it charges the modelled system one extra, empty listing
     * call — on every range whose cardinality happens to divide by the page size, and on the frontier
     * range of every run, where there is no upper bound to trim against and truncation is the only
     * completion signal the scanner has.
     */
    @Test
    void aRangeWhoseKeyCountIsAnExactMultipleOfThePageSizeEndsOnItsLastFullPage() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(EXACT_MULTIPLE_KEYS);
        SimListingView view = new SimListingView(store, null);

        List<Integer> pageSizes = new ArrayList<>();
        byte[] cursor = null;
        while (true) {
            SimListingView.Page page = view.page(cursor, PAGE_SIZE);
            pageSizes.add(page.keys().size());
            if (!page.truncated()) {
                break;
            }
            cursor = page.keys().getLast();
        }

        // One entry per modelled listing call: three full pages, no trailing empty one.
        assertThat(pageSizes)
                .isEqualTo(pageSizesFromPager(store, PAGE_SIZE))
                .containsExactly(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
    }

    /**
     * The complement: a range that does <b>not</b> divide by the page size still ends on a short page,
     * and still costs the same number of calls as the pager. Without this, the case above would also
     * pass on a view that never reported truncation at all.
     */
    @Test
    void aRangeWithARemainderStillEndsOnAShortPage() {
        ListingFixtureStore store = ListingFixtureStore.ofGeneratedKeys(EXACT_MULTIPLE_KEYS + 3);
        SimListingView view = new SimListingView(store, null);

        List<Integer> pageSizes = new ArrayList<>();
        byte[] cursor = null;
        while (true) {
            SimListingView.Page page = view.page(cursor, PAGE_SIZE);
            pageSizes.add(page.keys().size());
            if (!page.truncated()) {
                break;
            }
            cursor = page.keys().getLast();
        }

        assertThat(pageSizes)
                .isEqualTo(pageSizesFromPager(store, PAGE_SIZE))
                .containsExactly(PAGE_SIZE, PAGE_SIZE, PAGE_SIZE, 3);
    }

    // --- fixtures and the pager's own answer ----------------------------------

    /** Four sibling directories, each wide enough that a rollup must jump rather than scan out. */
    private static ListingFixtureStore directoryFixture() {
        List<byte[]> keys = new ArrayList<>();
        for (int dir = 0; dir < 4; dir++) {
            for (int child = 0; child < 3; child++) {
                keys.add(utf8("d" + dir + "/" + child));
            }
        }
        return new ListingFixtureStore(keys);
    }

    private static List<String> commonPrefixesFromPager(ListingFixtureStore store, byte[] startAfter, int maxKeys) {
        List<String> out = new ArrayList<>();
        for (S3ResultEntry entry : pagerPage(store, SLASH, startAfter, maxKeys).entries()) {
            if (entry instanceof S3ResultEntry.CommonPrefixResult) {
                out.add(text(entry.key()));
            }
        }
        return out;
    }

    /** The pager's page-by-page walk of the whole fixture: one entry per listing call. */
    private static List<Integer> pageSizesFromPager(ListingFixtureStore store, int maxKeys) {
        List<Integer> sizes = new ArrayList<>();
        byte[] startAfter = null;
        while (true) {
            S3ListResult result = pagerPage(store, null, startAfter, maxKeys);
            sizes.add(result.entries().size());
            if (!result.truncated()) {
                return sizes;
            }
            startAfter = result.entries().getLast().key();
        }
    }

    private static S3ListResult pagerPage(ListingFixtureStore store, byte[] delimiter, byte[] startAfter,
                                          int maxKeys) {
        ListObjectsV2Pager pager = new ListObjectsV2Pager(store, new ReplayMetrics());
        return pager.list(new S3ListRequest("bucket", null, delimiter, startAfter, null, maxKeys, true, false));
    }

    private static List<String> names(List<byte[]> keys) {
        return keys.stream().map(SimListingViewProtocolTest::text).toList();
    }

    private static String text(byte[] key) {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
