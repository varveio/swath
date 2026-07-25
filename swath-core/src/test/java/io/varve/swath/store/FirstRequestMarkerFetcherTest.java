/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

import static io.varve.swath.store.PageFetcherTestSupport.REQ;
import static io.varve.swath.store.PageFetcherTestSupport.stubFetcher;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link FirstRequestMarkerFetcher} logs {@code list_first_request_issued}/{@code
 * list_first_page_returned} exactly once each (on the very first {@link #fetchPage} call), then
 * passes every subsequent call straight through with no further logging or added behavior.
 */
class FirstRequestMarkerFetcherTest {

    private static ListAppender<ILoggingEvent> attach() {
        Logger logger =
                (Logger) LoggerFactory.getLogger(FirstRequestMarkerFetcher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        return appender;
    }

    private static void detach(ListAppender<ILoggingEvent> appender) {
        Logger logger =
                (Logger) LoggerFactory.getLogger(FirstRequestMarkerFetcher.class);
        logger.detachAppender(appender);
    }

    @Test
    void delegatesFetchPageAndCapabilities() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        PageFetcher delegate = stubFetcher(calls);
        FirstRequestMarkerFetcher fetcher = new FirstRequestMarkerFetcher(delegate, System.nanoTime());

        ListPage page = fetcher.fetchPage(REQ);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(page.httpStatus()).isEqualTo(200);
        assertThat(fetcher.capabilities()).isEqualTo(StoreCapabilities.s3());
    }

    @Test
    void logsBothMarkersExactlyOnceOnTheFirstCallOnly() throws Exception {
        ListAppender<ILoggingEvent> appender = attach();
        try {
            AtomicInteger calls = new AtomicInteger();
            PageFetcher delegate = stubFetcher(calls);
            FirstRequestMarkerFetcher fetcher = new FirstRequestMarkerFetcher(delegate, System.nanoTime());

            fetcher.fetchPage(REQ);
            fetcher.fetchPage(REQ);
            fetcher.fetchPage(REQ);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages.stream().filter(m -> m.startsWith("list_first_request_issued")).count())
                    .as("issued marker fires exactly once, not once per call")
                    .isEqualTo(1L);
            assertThat(messages.stream().filter(m -> m.startsWith("list_first_page_returned")).count())
                    .as("returned marker fires exactly once, not once per call")
                    .isEqualTo(1L);
            assertThat(calls.get()).isEqualTo(3);
        } finally {
            detach(appender);
        }
    }

    @Test
    void issuedMarkerFiresBeforeTheDelegateCallReturns() throws Exception {
        ListAppender<ILoggingEvent> appender = attach();
        try {
            PageFetcher delegate = new PageFetcher() {
                @Override
                public ListPage fetchPage(PageRequest req) {
                    // At this point the delegate call is already in flight — the issued marker
                    // must have logged already (it logs BEFORE delegating), the returned marker
                    // must not have (it logs AFTER the delegate returns).
                    List<String> soFar = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
                    assertThat(soFar).anyMatch(m -> m.startsWith("list_first_request_issued"));
                    assertThat(soFar).noneMatch(m -> m.startsWith("list_first_page_returned"));
                    return new ListPage(List.of(), List.of(), false, null, null, null, 200, Duration.ZERO);
                }

                @Override
                public StoreCapabilities capabilities() {
                    return StoreCapabilities.s3();
                }
            };
            new FirstRequestMarkerFetcher(delegate, System.nanoTime()).fetchPage(REQ);
        } finally {
            detach(appender);
        }
    }
}
