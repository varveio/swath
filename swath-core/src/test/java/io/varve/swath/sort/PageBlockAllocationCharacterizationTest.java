/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.management.HotSpotDiagnosticMXBean;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exact persisted-page allocation characterization. This is opt-in and runs its probe in a small
 * interpreted child JVM with {@code -XX:-UseTLAB}; every allocation is therefore an
 * {@code ObjectAllocationOutsideTLAB} event instead of relying on normal-TLAB sampling. The child
 * verifies the VM flag, requires a named payload-sized positive-control {@code byte[]} to appear,
 * then rejects payload-sized byte arrays whose stack contains the legacy
 * {@code parseSerializedFields} path or current PageBlock header/deserialize/cursor paths.
 *
 * <p>Exact invocation:
 * <pre>
 * JAVA_TOOL_OPTIONS='-Dswath.profile.allocations=exact' ./gradlew \
 *   :swath-core:test \
 *   --tests 'io.varve.swath.sort.PageBlockAllocationCharacterizationTest'
 * </pre>
 * The normal build sees this class but skips it.
 */
@EnabledIfSystemProperty(named = "swath.profile.allocations", matches = "exact")
public class PageBlockAllocationCharacterizationTest {

    private static final int ALLOCATION_FLOOR_BYTES = 2_048;
    private static final int POSITIVE_CONTROL_BYTES = 8_192;
    private static final ListEntryComparator CMP = new ListEntryComparator();
    private static volatile byte[] allocationSink;
    private static volatile int decodedRowsSink;

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void noTlabChildSeesPositiveControlAndNoPersistedPayloadCopy(@TempDir Path dir)
            throws Exception {
        Path recording = dir.resolve("page-block-copy-removal.jfr");
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xint");
        command.add("-XX:-UseTLAB");
        command.add("-cp");
        command.add(childClasspath());
        command.add(PageBlockAllocationCharacterizationTest.class.getName());
        command.add(recording.toString());

        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!child.waitFor(30, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            throw new AssertionError("no-TLAB allocation characterization child timed out");
        }
        String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        System.out.print(output);

        assertThat(child.exitValue()).as(output).isZero();
        assertThat(output).contains("use_tlab=false")
                .contains("positive_control_arrays=")
                .contains("legacy_parse_arrays=0")
                .contains("current_codec_arrays=0")
                .contains("page_block_decode_arrays=0");
    }

    /** Child entrypoint; the parent test always supplies {@code -XX:-UseTLAB}. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected JFR output path");
        }
        runProbe(Path.of(args[0]));
    }

    private static void runProbe(Path recordingPath) throws Exception {
        HotSpotDiagnosticMXBean hotspot =
                ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        boolean useTlab = Boolean.parseBoolean(hotspot.getVMOption("UseTLAB").getValue());
        if (useTlab) {
            throw new AssertionError("allocation characterization requires UseTLAB=false");
        }

        byte[] body = largeNoneBody();
        PageBlockCodec.Header warmHeader = PageBlockCodec.parseHeader(body);
        if (warmHeader.payloadLength() < ALLOCATION_FLOOR_BYTES) {
            throw new AssertionError("probe payload is below allocation floor: "
                    + warmHeader.payloadLength());
        }
        drain(PageBlockCodec.deserialize(body, warmHeader, Path.of("warmup.pageseg")));

        try (Recording recording = new Recording()) {
            recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
            recording.start();
            allocationSink = positiveControlAllocation();
            allocationSink[0] = 1; // consume the array; -Xint also prevents scalar replacement
            drain(PageBlock.deserialize(body, Path.of("allocation-probe.pageseg")));
            recording.stop();
            recording.dump(recordingPath);
        }

        Evidence evidence = readEvidence(recordingPath);
        System.out.printf("COPY_REMOVAL_ALLOCATION_EVIDENCE use_tlab=%s floor_bytes=%d "
                        + "positive_control_arrays=%d legacy_parse_arrays=%d "
                        + "current_codec_arrays=%d page_block_decode_arrays=%d%n",
                useTlab, ALLOCATION_FLOOR_BYTES, evidence.positiveControl(),
                evidence.legacyParse(), evidence.currentCodec(), evidence.pageBlockDecode());
        if (evidence.positiveControl() < 1) {
            throw new AssertionError("allocation capture missed its named positive control");
        }
        if (evidence.targetTotal() != 0) {
            throw new AssertionError("persisted PageBlock parsing/decoding allocated "
                    + evidence.targetTotal() + " payload-sized byte arrays");
        }
    }

    /** Named positive-control stack site required before a zero target count is credible. */
    private static byte[] positiveControlAllocation() {
        return new byte[POSITIVE_CONTROL_BYTES];
    }

    private static byte[] largeNoneBody() {
        List<ListEntry> rows = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            String storageClass = "storage-" + (i % PageBlock.DICT_CAP) + "-" + "x".repeat(4_096);
            rows.add(new ObjectEntry(KeyBytes.ofUtf8(String.format("key-%05d-long-suffix", i)),
                    i, i, null, storageClass, null, false,
                    null, null, null, null));
        }
        return PageBlock.pack(rows, CMP, PageCodec.NONE).serialize();
    }

    private static void drain(PageBlock block) {
        PageBlockCursor cursor = block.cursor();
        int rows = 0;
        while (cursor.hasNext()) {
            cursor.next();
            rows++;
        }
        decodedRowsSink = rows;
    }

    private static Evidence readEvidence(Path recording) throws IOException {
        long positive = 0;
        long legacy = 0;
        long currentCodec = 0;
        long pageBlockDecode = 0;
        try (RecordingFile events = new RecordingFile(recording)) {
            while (events.hasMoreEvents()) {
                RecordedEvent event = events.readEvent();
                if (!"jdk.ObjectAllocationOutsideTLAB".equals(event.getEventType().getName())
                        || event.getLong("allocationSize") < ALLOCATION_FLOOR_BYTES
                        || !isByteArray(event)
                        || event.getStackTrace() == null) {
                    continue;
                }
                List<RecordedFrame> frames = event.getStackTrace().getFrames();
                if (hasFrame(frames, PageBlockAllocationCharacterizationTest.class,
                        "positiveControlAllocation")) {
                    positive++;
                }
                if (hasFrame(frames, PageBlockCodec.class, "parseSerializedFields")) {
                    legacy++;
                }
                if (hasAnyFrame(frames, PageBlockCodec.class,
                        Set.of("parseHeader", "deserialize"))) {
                    currentCodec++;
                }
                if (hasAnyFrame(frames, PageBlock.class,
                        Set.of("deserialize", "cursor", "decodedPayload"))) {
                    pageBlockDecode++;
                }
            }
        }
        return new Evidence(positive, legacy, currentCodec, pageBlockDecode);
    }

    private static boolean hasAnyFrame(List<RecordedFrame> frames, Class<?> type,
                                       Set<String> methods) {
        return frames.stream().anyMatch(frame -> type.getName().equals(
                frame.getMethod().getType().getName())
                && methods.contains(frame.getMethod().getName()));
    }

    private static boolean hasFrame(List<RecordedFrame> frames, Class<?> type, String method) {
        return hasAnyFrame(frames, type, Set.of(method));
    }

    private static boolean isByteArray(RecordedEvent event) {
        String type = event.getClass("objectClass").getName();
        return "[B".equals(type) || "byte[]".equals(type);
    }

    private static String childClasspath() {
        LinkedHashSet<String> entries = new LinkedHashSet<>();
        addClasspath(entries, System.getProperty("java.class.path", ""));
        for (ClassLoader loader = PageBlockAllocationCharacterizationTest.class.getClassLoader();
             loader != null; loader = loader.getParent()) {
            if (loader instanceof URLClassLoader urls) {
                for (URL url : urls.getURLs()) {
                    if ("file".equals(url.getProtocol())) {
                        entries.add(Path.of(URI.create(url.toString())).toString());
                    }
                }
            }
        }
        return String.join(System.getProperty("path.separator"), entries);
    }

    private static void addClasspath(LinkedHashSet<String> entries, String classpath) {
        if (classpath.isBlank()) {
            return;
        }
        for (String entry : classpath.split(java.util.regex.Pattern.quote(
                System.getProperty("path.separator")))) {
            if (!entry.isBlank()) {
                entries.add(entry);
            }
        }
    }

    private record Evidence(long positiveControl, long legacyParse,
                            long currentCodec, long pageBlockDecode) {
        long targetTotal() {
            return legacyParse + currentCodec + pageBlockDecode;
        }
    }
}
