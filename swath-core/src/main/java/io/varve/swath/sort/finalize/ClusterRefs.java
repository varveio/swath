/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.spill.PageRef;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The ordered page references of one indivisible transitive overlap component, read exactly once by
 * the single encoder that owns the component's part. A component within the plan reference cap
 * stays in heap. A larger component is still legal input—one broad page can overlap an unbounded
 * number of narrow ones—so its references are appended to a staging file instead, which keeps
 * routing residency bounded by the cap rather than by the component. Only page coordinates ever
 * leave heap; rows stay in their page runs, and the encoder still admits pages one at a time under
 * its decoded-page budget.
 *
 * <p>The file carries no checksum of its own: it never outlives the merge that wrote it, and every
 * reference read back is validated against the referenced page's framed CRC and stored header
 * before a single row is decoded.
 */
sealed interface ClusterRefs {

    /** Open the one ordered pass this component supports. */
    Cursor open() throws IOException;

    /** Release any staging file backing these references. Safe to repeat. */
    void discard() throws IOException;

    /** Ordered reader with the single-reference lookahead incremental page admission needs. */
    interface Cursor extends Closeable {

        /** The next unconsumed reference, or {@code null} once the component is exhausted. */
        PageRef peek() throws IOException;

        /** Consume and return the reference {@link #peek()} would have returned. */
        PageRef next() throws IOException;
    }

    /** References for a component that fit the heap-resident plan reference cap. */
    record Heap(List<PageRef> refs) implements ClusterRefs {
        public Heap {
            refs = List.copyOf(refs);
        }

        @Override
        public Cursor open() {
            return new HeapCursor(refs);
        }

        @Override
        public void discard() {
        }
    }

    /** References for a component too large to keep in heap, appended in router order. */
    record Spilled(Path file, long refs) implements ClusterRefs {
        public Spilled {
            if (refs < 2) {
                throw new IllegalArgumentException("spilled overlap cluster needs two page refs");
            }
        }

        @Override
        public Cursor open() throws IOException {
            return new SpillCursor(file, refs);
        }

        @Override
        public void discard() throws IOException {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Collects one component in router order and promotes it out of heap the moment it outgrows
     * {@code heapLimit}. Closing without a preceding {@link #build()} removes a partial spill, so a
     * routing failure cannot leave a half-written component behind.
     */
    final class Builder implements Closeable {
        private final int heapLimit;
        private final Path stagingDir;
        private final int sequence;
        private final List<PageRef> heap = new ArrayList<>();
        private DataOutputStream out;
        private Path file;
        private long count;
        private boolean built;

        Builder(int heapLimit, Path stagingDir, int sequence) {
            if (heapLimit < 1 || sequence < 0) {
                throw new IllegalArgumentException("overlap cluster collection is out of bounds");
            }
            this.heapLimit = heapLimit;
            this.stagingDir = stagingDir;
            this.sequence = sequence;
        }

        void add(PageRef ref) throws IOException {
            if (out == null && heap.size() == heapLimit) {
                promote();
            }
            if (out == null) {
                heap.add(ref);
            } else {
                write(out, ref);
            }
            count = Math.addExact(count, 1);
        }

        long count() {
            return count;
        }

        boolean spilled() {
            return out != null;
        }

        /**
         * Freeze the collected component. A spilled component is still flushed by {@link #close()},
         * which every caller reaches before the plan carrying these references is dispatched.
         */
        ClusterRefs build() throws IOException {
            built = true;
            if (out == null) {
                return new Heap(heap);
            }
            out.flush();
            return new Spilled(file, count);
        }

        @Override
        public void close() throws IOException {
            if (out == null) {
                return;
            }
            try {
                out.close();
            } finally {
                out = null;
                if (!built) {
                    Files.deleteIfExists(file);
                }
            }
        }

        private void promote() throws IOException {
            file = stagingDir.resolve(StagingNames.clusterRefsTmp(sequence));
            out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)));
            for (PageRef ref : heap) {
                write(out, ref);
            }
            heap.clear();
        }
    }

    /** Index walk over an in-heap component. */
    final class HeapCursor implements Cursor {
        private final List<PageRef> refs;
        private int index;

        HeapCursor(List<PageRef> refs) {
            this.refs = refs;
        }

        @Override
        public PageRef peek() {
            return index < refs.size() ? refs.get(index) : null;
        }

        @Override
        public PageRef next() {
            return refs.get(index++);
        }

        @Override
        public void close() {
        }
    }

    /** Buffered sequential walk over a spilled component, decoding one reference ahead. */
    final class SpillCursor implements Cursor {
        private final DataInputStream in;
        private long remaining;
        private PageRef head;

        SpillCursor(Path file, long refs) throws IOException {
            in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)));
            remaining = refs;
        }

        @Override
        public PageRef peek() throws IOException {
            if (head == null && remaining > 0) {
                head = read(in);
                remaining--;
            }
            return head;
        }

        @Override
        public PageRef next() throws IOException {
            PageRef ref = peek();
            head = null;
            return ref;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private static void write(DataOutputStream out, PageRef ref) throws IOException {
        out.writeInt(ref.segmentId());
        out.writeLong(ref.ordinal());
        out.writeLong(ref.offset());
        out.writeInt(ref.framedLen());
        out.writeInt(ref.count());
        out.writeInt(ref.rawPayloadLength());
        writeKey(out, ref.minKey());
        writeKey(out, ref.maxKey());
    }

    private static PageRef read(DataInputStream in) throws IOException {
        int segmentId = in.readInt();
        long ordinal = in.readLong();
        long offset = in.readLong();
        int framedLen = in.readInt();
        int count = in.readInt();
        int rawPayloadLength = in.readInt();
        byte[] minKey = readKey(in);
        byte[] maxKey = readKey(in);
        return new PageRef(segmentId, ordinal, offset, framedLen, minKey, maxKey, count,
                rawPayloadLength);
    }

    private static void writeKey(DataOutputStream out, byte[] key) throws IOException {
        out.writeInt(key.length);
        out.write(key);
    }

    private static byte[] readKey(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > ByteMidpoint.MAX_KEY_LEN) {
            throw new IOException("spilled overlap cluster key length is out of bounds: " + length);
        }
        byte[] key = new byte[length];
        in.readFully(key);
        return key;
    }
}
