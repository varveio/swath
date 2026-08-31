/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

class StagingReconciliationTest {

    @Test
    void nullFileKeyIsRejectedWithoutWeakeningPhysicalIdentityGuarantee(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = Files.writeString(staging.resolve("seg-0.pageseg"), "durable");
        BasicFileAttributes actual = Files.readAttributes(
                segment, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

        assertThatThrownBy(() -> StagingReconciliation.fromPaths(
                staging, List.of(segment), ignored -> withoutFileKey(actual)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cannot establish physical identity")
                .hasMessageContaining("did not provide a file key")
                .hasMessageContaining(segment.toString());

        assertThat(segment).hasContent("durable");
    }

    @Test
    void symlinkedStagingDirectoryCannotRedirectOwnershipOrCleanup(@TempDir Path root)
            throws IOException {
        Path outside = Files.createDirectories(root.resolve("outside"));
        Path segment = Files.writeString(outside.resolve("seg-0.pageseg"), "outside segment");
        Path stale = Files.writeString(outside.resolve("merge-stale.pageseg"), "outside stale");
        Path stagingLink = Files.createSymbolicLink(root.resolve("_staging"), outside);

        assertThatThrownBy(() -> StagingReconciliation.fromPaths(
                stagingLink, List.of(stagingLink.resolve(segment.getFileName()))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort staging directory")
                .hasMessageContaining("symbolic link");

        assertThat(segment).hasContent("outside segment");
        assertThat(stale).hasContent("outside stale");
    }

    @Test
    void symlinkedOutputDirectoryIsRejectedAsPublicationAuthority(@TempDir Path root)
            throws IOException {
        Path outside = Files.createDirectories(root.resolve("outside-output"));
        Path prior = Files.writeString(outside.resolve("part-00000.parquet"), "prior");
        Path outputLink = Files.createSymbolicLink(root.resolve("data"), outside);

        assertThatThrownBy(() -> StagingReconciliation.DirectoryAuthority.capture(
                outputLink, "sort output directory"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort output directory")
                .hasMessageContaining("symbolic link");

        assertThat(prior).hasContent("prior");
    }

    @Test
    void directoryReplacementIsDetectedBeforeKickoffDeletion(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        Path segment = Files.writeString(staging.resolve("seg-0.pageseg"), "original");
        StagingReconciliation owned = StagingReconciliation.fromPaths(
                staging, List.of(segment));
        StagingReconciliation.DirectoryAuthority outputAuthority =
                StagingReconciliation.DirectoryAuthority.capture(
                        output, "sort output directory");

        Files.move(staging, root.resolve("old-staging"));
        Files.createDirectory(staging);
        Path planted = Files.writeString(staging.resolve("merge-planted.pageseg"), "planted");

        assertThatThrownBy(() -> publisher().sweepWorking(
                output, staging, owned, outputAuthority))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort staging directory identity changed");
        assertThatThrownBy(owned::deleteOwnedOriginals)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort staging directory identity changed");

        assertThat(planted).hasContent("planted");
        assertThat(root.resolve("old-staging/seg-0.pageseg")).hasContent("original");
    }

    @Test
    void outputDirectoryReplacementIsDetectedBeforeEitherKickoffSweep(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        Path segment = Files.writeString(staging.resolve("seg-0.pageseg"), "original");
        Path staleStaging = Files.writeString(
                staging.resolve("merge-stale.pageseg"), "staging stale");
        StagingReconciliation owned = StagingReconciliation.fromPaths(
                staging, List.of(segment));
        StagingReconciliation.DirectoryAuthority outputAuthority =
                StagingReconciliation.DirectoryAuthority.capture(
                        output, "sort output directory");

        Files.move(output, root.resolve("old-output"));
        Files.createDirectory(output);
        Path plantedOutput = Files.writeString(
                output.resolve("part-00000.parquet.tmp"), "output planted");

        assertThatThrownBy(() -> publisher().sweepWorking(
                output, staging, owned, outputAuthority))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort output directory identity changed");

        assertThat(staleStaging).hasContent("staging stale");
        assertThat(plantedOutput).hasContent("output planted");
        assertThat(segment).hasContent("original");
    }

    @Test
    void afterWorkingSweepReplacementIsDetectedBeforeLaterWork(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        Path outside = Files.createDirectories(root.resolve("outside"));
        Path outsideVictim = Files.writeString(
                outside.resolve("merge-victim.pageseg"), "outside");
        Path segment = Files.writeString(staging.resolve("seg-0.pageseg"), "original");
        StagingReconciliation owned = StagingReconciliation.fromPaths(
                staging, List.of(segment));
        StagingReconciliation.DirectoryAuthority outputAuthority =
                StagingReconciliation.DirectoryAuthority.capture(
                        output, "sort output directory");
        DatasetPublisher publisher = publisher((step, ignored) -> {
            if (step == PublicationStep.AFTER_WORKING_SWEEP) {
                Files.move(staging, root.resolve("old-staging"));
                Files.createSymbolicLink(staging, outside);
            }
        });

        assertThatThrownBy(() -> publisher.sweepWorking(
                output, staging, owned, outputAuthority))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("sort staging directory identity changed");

        assertThat(outsideVictim).hasContent("outside");
        assertThat(root.resolve("old-staging/seg-0.pageseg")).hasContent("original");
    }

    @Test
    void cascadeNamespaceInputsAreRejectedBeforeKickoffCleanup(@TempDir Path root)
            throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path mergeNamed = Files.writeString(
                staging.resolve("merge-input.pageseg"), "owned merge name");
        Path rangeNamed = Files.writeString(
                staging.resolve("merge-r0-input.pageseg"), "owned range name");
        Path stale = Files.writeString(
                staging.resolve("merge-stale.pageseg"), "disposable");

        assertThatThrownBy(() -> StagingReconciliation.fromPaths(
                staging, List.of(mergeNamed)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("disposable cascade namespace")
                .hasMessageContaining("merge-input.pageseg");
        assertThatThrownBy(() -> StagingReconciliation.fromPaths(
                staging, List.of(rangeNamed)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("disposable cascade namespace")
                .hasMessageContaining("merge-r0-input.pageseg");

        assertThat(mergeNamed).hasContent("owned merge name");
        assertThat(rangeNamed).hasContent("owned range name");
        assertThat(stale).hasContent("disposable");
    }

    @Test
    void productionAndFixtureInputNamesRemainValid(@TempDir Path root) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path segment = Files.writeString(staging.resolve("seg-run-0.pageseg"), "segment");
        Path fixture = Files.writeString(staging.resolve("fixture-0.pageseg"), "fixture");

        StagingReconciliation reconciliation = StagingReconciliation.fromPaths(
                staging, List.of(segment, fixture));

        assertThat(reconciliation.ownedPaths()).containsExactly(segment, fixture);
    }

    private static DatasetPublisher publisher() {
        return publisher(PublicationStepHook.NO_OP);
    }

    private static DatasetPublisher publisher(PublicationStepHook hook) {
        SortRun run = new SortRun(
                SortConfigs.base(), new ListEntryComparator(), DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY);
        return new DatasetPublisher(
                run, hook, LoggerFactory.getLogger(StagingReconciliationTest.class));
    }

    private static BasicFileAttributes withoutFileKey(BasicFileAttributes delegate) {
        return new BasicFileAttributes() {
            @Override
            public FileTime lastModifiedTime() {
                return delegate.lastModifiedTime();
            }

            @Override
            public FileTime lastAccessTime() {
                return delegate.lastAccessTime();
            }

            @Override
            public FileTime creationTime() {
                return delegate.creationTime();
            }

            @Override
            public boolean isRegularFile() {
                return delegate.isRegularFile();
            }

            @Override
            public boolean isDirectory() {
                return delegate.isDirectory();
            }

            @Override
            public boolean isSymbolicLink() {
                return delegate.isSymbolicLink();
            }

            @Override
            public boolean isOther() {
                return delegate.isOther();
            }

            @Override
            public long size() {
                return delegate.size();
            }

            @Override
            public Object fileKey() {
                return null;
            }
        };
    }
}
