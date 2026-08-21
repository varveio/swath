/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RuntimeAttestationTest {

    @TempDir
    Path temp;

    @Test
    void readsTheProcessOwnNestedCgroupAndPreservesMaxExplicitly() throws Exception {
        Path proc = Files.createDirectories(temp.resolve("proc/self"));
        Path cgroup = Files.createDirectories(temp.resolve("cgroup/batch/job/server"));
        Files.writeString(proc.resolve("cgroup"), "11:memory:/legacy\n0::/batch/job/server\n");
        Files.writeString(proc.resolve("status"), "Name:\tjava\nCpus_allowed_list:\t2-3,8\n");
        Files.writeString(cgroup.resolve("cpuset.cpus.effective"), "2-3,8\n");
        Files.writeString(cgroup.resolve("memory.max"), "2147483648\n");
        Files.writeString(cgroup.resolve("memory.swap.max"), "max\n");

        String json = attestation(proc).render();

        assertThat(json).isEqualTo("{\"schema_version\":\"runtime-attestation-v1\","
                + "\"cgroup_v2\":{\"directory\":\"" + cgroup + "\","
                + "\"cpuset_cpus_effective\":{\"value\":\"2-3,8\",\"error\":null},"
                + "\"memory_max\":{\"value\":2147483648,\"error\":null},"
                + "\"memory_swap_max\":{\"value\":\"max\",\"error\":null}},"
                + "\"proc_self_status\":{\"cpus_allowed_list\":{\"value\":\"2-3,8\","
                + "\"error\":null}}}");
    }

    @Test
    void keepsMissingSwapAndMalformedMemoryAsExplicitErrors() throws Exception {
        Path proc = Files.createDirectories(temp.resolve("proc/self"));
        Path cgroup = Files.createDirectories(temp.resolve("cgroup/work"));
        Files.writeString(proc.resolve("cgroup"), "0::/work\n");
        Files.writeString(proc.resolve("status"), "Cpus_allowed_list:\t0-1\n");
        Files.writeString(cgroup.resolve("cpuset.cpus.effective"), "0-1\n");
        Files.writeString(cgroup.resolve("memory.max"), "not-a-limit\n");

        String json = attestation(proc).render();

        assertThat(json).contains("\"memory_max\":{\"value\":null,\"error\":"
                + "\"invalid cgroup limit in " + cgroup.resolve("memory.max") + ": not-a-limit\"}");
        assertThat(json).contains("\"memory_swap_max\":{\"value\":null,\"error\":"
                + "\"missing: " + cgroup.resolve("memory.swap.max") + "\"}");
    }

    @Test
    void refusesAnUnresolvedCgroupWithoutDroppingTheProcCrossCheck() throws Exception {
        Path proc = Files.createDirectories(temp.resolve("proc/self"));
        Files.writeString(proc.resolve("cgroup"), "2:cpu:/legacy-only\n");
        Files.writeString(proc.resolve("status"), "Cpus_allowed_list:\t4-7\n");

        String json = attestation(proc).render();

        assertThat(json).contains("\"directory\":null");
        assertThat(json).contains("\"cpuset_cpus_effective\":{\"value\":null,\"error\":"
                + "\"cgroup v2 membership entry 0:: is missing:");
        assertThat(json).contains("\"memory_max\":{\"value\":null,\"error\":"
                + "\"cgroup v2 membership entry 0:: is missing:");
        assertThat(json).contains("\"cpus_allowed_list\":{\"value\":\"4-7\",\"error\":null}");
    }

    @Test
    void reportsAMissingProcStatusFieldInsteadOfInventingACpuList() throws Exception {
        Path proc = Files.createDirectories(temp.resolve("proc/self"));
        Path cgroup = Files.createDirectories(temp.resolve("cgroup"));
        Files.writeString(proc.resolve("cgroup"), "0::/\n");
        Files.writeString(proc.resolve("status"), "Name:\tjava\n");
        Files.writeString(cgroup.resolve("cpuset.cpus.effective"), "0-7\n");
        Files.writeString(cgroup.resolve("memory.max"), "max\n");
        Files.writeString(cgroup.resolve("memory.swap.max"), "0\n");

        String json = attestation(proc).render();

        assertThat(json).contains("\"memory_max\":{\"value\":\"max\",\"error\":null}");
        assertThat(json).contains("\"memory_swap_max\":{\"value\":0,\"error\":null}");
        assertThat(json).contains("\"cpus_allowed_list\":{\"value\":null,\"error\":"
                + "\"Cpus_allowed_list is missing from " + proc.resolve("status") + "\"}");
    }

    private RuntimeAttestation attestation(Path proc) {
        return new RuntimeAttestation(proc.resolve("cgroup"), proc.resolve("status"), temp.resolve("cgroup"));
    }
}
