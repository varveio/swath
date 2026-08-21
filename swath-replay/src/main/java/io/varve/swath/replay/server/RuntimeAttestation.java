/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/** Reads the resource limits visible to this process from Linux procfs and cgroup v2. */
final class RuntimeAttestation {

    private static final String SCHEMA_VERSION = "runtime-attestation-v1";

    private final Path procSelfCgroup;
    private final Path procSelfStatus;
    private final Path cgroupMount;

    RuntimeAttestation(Path procSelfCgroup, Path procSelfStatus, Path cgroupMount) {
        this.procSelfCgroup = procSelfCgroup;
        this.procSelfStatus = procSelfStatus;
        this.cgroupMount = cgroupMount;
    }

    static RuntimeAttestation system() {
        return new RuntimeAttestation(
                Path.of("/proc/self/cgroup"),
                Path.of("/proc/self/status"),
                Path.of("/sys/fs/cgroup"));
    }

    String render() {
        DirectoryResult resolved = resolveCgroupDirectory();
        Observation cpuset;
        Observation memoryMax;
        Observation memorySwapMax;
        if (resolved.directory() == null) {
            cpuset = Observation.error(resolved.error());
            memoryMax = Observation.error(resolved.error());
            memorySwapMax = Observation.error(resolved.error());
        } else {
            cpuset = readText(resolved.directory().resolve("cpuset.cpus.effective"), false);
            memoryMax = readLimit(resolved.directory().resolve("memory.max"));
            memorySwapMax = readLimit(resolved.directory().resolve("memory.swap.max"));
        }
        Observation statusCpuset = readStatusCpuList();

        return new StringBuilder(512)
                .append("{\"schema_version\":\"").append(SCHEMA_VERSION)
                .append("\",\"cgroup_v2\":{\"directory\":")
                .append(resolved.directory() == null
                        ? "null"
                        : quote(resolved.directory().toString()))
                .append(",\"cpuset_cpus_effective\":").append(cpuset.json())
                .append(",\"memory_max\":").append(memoryMax.json())
                .append(",\"memory_swap_max\":").append(memorySwapMax.json())
                .append("},\"proc_self_status\":{\"cpus_allowed_list\":")
                .append(statusCpuset.json())
                .append("}}")
                .toString();
    }

    private DirectoryResult resolveCgroupDirectory() {
        final String contents;
        try {
            contents = Files.readString(procSelfCgroup);
        } catch (NoSuchFileException e) {
            return DirectoryResult.error("cgroup v2 membership file is missing: " + procSelfCgroup);
        } catch (IOException | SecurityException e) {
            return DirectoryResult.error("cgroup v2 membership file is unreadable: "
                    + procSelfCgroup + " (" + errorDetail(e) + ")");
        }

        for (String line : contents.lines().toList()) {
            if (!line.startsWith("0::")) {
                continue;
            }
            String membership = line.substring(3);
            if (!membership.startsWith("/")) {
                return DirectoryResult.error("invalid cgroup v2 membership path: " + membership);
            }
            Path mount = cgroupMount.toAbsolutePath().normalize();
            Path directory = mount.resolve(membership.substring(1)).normalize();
            if (!directory.startsWith(mount)) {
                return DirectoryResult.error("cgroup v2 membership escapes its mount: " + membership);
            }
            return new DirectoryResult(directory, null);
        }
        return DirectoryResult.error("cgroup v2 membership entry 0:: is missing: " + procSelfCgroup);
    }

    private Observation readStatusCpuList() {
        final String contents;
        try {
            contents = Files.readString(procSelfStatus);
        } catch (NoSuchFileException e) {
            return Observation.error("proc status file is missing: " + procSelfStatus);
        } catch (IOException | SecurityException e) {
            return Observation.error("proc status file is unreadable: "
                    + procSelfStatus + " (" + errorDetail(e) + ")");
        }
        for (String line : contents.lines().toList()) {
            if (line.startsWith("Cpus_allowed_list:")) {
                String value = line.substring("Cpus_allowed_list:".length()).trim();
                return value.isEmpty()
                        ? Observation.error("Cpus_allowed_list is empty in " + procSelfStatus)
                        : Observation.string(value);
            }
        }
        return Observation.error("Cpus_allowed_list is missing from " + procSelfStatus);
    }

    private static Observation readLimit(Path path) {
        Observation raw = readText(path, true);
        if (raw.error() != null || "max".equals(raw.value())) {
            return raw;
        }
        try {
            BigInteger value = new BigInteger(raw.value());
            if (value.signum() < 0) {
                return Observation.error("invalid negative cgroup limit in " + path + ": " + raw.value());
            }
            return Observation.number(value.toString());
        } catch (NumberFormatException e) {
            return Observation.error("invalid cgroup limit in " + path + ": " + raw.value());
        }
    }

    private static Observation readText(Path path, boolean allowMax) {
        final String value;
        try {
            value = Files.readString(path).trim();
        } catch (NoSuchFileException e) {
            return Observation.error("missing: " + path);
        } catch (IOException | SecurityException e) {
            return Observation.error("unreadable: " + path + " (" + errorDetail(e) + ")");
        }
        if (value.isEmpty()) {
            return Observation.error("empty: " + path);
        }
        if (allowMax && "max".equals(value)) {
            return Observation.string(value);
        }
        return Observation.string(value);
    }

    private static String errorDetail(Exception exception) {
        String message = exception.getMessage();
        return exception.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static String quote(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append("\\u%04x".formatted((int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }

    private record DirectoryResult(Path directory, String error) {
        private static DirectoryResult error(String error) {
            return new DirectoryResult(null, error);
        }
    }

    private record Observation(String value, String error, boolean number) {
        private static Observation string(String value) {
            return new Observation(value, null, false);
        }

        private static Observation number(String value) {
            return new Observation(value, null, true);
        }

        private static Observation error(String error) {
            return new Observation(null, error, false);
        }

        private String json() {
            String renderedValue = value == null ? "null" : number ? value : quote(value);
            return "{\"value\":" + renderedValue + ",\"error\":"
                    + (error == null ? "null" : quote(error)) + "}";
        }
    }
}
