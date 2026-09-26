/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.function.BiFunction;

/** Validated, immutable serve settings assembled at the CLI boundary. */
public record ServeConfig(Path fixture, String host, int port, String bucket,
                          ServingMode servingMode, int parquetConnections, int maxConcurrentRequests,
                          Set<Protocol> protocols, String azureAccount,
                          long responseBufferBudget, int maxResponseBytes,
                          Duration stopTimeout, Duration idleTimeout, Duration writeTimeout,
                          String advertisedHost,
                          BiFunction<S3ListRequest, S3ListResult, Duration> s3Latency) {
    public static final long DEFAULT_RESPONSE_BUFFER_BUDGET = 256L * 1024 * 1024;
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

    public ServeConfig {
        protocols = Set.copyOf(protocols);
        if (protocols.isEmpty() || protocols.contains(Protocol.REPLAY)) {
            throw new IllegalArgumentException("--protocols must name at least one protocol");
        }
        ReplayServer.validateMaxConcurrentRequests(maxConcurrentRequests);
        if (maxConcurrentRequests > Integer.MAX_VALUE / 2) {
            throw new IllegalArgumentException("max concurrent requests exceeds response-count range");
        }
        if (responseBufferBudget <= 0 || maxResponseBytes <= 0
                || maxResponseBytes > responseBufferBudget) {
            throw new IllegalArgumentException("invalid response buffer bounds");
        }
        if (stopTimeout.isZero() || stopTimeout.isNegative() || idleTimeout.isZero()
                || idleTimeout.isNegative() || writeTimeout.isZero() || writeTimeout.isNegative()) {
            throw new IllegalArgumentException("serve timeouts must be positive");
        }
        if (protocols.contains(Protocol.S3) && protocols.contains(Protocol.GCS)
                && "storage".equals(bucket)) {
            throw new IllegalArgumentException("S3 bucket storage conflicts with enabled GCS route");
        }
        if (!protocols.contains(Protocol.AZURE) && azureAccount != null) {
            throw new IllegalArgumentException("--azure-account requires azure in --protocols");
        }
        if (protocols.contains(Protocol.AZURE)) {
            azureAccount = azureAccount == null ? "replay" : azureAccount;
            if (!azureAccount.matches("[a-z0-9]{3,24}")) {
                throw new IllegalArgumentException("invalid Azure account name");
            }
            if (bucket == null || !bucket.matches("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
                    || bucket.contains("--")) {
                throw new IllegalArgumentException("bucket cannot be exposed as an Azure container");
            }
            if (wildcardHost(host)
                    && (advertisedHost == null || advertisedHost.isBlank())) {
                throw new IllegalArgumentException("--advertised-host is required for wildcard Azure binds");
            }
        }
        if (advertisedHost != null && !validAdvertisedHost(advertisedHost)) {
            throw new IllegalArgumentException("--advertised-host must be a concrete host without scheme or port");
        }
        if (protocols.contains(Protocol.GCS) && !validGcsBucket(bucket)) {
            throw new IllegalArgumentException("bucket cannot be exposed as a GCS bucket");
        }
    }

    public int maxResponses() { return maxConcurrentRequests * 2; }

    private static boolean wildcardHost(String host) {
        if ("0.0.0.0".equals(host)) return true;
        if (host == null || !host.contains(":")) return false;
        try {
            String literal = host.startsWith("[") && host.endsWith("]")
                    ? host.substring(1, host.length() - 1) : host;
            return java.net.InetAddress.getByName(literal).isAnyLocalAddress();
        } catch (java.net.UnknownHostException e) {
            return false;
        }
    }

    private static boolean validAdvertisedHost(String value) {
        if (value.isBlank() || value.contains("/") || value.contains("@")
                || value.contains("?") || value.contains("%")) {
            return false;
        }
        String host = value.startsWith("[") && value.endsWith("]")
                ? value.substring(1, value.length() - 1) : value;
        if (host.contains(":")) {
            if (!host.matches("[0-9a-fA-F:]+") || host.chars().filter(c -> c == ':').count() < 2) {
                return false;
            }
            try {
                java.net.InetAddress address = java.net.InetAddress.getByName(host);
                return address instanceof java.net.Inet6Address && !address.isAnyLocalAddress()
                        && !host.toLowerCase(java.util.Locale.ROOT).contains("ffff:");
            } catch (java.net.UnknownHostException e) {
                return false;
            }
        }
        if (value.contains("[") || value.contains("]") || value.length() > 253) {
            return false;
        }
        if ("0.0.0.0".equals(value)) {
            return false;
        }
        if (value.matches("[0-9.]+") && value.contains(".")) {
            String[] octets = value.split("\\.", -1);
            if (octets.length != 4) return false;
            for (String octet : octets) {
                if (!octet.matches("[0-9]{1,3}") || (octet.length() > 1 && octet.startsWith("0"))
                        || Integer.parseInt(octet) > 255) {
                    return false;
                }
            }
        }
        for (String label : value.split("\\.", -1)) {
            if (label.isEmpty() || label.length() > 63
                    || !label.matches("[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?")) {
                return false;
            }
        }
        return true;
    }

    private static boolean validGcsBucket(String name) {
        if (name == null || name.length() < 3 || name.length() > (name.contains(".") ? 222 : 63)
                || !name.matches("[a-z0-9][a-z0-9._-]*[a-z0-9]")
                || name.startsWith("goog")) {
            return false;
        }
        String normalized = name.replace('0', 'o').replace('1', 'l');
        if (normalized.contains("google")) {
            return false;
        }
        for (String label : name.split("\\.", -1)) {
            if (label.isEmpty() || label.length() > 63) {
                return false;
            }
        }
        String[] parts = name.split("\\.", -1);
        if (parts.length == 4) {
            boolean ipv4 = true;
            for (String part : parts) {
                if (!part.matches("[0-9]{1,3}")) {
                    ipv4 = false;
                    break;
                }
                if (Integer.parseInt(part) > 255) {
                    ipv4 = false;
                    break;
                }
            }
            if (ipv4) {
                return false;
            }
        }
        return true;
    }

    public static ServeConfig s3Defaults(Path fixture, String host, int port, String bucket,
                                         ServingMode mode, int connections, int requests,
                                         BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        return new ServeConfig(fixture, host, port, bucket, mode, connections, requests,
                Set.of(Protocol.S3), null, DEFAULT_RESPONSE_BUFFER_BUDGET, DEFAULT_MAX_RESPONSE_BYTES,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null, latency);
    }
}
