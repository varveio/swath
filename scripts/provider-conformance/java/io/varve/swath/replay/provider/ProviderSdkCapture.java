/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.EncodingType;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Runs one unmodified official-SDK auto-paginated listing walk and records every HTTP attempt
 * in the raw capture shape {@code evidence.py sanitize} consumes.
 *
 * <p>Usage: {@code ProviderSdkCapture --options OPTIONS.json --output RAW.json}. OPTIONS is a
 * UTF-8 JSON object whose fields are the {@link Options} components; enum values are lowercase
 * with hyphens. Namespace identity (bucket, projectId, endpoint, container) may instead come from
 * {@value #IDENTITY_ENV} so it is never written to a file; Azure SAS credentials are read from
 * {@value #SAS_ENV}; GCS ADC credentials are read from the file named by {@value #ADC_ENV}; S3
 * session credentials are read from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
 * AWS_SESSION_TOKEN; and {@value Redactor#ENV} names private values replaced before anything is
 * recorded.
 * Exit status: 0 complete, 3 incomplete (the capture is still written), 2 usage error.
 */
public final class ProviderSdkCapture {
    public static final String SCHEMA_VERSION = "provider-sdk-capture-raw-v1";
    public static final String SAS_ENV = "AZURE_CONTAINER_SAS";
    public static final String ACCOUNT_ENV = "AZURE_STORAGE_ACCOUNT";
    public static final String CONTAINER_ENV = "AZURE_CONTAINER";
    /** JSON object with any of bucket, projectId, endpoint and container. */
    public static final String IDENTITY_ENV = "SWATH_CAPTURE_IDENTITY";
    public static final String ADC_ENV = "GOOGLE_APPLICATION_CREDENTIALS";
    static final Set<String> IDENTITY_FIELDS = Set.of("bucket", "projectId", "endpoint", "container");
    public static final String DEFAULT_S3_REGION = "us-east-1";
    /** The one S3 encoding the profile names; anything else is refused before an attempt. */
    public static final String S3_URL_ENCODING = "url";
    /** Aggregate response-byte cap when none is given; the campaign passes its remaining allowance. */
    public static final long DEFAULT_MAX_TOTAL_RESPONSE_BYTES = 512L * 1024 * 1024;
    static final int EXIT_COMPLETE = 0;
    static final int EXIT_USAGE = 2;
    static final int EXIT_INCOMPLETE = 3;
    private static final int MAX_SDK_ATTEMPTS = 3;
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private ProviderSdkCapture() {
    }

    /** Official client identity: the product token the SDK sends and the jar carrying it. */
    public enum Provider {
        GCS("gcloud-java", "com.google.cloud", "google-cloud-storage"),
        AZURE("azsdk-java-azure-storage-blob", "com.azure", "azure-storage-blob"),
        S3("aws-sdk-java", "software.amazon.awssdk", "s3");

        private final String product;
        private final String group;
        private final String artifact;

        Provider(String product, String group, String artifact) {
            this.product = product;
            this.group = group;
            this.artifact = artifact;
        }

        public String product() { return product; }

        /** The version of the SDK jar actually loaded, from its Maven metadata. */
        public String sdkVersion() {
            String resource = "META-INF/maven/" + group + "/" + artifact + "/pom.properties";
            try (InputStream input = ProviderSdkCapture.class.getClassLoader().getResourceAsStream(resource)) {
                if (input == null) {
                    throw new IllegalStateException("SDK metadata not found: " + resource);
                }
                Properties properties = new Properties();
                properties.load(input);
                String version = properties.getProperty("version");
                if (version == null || version.isBlank()) {
                    throw new IllegalStateException("SDK metadata has no version: " + resource);
                }
                return version;
            } catch (IOException e) {
                throw new IllegalStateException("cannot read SDK metadata " + resource, e);
            }
        }
    }

    /** {@code env} is S3 session credentials from the process environment only. */
    public enum Credentials { ADC, NONE, SAS_ENV, ENV }

    public enum Walk { FLAT, HIERARCHY }

    public enum Status { COMPLETE, INCOMPLETE }

    public record Entry(String name, boolean prefix) { }

    /**
     * One walk. GCS uses {@code bucket}, {@code projectId}, {@code startOffset}/{@code endOffset};
     * Azure uses {@code container}, {@code serviceVersion}, {@code startFrom} and
     * {@code clientRequestIdPrefix}; S3 uses {@code bucket}, {@code region}, {@code startAfter},
     * {@code encodingType} and {@code fetchOwner}. A null GCS or S3 endpoint means the SDK's
     * default host (an S3 endpoint override switches to path-style addressing); a null Azure
     * endpoint is derived from {@value #ACCOUNT_ENV} for SAS credentials.
     */
    public record Options(Provider provider, String endpoint, String bucket, String projectId,
                          String container, String serviceVersion, Credentials credentials, Walk walk,
                          String prefix, String delimiter, String startOffset, String endOffset,
                          String startFrom, Integer pageSize, int maxAttempts, long deadlineEpochMs,
                          int requestTimeoutSeconds, int maxPages, int maxEntries, int maxResponseBytes,
                          long maxTotalResponseBytes, int retryAttempts, String clientRequestIdPrefix,
                          String region, String startAfter, String encodingType, Boolean fetchOwner) {
        public Options {
            require(provider != null && credentials != null && walk != null, "provider, credentials and walk are required");
            require(maxAttempts >= 0 && maxPages >= 1 && maxEntries >= 0 && maxResponseBytes >= 1
                    && maxTotalResponseBytes >= 0, "attempt, page, entry and response bounds must be set");
            require(requestTimeoutSeconds >= 1 && requestTimeoutSeconds <= AttemptGate.MAX_REQUEST_TIMEOUT_SECONDS,
                    "requestTimeoutSeconds must be 1.." + AttemptGate.MAX_REQUEST_TIMEOUT_SECONDS);
            require(retryAttempts >= 1 && retryAttempts <= MAX_SDK_ATTEMPTS,
                    "retryAttempts must be 1.." + MAX_SDK_ATTEMPTS);
            require(pageSize == null || pageSize >= 1, "pageSize must be positive");
            require(provider == Provider.S3 || region == null && startAfter == null && encodingType == null
                    && fetchOwner == null, "region, startAfter, encodingType and fetchOwner are S3 options");
            if (provider == Provider.GCS) {
                require(bucket != null && !bucket.isBlank() && projectId != null && !projectId.isBlank(),
                        "GCS walks need bucket and an explicit projectId");
                require(credentials == Credentials.ADC || credentials == Credentials.NONE,
                        "GCS credentials are adc or none");
                require(startFrom == null && serviceVersion == null && clientRequestIdPrefix == null,
                        "startFrom, serviceVersion and clientRequestIdPrefix are Azure options");
            } else if (provider == Provider.S3) {
                require(bucket != null && !bucket.isBlank(), "S3 walks need a bucket");
                require(credentials == Credentials.ENV || credentials == Credentials.NONE,
                        "S3 credentials are env or none");
                require(projectId == null && container == null && serviceVersion == null
                        && clientRequestIdPrefix == null && startOffset == null && endOffset == null
                        && startFrom == null, "S3 walks take only S3 options");
                require(encodingType == null || S3_URL_ENCODING.equals(encodingType),
                        "encodingType must be " + S3_URL_ENCODING);
            } else {
                require(serviceVersion != null && !serviceVersion.isBlank(), "Azure walks need an explicit serviceVersion");
                require(credentials == Credentials.SAS_ENV || credentials == Credentials.NONE,
                        "Azure credentials are sas-env or none");
                require(clientRequestIdPrefix != null, "Azure walks need clientRequestIdPrefix");
                require(startOffset == null && endOffset == null && projectId == null,
                        "startOffset, endOffset and projectId are GCS options");
                require(endpoint != null || credentials == Credentials.SAS_ENV,
                        "anonymous Azure walks need an endpoint");
            }
            require(walk == Walk.HIERARCHY || delimiter == null, "delimiter applies to hierarchy walks");
        }

        public static Builder builder(Provider provider) {
            return new Builder(provider);
        }
    }

    /** Builder with the live campaign's conservative defaults. */
    public static final class Builder {
        private final Provider provider;
        private String endpoint;
        private String bucket;
        private String projectId;
        private String container;
        private String serviceVersion;
        private Credentials credentials = Credentials.NONE;
        private Walk walk = Walk.FLAT;
        private String prefix;
        private String delimiter;
        private String startOffset;
        private String endOffset;
        private String startFrom;
        private Integer pageSize;
        private int maxAttempts;
        private long deadlineEpochMs;
        private int requestTimeoutSeconds = AttemptGate.MAX_REQUEST_TIMEOUT_SECONDS;
        private int maxPages = 64;
        private int maxEntries = 10_000;
        private int maxResponseBytes = ExchangeRecorder.DEFAULT_MAX_RESPONSE_BYTES;
        private long maxTotalResponseBytes = DEFAULT_MAX_TOTAL_RESPONSE_BYTES;
        private int retryAttempts = MAX_SDK_ATTEMPTS;
        private String clientRequestIdPrefix;
        private String region;
        private String startAfter;
        private String encodingType;
        private Boolean fetchOwner;

        private Builder(Provider provider) { this.provider = provider; }

        public Builder endpoint(String value) { endpoint = value; return this; }
        public Builder bucket(String value) { bucket = value; return this; }
        public Builder projectId(String value) { projectId = value; return this; }
        public Builder container(String value) { container = value; return this; }
        public Builder serviceVersion(String value) { serviceVersion = value; return this; }
        public Builder credentials(Credentials value) { credentials = value; return this; }
        public Builder walk(Walk value) { walk = value; return this; }
        public Builder prefix(String value) { prefix = value; return this; }
        public Builder delimiter(String value) { delimiter = value; return this; }
        public Builder startOffset(String value) { startOffset = value; return this; }
        public Builder endOffset(String value) { endOffset = value; return this; }
        public Builder startFrom(String value) { startFrom = value; return this; }
        public Builder pageSize(Integer value) { pageSize = value; return this; }
        public Builder maxAttempts(int value) { maxAttempts = value; return this; }
        public Builder deadlineEpochMs(long value) { deadlineEpochMs = value; return this; }
        public Builder requestTimeoutSeconds(int value) { requestTimeoutSeconds = value; return this; }
        public Builder maxPages(int value) { maxPages = value; return this; }
        public Builder maxEntries(int value) { maxEntries = value; return this; }
        public Builder maxResponseBytes(int value) { maxResponseBytes = value; return this; }
        public Builder maxTotalResponseBytes(long value) { maxTotalResponseBytes = value; return this; }
        public Builder retryAttempts(int value) { retryAttempts = value; return this; }
        public Builder clientRequestIdPrefix(String value) { clientRequestIdPrefix = value; return this; }
        public Builder region(String value) { region = value; return this; }
        public Builder startAfter(String value) { startAfter = value; return this; }
        public Builder encodingType(String value) { encodingType = value; return this; }
        public Builder fetchOwner(Boolean value) { fetchOwner = value; return this; }

        public Options build() {
            return new Options(provider, endpoint, bucket, projectId, container, serviceVersion, credentials,
                    walk, prefix, delimiter, startOffset, endOffset, startFrom, pageSize, maxAttempts,
                    deadlineEpochMs, requestTimeoutSeconds, maxPages, maxEntries, maxResponseBytes,
                    maxTotalResponseBytes, retryAttempts, clientRequestIdPrefix, region, startAfter, encodingType,
                    fetchOwner);
        }
    }

    /**
     * {@code storageAttempts} and {@code authAttempts} include {@code transportResends}, the
     * resends observed below the SDK (HTTPS connections beyond the counted attempts).
     */
    public record Result(Provider provider, Status status, String reason, int storageAttempts, int authAttempts,
                         int transportResends, long responseBytes, String sdkVersion, String javaVersion,
                         List<Entry> entries, List<Map<String, Object>> steps) {
        public Map<String, Object> toJson() {
            Map<String, Object> sdk = new LinkedHashMap<>();
            sdk.put("product", provider.product());
            sdk.put("version", sdkVersion);
            List<Map<String, Object>> listed = new ArrayList<>();
            for (Entry entry : entries) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("name", entry.name());
                item.put("prefix", entry.prefix());
                listed.add(item);
            }
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("schema_version", SCHEMA_VERSION);
            json.put("provider", provider.name().toLowerCase(Locale.ROOT));
            json.put("status", status.name());
            json.put("reason", reason);
            json.put("storage_attempts", storageAttempts);
            json.put("auth_attempts", authAttempts);
            json.put("transport_resends", transportResends);
            json.put("response_bytes", responseBytes);
            json.put("sdk", sdk);
            json.put("java_version", javaVersion);
            json.put("entries", listed);
            json.put("steps", steps);
            return json;
        }
    }

    /**
     * Runs one bounded walk, redacting the values named by {@value Redactor#ENV}. Option and
     * SDK-version errors throw before any attempt.
     */
    public static Result run(Options options) {
        return run(options, Redactor.fromEnvironment(System.getenv(Redactor.ENV)));
    }

    static Result run(Options options, Redactor redactor) {
        disableHiddenTransportResends();
        String sdkVersion = options.provider().sdkVersion();
        BlobServiceVersion azureVersion = options.provider() == Provider.AZURE
                ? azureServiceVersion(options.serviceVersion(), sdkVersion) : null;
        AttemptGate gate = new AttemptGate(options.maxAttempts(), options.deadlineEpochMs(),
                options.requestTimeoutSeconds(), options.maxTotalResponseBytes());
        ExchangeRecorder recorder = new ExchangeRecorder(options.maxPages(), options.maxResponseBytes(), redactor);
        List<Entry> entries = new ArrayList<>();
        Status status = Status.COMPLETE;
        String reason = null;
        try {
            switch (options.provider()) {
                case GCS -> walkGcs(options, gate, recorder, entries);
                case AZURE -> walkAzure(options, azureVersion, gate, recorder, entries);
                case S3 -> walkS3(options, gate, recorder, entries);
            }
        } catch (RuntimeException | IOException e) {
            status = Status.INCOMPLETE;
            reason = redactor.apply(classify(e));
        }
        if (status == Status.COMPLETE && gate.pastDeadline()) {
            status = Status.INCOMPLETE;
            reason = "bound:deadline";
        }
        List<Entry> listed = new ArrayList<>();
        for (Entry entry : entries) listed.add(new Entry(redactor.apply(entry.name()), entry.prefix()));
        return new Result(options.provider(), status, reason, gate.attempts(AttemptGate.Kind.STORAGE),
                gate.attempts(AttemptGate.Kind.AUTH), gate.resends(), gate.responseBytes(), sdkVersion,
                System.getProperty("java.version"), List.copyOf(listed), recorder.steps());
    }

    /**
     * Turns off the resends the JDK makes below the gate where a property allows it. These are
     * read once, when the JDK's HTTP classes initialize, so they take effect in a fresh driver
     * process. HttpURLConnection still resends an idempotent request once after a read failure;
     * over HTTPS that resend opens a counted connection, and the campaign also charges each
     * reported attempt twice as an upper bound for any resend the JDK makes below the gate.
     */
    static void disableHiddenTransportResends() {
        System.setProperty("http.keepAlive", "false");
        System.setProperty("jdk.httpclient.disableRetryConnect", "true");
    }

    private static void walkGcs(Options options, AttemptGate gate, ExchangeRecorder recorder,
                                List<Entry> entries) throws IOException {
        GcsGatedTransport storageTransport = GcsGatedTransport.storage(gate, recorder);
        GcsGatedTransport authTransport = GcsGatedTransport.auth(gate, recorder);
        com.google.auth.Credentials credentials;
        if (options.credentials() == Credentials.ADC) {
            // An explicit credential file: no discovery, so no metadata-server probe.
            try (InputStream adc = Files.newInputStream(Path.of(requiredEnv(ADC_ENV)))) {
                credentials = GoogleCredentials.fromStream(adc, () -> authTransport);
            }
        } else {
            credentials = NoCredentials.getInstance();
        }
        StorageOptions.Builder builder = StorageOptions.http()
                .setProjectId(options.projectId())
                .setCredentials(credentials)
                .setTransportOptions(HttpTransportOptions.newBuilder()
                        .setHttpTransportFactory(() -> storageTransport)
                        .setConnectTimeout(gate.requestTimeoutMillis())
                        .setReadTimeout(gate.requestTimeoutMillis())
                        .build())
                .setRetrySettings(StorageOptions.getDefaultRetrySettings().toBuilder()
                        .setMaxAttempts(options.retryAttempts()).build());
        if (options.endpoint() != null) {
            builder.setHost(options.endpoint());
        }
        List<Storage.BlobListOption> list = new ArrayList<>();
        if (options.pageSize() != null) list.add(Storage.BlobListOption.pageSize(options.pageSize()));
        if (options.prefix() != null) list.add(Storage.BlobListOption.prefix(options.prefix()));
        if (options.startOffset() != null) list.add(Storage.BlobListOption.startOffset(options.startOffset()));
        if (options.endOffset() != null) list.add(Storage.BlobListOption.endOffset(options.endOffset()));
        if (options.walk() == Walk.HIERARCHY) {
            list.add(options.delimiter() == null || options.delimiter().equals("/")
                    ? Storage.BlobListOption.currentDirectory()
                    : Storage.BlobListOption.delimiter(options.delimiter()));
        }
        try (Storage storage = builder.build().getService()) {
            for (Blob blob : storage.list(options.bucket(), list.toArray(Storage.BlobListOption[]::new)).iterateAll()) {
                add(entries, new Entry(blob.getName(), blob.isDirectory()), options.maxEntries());
            }
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static void walkAzure(Options options, BlobServiceVersion version, AttemptGate gate,
                                  ExchangeRecorder recorder, List<Entry> entries) {
        String endpoint = options.endpoint() != null ? options.endpoint()
                : "https://" + requiredEnv(ACCOUNT_ENV) + ".blob.core.windows.net";
        String containerName = options.container() != null ? options.container() : requiredEnv(CONTAINER_ENV);
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .serviceVersion(version)
                .httpClient(new AzureGatedHttpClient(gate, recorder))
                .retryOptions(new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, options.retryAttempts(),
                        gate.requestTimeoutSeconds(), null, null, null))
                .addPolicy(new ClientRequestIdPolicy(options.clientRequestIdPrefix()));
        if (options.credentials() == Credentials.SAS_ENV) {
            builder.sasToken(requiredEnv(SAS_ENV));
        }
        BlobContainerClient container = builder.buildClient().getBlobContainerClient(containerName);
        ListBlobsOptions list = new ListBlobsOptions();
        if (options.pageSize() != null) list.setMaxResultsPerPage(options.pageSize());
        if (options.prefix() != null) list.setPrefix(options.prefix());
        if (options.startFrom() != null) list.setStartFrom(options.startFrom());
        Iterator<BlobItem> items = options.walk() == Walk.FLAT
                ? container.listBlobs(list, null).iterator()
                : container.listBlobsByHierarchy(options.delimiter() == null ? "/" : options.delimiter(),
                        list, null).iterator();
        while (items.hasNext()) {
            BlobItem item = items.next();
            add(entries, new Entry(item.getName(), Boolean.TRUE.equals(item.isPrefix())), options.maxEntries());
        }
    }

    /**
     * The SDK's own ListObjectsV2 paginator, unmodified: the SDK follows NextContinuationToken.
     * Region and credentials are explicit, so nothing is discovered from instance metadata.
     */
    private static void walkS3(Options options, AttemptGate gate, ExchangeRecorder recorder, List<Entry> entries) {
        AwsCredentialsProvider credentials = options.credentials() == Credentials.ENV
                ? EnvironmentVariableCredentialsProvider.create() : AnonymousCredentialsProvider.create();
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(options.region() == null ? DEFAULT_S3_REGION : options.region()))
                .credentialsProvider(credentials)
                .httpClient(new S3GatedHttpClient(gate, recorder))
                .overrideConfiguration(configuration -> configuration.retryStrategy(
                        AwsRetryStrategy.standardRetryStrategy().toBuilder()
                                .maxAttempts(options.retryAttempts()).build()));
        // Path-style on both endpoints, so the live walk and the replay walk share one request shape
        // (replay is addressed by path); the paginator itself is unmodified.
        builder.forcePathStyle(true);
        if (options.endpoint() != null) {
            builder.endpointOverride(URI.create(options.endpoint()));
        }
        ListObjectsV2Request.Builder request = ListObjectsV2Request.builder().bucket(options.bucket());
        if (options.prefix() != null) request.prefix(options.prefix());
        if (options.walk() == Walk.HIERARCHY) request.delimiter(options.delimiter() == null ? "/" : options.delimiter());
        if (options.startAfter() != null) request.startAfter(options.startAfter());
        if (options.pageSize() != null) request.maxKeys(options.pageSize());
        if (options.encodingType() != null) request.encodingType(EncodingType.URL);
        if (options.fetchOwner() != null) request.fetchOwner(options.fetchOwner());
        try (S3Client s3 = builder.build()) {
            for (ListObjectsV2Response page : s3.listObjectsV2Paginator(request.build())) {
                for (S3Object object : page.contents()) {
                    add(entries, new Entry(object.key(), false), options.maxEntries());
                }
                for (CommonPrefix prefix : page.commonPrefixes()) {
                    add(entries, new Entry(prefix.prefix(), true), options.maxEntries());
                }
            }
        }
    }

    private static void add(List<Entry> entries, Entry entry, int maxEntries) {
        if (entries.size() >= maxEntries) {
            throw new CaptureBoundException("entries");
        }
        entries.add(entry);
    }

    static BlobServiceVersion azureServiceVersion(String requested, String sdkVersion) {
        for (BlobServiceVersion candidate : BlobServiceVersion.values()) {
            if (candidate.getVersion().equals(requested)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("azure-storage-blob " + sdkVersion
                + " does not support service version " + requested + "; it is not substituted");
    }

    // Reasons carry status and service codes only: SDK messages can echo URLs or account names.
    private static String classify(Exception error) {
        CaptureBoundException bound = CaptureBoundException.find(error);
        if (bound != null) {
            return "bound:" + bound.bound();
        }
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof StorageException storage) {
                return "sdk_error:gcs:" + storage.getCode() + ":" + storage.getReason();
            }
            if (current instanceof BlobStorageException blob) {
                return "sdk_error:azure:" + blob.getStatusCode() + ":" + blob.getErrorCode();
            }
            if (current instanceof S3Exception s3) {
                return "sdk_error:s3:" + s3.statusCode() + ":"
                        + (s3.awsErrorDetails() == null ? null : s3.awsErrorDetails().errorCode());
            }
            if (current.getCause() == current) break;
        }
        return "error:" + error.getClass().getSimpleName();
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("environment variable " + name + " is required");
        }
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private static final Set<String> OPTION_FIELDS = Set.of("provider", "endpoint", "bucket", "projectId",
            "container", "serviceVersion", "credentials", "walk", "prefix", "delimiter", "startOffset",
            "endOffset", "startFrom", "pageSize", "maxAttempts", "deadlineEpochMs", "requestTimeoutSeconds",
            "maxPages", "maxEntries", "maxResponseBytes", "maxTotalResponseBytes", "retryAttempts",
            "clientRequestIdPrefix", "region", "startAfter", "encodingType", "fetchOwner");

    public static Options parseOptions(JsonNode json) {
        require(json.isObject(), "options must be a JSON object");
        json.fieldNames().forEachRemaining(field -> require(OPTION_FIELDS.contains(field), "unknown option " + field));
        Builder builder = Options.builder(enumValue(Provider.class, text(json, "provider")))
                .endpoint(text(json, "endpoint")).bucket(text(json, "bucket")).projectId(text(json, "projectId"))
                .container(text(json, "container")).serviceVersion(text(json, "serviceVersion"))
                .prefix(text(json, "prefix")).delimiter(text(json, "delimiter"))
                .startOffset(text(json, "startOffset")).endOffset(text(json, "endOffset"))
                .startFrom(text(json, "startFrom")).clientRequestIdPrefix(text(json, "clientRequestIdPrefix"))
                .region(text(json, "region")).startAfter(text(json, "startAfter"))
                .encodingType(text(json, "encodingType"));
        if (json.hasNonNull("fetchOwner")) {
            require(json.get("fetchOwner").isBoolean(), "fetchOwner must be a boolean");
            builder.fetchOwner(json.get("fetchOwner").booleanValue());
        }
        if (json.hasNonNull("credentials")) builder.credentials(enumValue(Credentials.class, text(json, "credentials")));
        if (json.hasNonNull("walk")) builder.walk(enumValue(Walk.class, text(json, "walk")));
        if (json.hasNonNull("pageSize")) builder.pageSize(integer(json, "pageSize"));
        require(json.hasNonNull("maxAttempts") && json.hasNonNull("deadlineEpochMs"),
                "maxAttempts and deadlineEpochMs are required");
        builder.maxAttempts(integer(json, "maxAttempts"));
        require(json.get("deadlineEpochMs").canConvertToLong(), "deadlineEpochMs must be an integer");
        builder.deadlineEpochMs(json.get("deadlineEpochMs").longValue());
        if (json.hasNonNull("requestTimeoutSeconds")) builder.requestTimeoutSeconds(integer(json, "requestTimeoutSeconds"));
        if (json.hasNonNull("maxPages")) builder.maxPages(integer(json, "maxPages"));
        if (json.hasNonNull("maxEntries")) builder.maxEntries(integer(json, "maxEntries"));
        if (json.hasNonNull("maxResponseBytes")) builder.maxResponseBytes(integer(json, "maxResponseBytes"));
        if (json.hasNonNull("maxTotalResponseBytes")) {
            require(json.get("maxTotalResponseBytes").canConvertToLong()
                    && json.get("maxTotalResponseBytes").isIntegralNumber(), "maxTotalResponseBytes must be an integer");
            builder.maxTotalResponseBytes(json.get("maxTotalResponseBytes").longValue());
        }
        if (json.hasNonNull("retryAttempts")) builder.retryAttempts(integer(json, "retryAttempts"));
        return builder.build();
    }

    private static String text(JsonNode json, String field) {
        JsonNode value = json.get(field);
        if (value == null || value.isNull()) return null;
        require(value.isTextual(), field + " must be a string");
        return value.textValue();
    }

    private static int integer(JsonNode json, String field) {
        JsonNode value = json.get(field);
        require(value.canConvertToInt() && value.isIntegralNumber(), field + " must be an integer");
        return value.intValue();
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, String value) {
        require(value != null, type.getSimpleName() + " is required");
        try {
            return Enum.valueOf(type, value.toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + type.getSimpleName() + " " + value, e);
        }
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    static int execute(String[] args) {
        Path optionsFile = null;
        Path output = null;
        for (int i = 0; i < args.length; i++) {
            if (i + 1 >= args.length) return usage("missing value for " + args[i]);
            switch (args[i]) {
                case "--options" -> optionsFile = Path.of(args[++i]);
                case "--output" -> output = Path.of(args[++i]);
                default -> {
                    return usage("unknown argument " + args[i]);
                }
            }
        }
        if (optionsFile == null || output == null) return usage("--options and --output are required");
        Result result;
        try {
            JsonNode options = JSON.readTree(Files.readAllBytes(optionsFile));
            require(options.isObject(), "options must be a JSON object");
            mergeIdentity((ObjectNode) options, System.getenv(IDENTITY_ENV));
            result = run(parseOptions(options));
        } catch (IOException | IllegalArgumentException e) {
            return usage(e.getMessage());
        }
        try {
            Path parent = output.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Files.write(output, JSON.writeValueAsBytes(result.toJson()));
        } catch (IOException e) {
            System.err.println("cannot write capture: " + e.getMessage());
            return EXIT_USAGE;
        }
        System.out.println(result.status() + " storage_attempts=" + result.storageAttempts()
                + " auth_attempts=" + result.authAttempts() + " transport_resends=" + result.transportResends()
                + " response_bytes=" + result.responseBytes() + " steps=" + result.steps().size()
                + (result.reason() == null ? "" : " reason=" + result.reason()));
        return result.status() == Status.COMPLETE ? EXIT_COMPLETE : EXIT_INCOMPLETE;
    }

    /**
     * Adds namespace identity from {@value #IDENTITY_ENV}. A field present in both places must
     * agree; error messages name the field, never its value.
     */
    static void mergeIdentity(ObjectNode options, String identityJson) throws IOException {
        if (identityJson == null || identityJson.isBlank()) return;
        JsonNode identity = JSON.readTree(identityJson);
        require(identity.isObject(), IDENTITY_ENV + " must be a JSON object");
        for (var field : identity.properties()) {
            require(IDENTITY_FIELDS.contains(field.getKey()), IDENTITY_ENV + " has unknown field " + field.getKey());
            require(field.getValue().isTextual(), IDENTITY_ENV + " field " + field.getKey() + " must be a string");
            JsonNode existing = options.get(field.getKey());
            require(existing == null || existing.isNull() || existing.equals(field.getValue()),
                    "options and " + IDENTITY_ENV + " disagree on " + field.getKey());
            options.set(field.getKey(), field.getValue());
        }
    }

    private static int usage(String message) {
        System.err.println("usage: ProviderSdkCapture --options OPTIONS.json --output RAW.json: " + message);
        return EXIT_USAGE;
    }
}
