/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidUriException;
import io.varve.swath.output.OutputFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * The persisted subset of the resume classification — {@code IDENTITY} and {@code STICKY} options
 * only — as hand-written accessor rows. This is a <b>consolidation</b> of the
 * duplication that already exists: {@link io.varve.swath.checkpoint.SoftRestoreContext}'s nine
 * components plus {@link io.varve.swath.runtime.ArgsHashFields}' four live fields, previously
 * scattered and unclassified. {@code FREE} options need no row — their {@link Resume} annotation is
 * their whole footprint.
 *
 * <p>Names, arity and classification are owned elsewhere (picocli {@code CommandSpec} and the
 * {@link Resume} annotation); this registry owns only the persistence accessors and is guarded by
 * {@link ResumeRegistryDriftTest}. {@link #identitySpec(ListCommand)} is the computation seam that
 * persists and compares as {@code run_meta.identity_spec}.
 */
final class ResumeRegistry {

    private ResumeRegistry() {
    }

    /**
     * A single persisted classification accessor. {@code option} is the option's
     * longest name, or {@code null} for a column with no single {@code @Option} behind it (the
     * filter group, and the two {@code SoftRestoreContext} fields that have no CLI surface). The
     * {@code canonical}/{@code restore} pair reads and writes the value directly on a live
     * {@link ListCommand}, so no intermediate resolved-config type is needed.
     */
    record ResumeSpec(String option, String column, ResumeClass resumeClass, boolean restored,
                      Function<ListCommand, String> canonical, BiConsumer<ListCommand, String> restore) {
    }

    /** {@code args_hash} identity is re-supplied from the checkpoint's {@code RunIdentity} by
     * {@code ResumeCommand}, never soft-restored field-by-field, so these rows carry no restore. */
    private static final BiConsumer<ListCommand, String> NO_RESTORE = (lc, v) -> {
    };

    /** The seven filter options collapse to one stored column ({@code identityGroup}):
     * a per-option row would fight {@link FilterOptions#spec()}/{@link FilterSpecCodec}. */
    static final Set<String> FILTER_OPTIONS = Set.of("--include", "--exclude", "--min-size",
            "--max-size", "--modified-since", "--modified-until", "--storage-class");

    private static final ResumeSpec FILTER_SPEC = new ResumeSpec(null, "filter_spec",
            ResumeClass.IDENTITY, true,
            lc -> lc.filters.spec(),
            ResumeRegistry::restoreFilters);

    private static final List<ResumeSpec> ROWS = List.of(
            // args_hash identity (ArgsHashFields' four live fields) — restored=false: re-supplied
            // from RunIdentity, never soft-restored, and its four fields are also IDENTITY.
            new ResumeSpec(null, "scheme", ResumeClass.IDENTITY, false,
                    lc -> lc.uri == null ? "" : "s3", NO_RESTORE),
            new ResumeSpec("--endpoint-url", "endpoint", ResumeClass.IDENTITY, false,
                    lc -> nz(lc.connection.endpointUrl), NO_RESTORE),
            new ResumeSpec(null, "bucket", ResumeClass.IDENTITY, false,
                    ResumeRegistry::bucketOf, NO_RESTORE),
            new ResumeSpec(null, "prefix", ResumeClass.IDENTITY, false,
                    ResumeRegistry::prefixOf, NO_RESTORE),
            // RunKey identity fields compared today (ListCommand resume-safety check).
            new ResumeSpec("--format", "output_format", ResumeClass.IDENTITY, true,
                    ResumeRegistry::formatToken, ResumeRegistry::restoreFormat),
            FILTER_SPEC,
            new ResumeSpec("--sort", "sort_enabled", ResumeClass.IDENTITY, true,
                    lc -> Boolean.toString(lc.sorting.sort), (lc, v) -> lc.sorting.sort = parseBool(v)),
            // SoftRestoreContext identity components.
            new ResumeSpec("--output", "output", ResumeClass.IDENTITY, true,
                    lc -> nz(lc.output.destination), (lc, v) -> lc.output.destination = v),
            // restored=true: a bare resume must restore output_type — a run created with
            // --output-type is resumed without re-passing it, so an unrestored slot would spuriously
            // mismatch its identity_spec. destination_kind (below) carries the RESOLVED file/dir
            // decision; output_type is the raw override, restored so the fingerprint reproduces.
            new ResumeSpec("--output-type", "output_type", ResumeClass.IDENTITY, true,
                    lc -> nz(lc.output.outputType), (lc, v) -> lc.output.outputType = v),
            new ResumeSpec("--fetch-owner", "fetch_owner", ResumeClass.IDENTITY, true,
                    lc -> Boolean.toString(lc.connection.fetchOwner),
                    (lc, v) -> lc.connection.fetchOwner = parseBool(v)),
            new ResumeSpec(null, "raw_output", ResumeClass.IDENTITY, true,
                    lc -> Boolean.toString(lc.output.rawOutput),
                    (lc, v) -> lc.output.rawOutput = parseBool(v)),
            new ResumeSpec(null, "destination_kind", ResumeClass.IDENTITY, false,
                    lc -> lc.output.resolvedKind.name(), NO_RESTORE),
            // SoftRestoreContext sticky components (auth/region/billing — restored unless re-passed).
            new ResumeSpec("--no-sign-request", "no_sign_request", ResumeClass.STICKY, true,
                    lc -> Boolean.toString(lc.connection.noSignRequest),
                    (lc, v) -> lc.connection.noSignRequest = parseBool(v)),
            new ResumeSpec("--profile", "profile", ResumeClass.STICKY, true,
                    lc -> nz(lc.connection.profile), (lc, v) -> lc.connection.profile = v),
            new ResumeSpec("--region", "region", ResumeClass.STICKY, true,
                    lc -> nz(lc.connection.region), (lc, v) -> lc.connection.region = v),
            new ResumeSpec("--requester-pays", "request_payer", ResumeClass.STICKY, true,
                    lc -> Boolean.toString(lc.connection.requestPayerEnabled),
                    (lc, v) -> lc.connection.requestPayerEnabled = parseBool(v)));

    /** Every persisted accessor row (identity and sticky). */
    static List<ResumeSpec> rows() {
        return ROWS;
    }

    /** Whether {@code optionName} (a {@code CommandSpec} longest name) maps to a persisted row —
     * true for every filter option via the {@code filter_spec} group, else an exact row match. */
    static boolean hasPersistedRow(String optionName) {
        if (FILTER_OPTIONS.contains(optionName)) {
            return true;
        }
        for (ResumeSpec row : ROWS) {
            if (optionName.equals(row.option())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Walk the registry over a live {@link ListCommand} and return the canonical, label-tagged
     * identity string (the {@code run_meta.identity_spec} value — a readable label, not a
     * digest, so a refusal can name the changed option). Includes {@code IDENTITY} rows only; the
     * {@code args_hash} fields are deliberately covered too, so any of them changing also refuses
     * a resume rather than silently proceeding.
     */
    static String identitySpec(ListCommand command) {
        // Length-prefixed via the shared LengthPrefixedFields grammar so a value containing the
        // delimiters — a directory-dataset -o path with ';'/'=', or the whole filter_spec blob (itself a
        // FilterSpecCodec string full of ';'/'='/':') carried as one opaque value — cannot fragment and
        // hide a genuine change (a naive `column=value;...` join could let such a value mask a real
        // change, silently defeating the I1 commit-before-emit resume-safety check).
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        for (ResumeSpec row : ROWS) {
            if (row.resumeClass() != ResumeClass.IDENTITY) {
                continue;
            }
            fields.put(row.column(), nz(row.canonical().apply(command)));
        }
        return LengthPrefixedFields.encode(fields);
    }

    /**
     * The IDENTITY columns whose value differs between two {@link #identitySpec} strings — the
     * changed options a resume refusal names — an improvement over a blanket "the filter,
     * output format, or --sort/--no-sort mode changed" message. Both strings are produced by the same
     * registry on the same build, so they share column set and order; a positional/keyed walk is
     * exact. Empty when nothing identity-relevant changed (a bare resume that restored every value).
     */
    static List<String> changedColumns(String storedSpec, String currentSpec) {
        Map<String, String> current = decodeCurrent(currentSpec);
        Map<String, String> stored;
        try {
            stored = LengthPrefixedFields.decode(storedSpec);
        } catch (LengthPrefixedFields.Malformed e) {
            // A stored identity_spec that no longer parses cannot be confirmed to match this
            // invocation, so refuse: report every current IDENTITY column as changed.
            return new ArrayList<>(current.keySet());
        }
        List<String> changed = new ArrayList<>();
        for (Map.Entry<String, String> e : current.entrySet()) {
            if (!Objects.equals(e.getValue(), stored.get(e.getKey()))) {
                changed.add(e.getKey());
            }
        }
        for (String column : stored.keySet()) {
            if (!current.containsKey(column)) {
                changed.add(column);
            }
        }
        return changed;
    }

    /** Decode a spec this registry just produced; a decode failure is a broken invariant, not input. */
    private static Map<String, String> decodeCurrent(String spec) {
        try {
            return LengthPrefixedFields.decode(spec);
        } catch (LengthPrefixedFields.Malformed e) {
            throw new IllegalStateException("freshly-built identity_spec did not decode: " + spec, e);
        }
    }

    private static String formatToken(ListCommand command) {
        // The RESOLVED format (what output_format stores), not the raw --format auto/null: identitySpec
        // must fingerprint the same value at creation and at resume, or every resume would spuriously
        // refuse. ListCommand keeps output.resolvedFormat in step with its resolved local across the
        // resume restore. Null only before resolveOutput has run — no persisted run reaches
        // identitySpec in that state — so "auto" is a harmless placeholder there.
        OutputFormat format = command.output.resolvedFormat;
        return format == null ? "auto" : OutputOptions.token(format);
    }

    private static void restoreFormat(ListCommand command, String value) {
        command.output.format = "auto".equals(value)
                ? null : OutputFormat.valueOf(value.toUpperCase(Locale.ROOT));
    }

    private static void restoreFilters(ListCommand command, String spec) {
        FilterSpecCodec.Decoded decoded;
        try {
            decoded = FilterSpecCodec.decode(spec);
        } catch (CheckpointException e) {
            throw new IllegalStateException("stored filter_spec did not decode: " + spec, e);
        }
        command.filters.include = decoded.include();
        command.filters.exclude = decoded.exclude();
        command.filters.minSize = decoded.minSize();
        command.filters.maxSize = decoded.maxSize();
        command.filters.modifiedAfter = decoded.modifiedAfter();
        command.filters.modifiedBefore = decoded.modifiedBefore();
        command.filters.storageClasses = decoded.storageClasses();
    }

    private static String bucketOf(ListCommand command) {
        S3Uri parsed = parseUri(command);
        return parsed == null ? "" : parsed.bucket();
    }

    private static String prefixOf(ListCommand command) {
        S3Uri parsed = parseUri(command);
        return parsed == null ? "" : parsed.prefixAsString();
    }

    private static S3Uri parseUri(ListCommand command) {
        if (command.uri == null) {
            return null;
        }
        try {
            return S3Uri.parse(command.uri);
        } catch (InvalidUriException e) {
            return null;
        }
    }

    private static boolean parseBool(String value) {
        return Boolean.parseBoolean(value);
    }

    private static String nz(String value) {
        return value == null ? "" : value;
    }
}
