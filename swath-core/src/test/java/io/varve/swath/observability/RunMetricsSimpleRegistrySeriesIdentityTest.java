/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization: the FROZEN in-process meter-series identity of {@link RunMetrics}, as seen
 * through a {@link SimpleMeterRegistry}.
 *
 * <p><b>SCOPE — this is the SIMPLE-registry / run-summary surface, NOT the exported wire.</b>
 * {@code MeterRegistries} builds a {@link SimpleMeterRegistry} by default and an {@code
 * OtlpMeterRegistry} when {@code SWATH_OTLP_ENDPOINT}/{@code --metrics-export=otlp} is set, and the
 * two do NOT emit the same set. In particular Micrometer's {@code SimpleMeterRegistry.newTimer}
 * registers {@code HistogramGauges} — 3 extra {@code *.percentile{phi}} GAUGE meters per
 * percentile-publishing timer, 66 in total here — that {@code OtlpMeterRegistry.newTimer} does not
 * create at all (OTLP encodes percentiles as Summary quantiles on the wire instead). So this
 * snapshot holds {@value #EXPECTED_SIMPLE_METER_COUNT} ids where the same run yields
 * {@value RunMetricsOtlpSeriesIdentityTest#EXPECTED_OTLP_METER_COUNT} meters under OTLP. That side
 * is guarded separately by {@link RunMetricsOtlpSeriesIdentityTest} — treat the two as a PAIR;
 * neither alone describes what a consumer receives.
 *
 * <p><b>Known coverage gap — do not overstate what this covers.</b> It is tempting to say this
 * guard also governs the JSON run-summary's generic {@code meters[]} readback. It does NOT, in
 * general: {@code ListRunner} (~:1334) hands {@code JsonRunSummaryWriter} whatever registry the run
 * is using — {@code ctx.metrics().registry()} — so in OTLP mode that block is rendered from an
 * {@code OtlpMeterRegistry} (a different, smaller meter set) rather than from the set pinned here.
 * This snapshot therefore governs the {@code meters[]} readback only on the DEFAULT no-export path.
 * OTLP-mode JSON meter rendering is covered by neither guard.
 *
 * <p><b>This pins a frozen public surface.</b> Varve consumes swath's emitted metric series by
 * name and tag set; a meter that is added, dropped, renamed or re-tagged is a breaking change to
 * that consumer. {@link #EXPECTED_METER_IDS} and {@link #EXPECTED_DETERMINISTIC_COUNTS} are
 * therefore updated DELIBERATELY, in the same commit as the intended metric change, and shipped in
 * the Varve-facing change note — they are NEVER silently re-captured to turn a red test green. A
 * refactor (e.g. extracting subsystems out of {@code RunMetrics} behind a facade) must leave both
 * constants byte-identical; if it doesn't, the refactor changed the public surface.
 *
 * <p>Deliberately identity-only: meter NAME, TYPE and the full TAG SET (keys and values). Magnitudes
 * that depend on wall clock or platform are normalized out; the event COUNTS that are deterministic
 * for this workload are pinned separately in {@link #EXPECTED_DETERMINISTIC_COUNTS}.
 */
final class RunMetricsSimpleRegistrySeriesIdentityTest {

    /**
     * {@code swath.process.cpu.time} is the one platform-conditional series — {@link RunMetrics}
     * only registers it when the {@code OperatingSystemMXBean} CPU-time reading is available (a
     * {@link FunctionCounter} cannot honestly report {@code NaN}), so it is filtered out of the
     * comparison on a platform that lacks it rather than being absent from the frozen list.
     */
    private static final String CPU_TIME_METER_ID = "COUNTER|swath.process.cpu.time|{}";

    /**
     * Every meter series {@link RunMetricsCharacterizationWorkload#drive} emits, as
     * {@code TYPE|name|{sorted tag=value pairs}}. See the class javadoc: FROZEN, updated only
     * deliberately.
     */
    private static final List<String> EXPECTED_METER_IDS = List.of(
            "COUNTER|swath.aimd.growth_freeze|{}",
            "COUNTER|swath.aimd.latency_freeze|{}",
            "COUNTER|swath.aimd.target_reductions|{}",
            "COUNTER|swath.aimd.timeout_shed|{}",
            "COUNTER|swath.aimd.votes|{}",
            "COUNTER|swath.api.calls|{strategy=WORK_STEALING}",
            "COUNTER|swath.bytes.estimated|{}",
            "COUNTER|swath.entries.emitted|{}",
            "COUNTER|swath.errors|{type=accessdenied}",
            "COUNTER|swath.errors|{type=nosuchbucket}",
            "COUNTER|swath.errors|{type=throttle}",
            "COUNTER|swath.idle_backoff.resets|{}",
            "COUNTER|swath.idle_backoff.slot_denied|{}",
            "COUNTER|swath.open_frontier.keys_emitted|{}",
            "COUNTER|swath.output.broken_pipe|{}",
            "COUNTER|swath.output.bytes|{format=jsonl}",
            "COUNTER|swath.output.bytes|{format=parquet}",
            "COUNTER|swath.output.files|{format=jsonl,outcome=written}",
            "COUNTER|swath.output.files|{format=parquet,outcome=written}",
            "COUNTER|swath.page.raw_count|{}",
            "COUNTER|swath.page.raw_keys|{}",
            "COUNTER|swath.page.short_truncated|{}",
            "COUNTER|swath.parquet.parts|{outcome=discarded}",
            "COUNTER|swath.parquet.parts|{outcome=finalize_failed}",
            "COUNTER|swath.parquet.parts|{outcome=finalized}",
            "COUNTER|swath.parquet.rotation|{trigger=rows}",
            "COUNTER|swath.parquet.rotation|{trigger=size}",
            "COUNTER|swath.parquet.rotation|{trigger=time}",
            "COUNTER|swath.probe.empty_upper_bisections|{}",
            "COUNTER|swath.probe.fetches|{}",
            "COUNTER|swath.probe.structure_fetches|{}",
            CPU_TIME_METER_ID,
            "COUNTER|swath.progress.units|{}",
            "COUNTER|swath.s3.pool.connection_aborted|{}",
            "COUNTER|swath.s3.pool.handshakes|{}",
            "COUNTER|swath.s3.socket_closure_recovered|{}",
            "COUNTER|swath.sort.entries|{}",
            "COUNTER|swath.sort.merge.passes|{}",
            "COUNTER|swath.sort.segment.bytes|{}",
            "COUNTER|swath.sort.segments.written|{}",
            "COUNTER|swath.split.guard_aborts|{}",
            "COUNTER|swath.split.unsplittable_victims|{}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_CREATED,reason=split_committed}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=empty}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=large}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=small}",
            "COUNTER|swath.steal_reason|{outcome=CHILD_MASS,reason=tiny}",
            "COUNTER|swath.steal_reason|{outcome=NO_VICTIM,reason=no_splittable_victim}",
            "COUNTER|swath.steal_reason|{outcome=OWNER_SPLIT,reason=self_published}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=dead_zone}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=hex_alpha}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=hex_digit}",
            "COUNTER|swath.steal_reason|{outcome=PIVOT_BYTE,reason=other}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=args_hash_refused}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=durable_cursor_lag}",
            "COUNTER|swath.steal_reason|{outcome=RESUME,reason=nodes_reopened}",
            "COUNTER|swath.steal_reason|{outcome=RETRY,reason=split_aborted}",
            "COUNTER|swath.steal_reason|{outcome=SEED,reason=radix_bands}",
            "COUNTER|swath.steal_reason|{outcome=SHED,reason=timeout_storm_probe_fed}",
            "COUNTER|swath.steal_reason|{outcome=SHED,reason=timeout_storm_worker_fed}",
            "COUNTER|swath.steal_reason|{outcome=STEAL,reason=attempted}",
            "COUNTER|swath.steal_reason|{outcome=UNSPLITTABLE,reason=no_pivot}",
            "COUNTER|swath.steals|{result=CHILD_CREATED}",
            "COUNTER|swath.steals|{result=NO_VICTIM}",
            "COUNTER|swath.throttle.events|{type=attempt_timeout}",
            "COUNTER|swath.throttle.events|{type=network}",
            "COUNTER|swath.throttle.events|{type=server5xx}",
            "COUNTER|swath.throttle.events|{type=slowdown}",
            "DISTRIBUTION_SUMMARY|swath.checkpoint.commit_batch_size|{}",
            "DISTRIBUTION_SUMMARY|swath.sort.page_runs_per_buffer|{}",
            "GAUGE|swath.aimd.latency_baseline_ms|{}",
            "GAUGE|swath.aimd.target_low_water|{}",
            "GAUGE|swath.api.latency.percentile|{op=listObjectsV2,phi=0.5}",
            "GAUGE|swath.api.latency.percentile|{op=listObjectsV2,phi=0.99}",
            "GAUGE|swath.api.latency.percentile|{op=listObjectsV2,phi=0.9}",
            "GAUGE|swath.checkpoint.commit.latency.percentile|{phi=0.5}",
            "GAUGE|swath.checkpoint.commit.latency.percentile|{phi=0.99}",
            "GAUGE|swath.checkpoint.commit.latency.percentile|{phi=0.9}",
            "GAUGE|swath.checkpoint.commit.wait.percentile|{phi=0.5}",
            "GAUGE|swath.checkpoint.commit.wait.percentile|{phi=0.99}",
            "GAUGE|swath.checkpoint.commit.wait.percentile|{phi=0.9}",
            "GAUGE|swath.checkpoint.queue.depth|{}",
            "GAUGE|swath.checkpoint.queue.wait.percentile|{phi=0.5}",
            "GAUGE|swath.checkpoint.queue.wait.percentile|{phi=0.99}",
            "GAUGE|swath.checkpoint.queue.wait.percentile|{phi=0.9}",
            "GAUGE|swath.disk.free_bytes|{}",
            "GAUGE|swath.emit.latency.percentile|{phi=0.5}",
            "GAUGE|swath.emit.latency.percentile|{phi=0.99}",
            "GAUGE|swath.emit.latency.percentile|{phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=connect_acquire,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=connect_acquire,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=connect_acquire,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=response_parse,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=response_parse,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=response_parse,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=sdk_unmarshal,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=sdk_unmarshal,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=sdk_unmarshal,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=total,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=total,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=total,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=ttfb,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=ttfb,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=pivot_probe,phase=ttfb,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=connect_acquire,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=connect_acquire,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=connect_acquire,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=response_parse,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=response_parse,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=response_parse,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=sdk_unmarshal,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=sdk_unmarshal,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=sdk_unmarshal,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=total,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=total,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=total,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=ttfb,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=ttfb,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=structure_probe,phase=ttfb,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=connect_acquire,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=connect_acquire,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=connect_acquire,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=response_parse,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=response_parse,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=response_parse,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=sdk_unmarshal,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=sdk_unmarshal,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=sdk_unmarshal,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=total,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=total,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=total,phi=0.9}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=ttfb,phi=0.5}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=ttfb,phi=0.99}",
            "GAUGE|swath.fetch.latency.phase.percentile|{call_class=worker_page,phase=ttfb,phi=0.9}",
            "GAUGE|swath.idle_backoff.level|{}",
            "GAUGE|swath.in_flight.avg|{}",
            "GAUGE|swath.owner_split.demand_gated_t_min|{}",
            "GAUGE|swath.owner_split.demand_gated_t|{}",
            "GAUGE|swath.parquet.write.latency.percentile|{phi=0.5}",
            "GAUGE|swath.parquet.write.latency.percentile|{phi=0.99}",
            "GAUGE|swath.parquet.write.latency.percentile|{phi=0.9}",
            "GAUGE|swath.phase|{}",
            "GAUGE|swath.process.memory.heap.bytes|{kind=current}",
            "GAUGE|swath.process.memory.heap.bytes|{kind=peak}",
            "GAUGE|swath.process.memory.rss.bytes|{kind=current}",
            "GAUGE|swath.process.memory.rss.bytes|{kind=peak}",
            "GAUGE|swath.queue.wait.percentile|{phi=0.5}",
            "GAUGE|swath.queue.wait.percentile|{phi=0.99}",
            "GAUGE|swath.queue.wait.percentile|{phi=0.9}",
            "GAUGE|swath.run.throughput|{}",
            "GAUGE|swath.s3.pool.idle_available|{}",
            "GAUGE|swath.s3.pool.leased|{}",
            "GAUGE|swath.s3.pool.max|{}",
            "GAUGE|swath.s3.pool.pending_acquisition|{}",
            "GAUGE|swath.sort.handoff.queue.depth.peak|{}",
            "GAUGE|swath.sort.off_thread.buffers.peak|{}",
            "GAUGE|swath.sort.staging.bytes.peak|{}",
            "GAUGE|swath.tail_occupancy.avg_in_flight|{pct=10}",
            "GAUGE|swath.tail_occupancy.avg_in_flight|{pct=5}",
            "GAUGE|swath.tail_occupancy.wall_share|{pct=10}",
            "GAUGE|swath.tail_occupancy.wall_share|{pct=5}",
            "GAUGE|swath.workers.active|{}",
            "TIMER|swath.api.latency|{op=listObjectsV2}",
            "TIMER|swath.checkpoint.commit.latency|{}",
            "TIMER|swath.checkpoint.commit.wait|{}",
            "TIMER|swath.checkpoint.queue.wait|{}",
            "TIMER|swath.emit.latency|{}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=pivot_probe,phase=ttfb}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=structure_probe,phase=ttfb}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=connect_acquire}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=response_parse}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=sdk_unmarshal}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=total}",
            "TIMER|swath.fetch.latency.phase|{call_class=worker_page,phase=ttfb}",
            "TIMER|swath.idle_backoff.park_time|{}",
            "TIMER|swath.parquet.finalize.latency|{}",
            "TIMER|swath.parquet.write.latency|{}",
            "TIMER|swath.queue.wait|{}",
            "TIMER|swath.rate_limit.api_wait|{}",
            "TIMER|swath.rate_limit.wait|{}",
            "TIMER|swath.run.duration|{}",
            "TIMER|swath.sort.backpressure.wait|{}",
            "TIMER|swath.sort.merge.latency|{}",
            "TIMER|swath.sort.merge.range.latency|{}");

    /** Deterministic event counts for the same workload, as {@code name{tags}=count}. FROZEN. */
    private static final List<String> EXPECTED_DETERMINISTIC_COUNTS = List.of(
            "swath.aimd.growth_freeze{}=1",
            "swath.aimd.latency_freeze{}=1",
            "swath.aimd.target_reductions{}=1",
            "swath.aimd.timeout_shed{}=1",
            "swath.aimd.votes{}=1",
            "swath.api.calls{strategy=WORK_STEALING}=3",
            "swath.api.latency{op=listObjectsV2}=1",
            "swath.bytes.estimated{}=90000",
            "swath.checkpoint.commit.latency{}=1",
            "swath.checkpoint.commit.wait{}=1",
            "swath.checkpoint.commit_batch_size{}=1",
            "swath.checkpoint.queue.wait{}=1",
            "swath.emit.latency{}=1",
            "swath.entries.emitted{}=910",
            "swath.errors{type=accessdenied}=1",
            "swath.errors{type=nosuchbucket}=1",
            "swath.errors{type=throttle}=1",
            "swath.fetch.latency.phase{call_class=pivot_probe,phase=connect_acquire}=1",
            "swath.fetch.latency.phase{call_class=pivot_probe,phase=response_parse}=1",
            "swath.fetch.latency.phase{call_class=pivot_probe,phase=sdk_unmarshal}=1",
            "swath.fetch.latency.phase{call_class=pivot_probe,phase=total}=1",
            "swath.fetch.latency.phase{call_class=pivot_probe,phase=ttfb}=1",
            "swath.fetch.latency.phase{call_class=structure_probe,phase=connect_acquire}=1",
            "swath.fetch.latency.phase{call_class=structure_probe,phase=response_parse}=1",
            "swath.fetch.latency.phase{call_class=structure_probe,phase=sdk_unmarshal}=1",
            "swath.fetch.latency.phase{call_class=structure_probe,phase=total}=1",
            "swath.fetch.latency.phase{call_class=structure_probe,phase=ttfb}=1",
            "swath.fetch.latency.phase{call_class=worker_page,phase=connect_acquire}=1",
            "swath.fetch.latency.phase{call_class=worker_page,phase=response_parse}=1",
            "swath.fetch.latency.phase{call_class=worker_page,phase=sdk_unmarshal}=1",
            "swath.fetch.latency.phase{call_class=worker_page,phase=total}=1",
            "swath.fetch.latency.phase{call_class=worker_page,phase=ttfb}=1",
            "swath.idle_backoff.park_time{}=1",
            "swath.idle_backoff.resets{}=1",
            "swath.idle_backoff.slot_denied{}=1",
            "swath.open_frontier.keys_emitted{}=0",
            "swath.output.broken_pipe{}=1",
            "swath.output.bytes{format=jsonl}=512",
            "swath.output.bytes{format=parquet}=4096",
            "swath.output.files{format=jsonl,outcome=written}=1",
            "swath.output.files{format=parquet,outcome=written}=2",
            "swath.page.raw_count{}=1",
            "swath.page.raw_keys{}=900",
            "swath.page.short_truncated{}=1",
            "swath.parquet.finalize.latency{}=1",
            "swath.parquet.parts{outcome=discarded}=1",
            "swath.parquet.parts{outcome=finalize_failed}=1",
            "swath.parquet.parts{outcome=finalized}=1",
            "swath.parquet.rotation{trigger=rows}=1",
            "swath.parquet.rotation{trigger=size}=1",
            "swath.parquet.rotation{trigger=time}=1",
            "swath.parquet.write.latency{}=1",
            "swath.probe.empty_upper_bisections{}=1",
            "swath.probe.fetches{}=1",
            "swath.probe.structure_fetches{}=1",
            "swath.progress.units{}=950",
            "swath.queue.wait{}=1",
            "swath.rate_limit.api_wait{}=1",
            "swath.rate_limit.wait{}=1",
            "swath.run.duration{}=1",
            "swath.s3.pool.connection_aborted{}=1",
            "swath.s3.pool.handshakes{}=1",
            "swath.s3.socket_closure_recovered{}=1",
            "swath.sort.backpressure.wait{}=1",
            "swath.sort.entries{}=500",
            "swath.sort.merge.latency{}=1",
            "swath.sort.merge.passes{}=3",
            "swath.sort.merge.range.latency{}=1",
            "swath.sort.page_runs_per_buffer{}=1",
            "swath.sort.segment.bytes{}=2048",
            "swath.sort.segments.written{}=3",
            "swath.split.guard_aborts{}=1",
            "swath.split.unsplittable_victims{}=1",
            "swath.steal_reason{outcome=CHILD_CREATED,reason=split_committed}=1",
            "swath.steal_reason{outcome=CHILD_MASS,reason=empty}=1",
            "swath.steal_reason{outcome=CHILD_MASS,reason=large}=1",
            "swath.steal_reason{outcome=CHILD_MASS,reason=small}=1",
            "swath.steal_reason{outcome=CHILD_MASS,reason=tiny}=1",
            "swath.steal_reason{outcome=NO_VICTIM,reason=no_splittable_victim}=1",
            "swath.steal_reason{outcome=OWNER_SPLIT,reason=self_published}=1",
            "swath.steal_reason{outcome=PIVOT_BYTE,reason=dead_zone}=1",
            "swath.steal_reason{outcome=PIVOT_BYTE,reason=hex_alpha}=1",
            "swath.steal_reason{outcome=PIVOT_BYTE,reason=hex_digit}=1",
            "swath.steal_reason{outcome=PIVOT_BYTE,reason=other}=1",
            "swath.steal_reason{outcome=RESUME,reason=args_hash_refused}=1",
            "swath.steal_reason{outcome=RESUME,reason=durable_cursor_lag}=1",
            "swath.steal_reason{outcome=RESUME,reason=nodes_reopened}=2",
            "swath.steal_reason{outcome=RETRY,reason=split_aborted}=1",
            "swath.steal_reason{outcome=SEED,reason=radix_bands}=4",
            "swath.steal_reason{outcome=SHED,reason=timeout_storm_probe_fed}=1",
            "swath.steal_reason{outcome=SHED,reason=timeout_storm_worker_fed}=2",
            "swath.steal_reason{outcome=STEAL,reason=attempted}=1",
            "swath.steal_reason{outcome=UNSPLITTABLE,reason=no_pivot}=1",
            "swath.steals{result=CHILD_CREATED}=1",
            "swath.steals{result=NO_VICTIM}=1",
            "swath.throttle.events{type=attempt_timeout}=1",
            "swath.throttle.events{type=network}=1",
            "swath.throttle.events{type=server5xx}=1",
            "swath.throttle.events{type=slowdown}=1");

    /** Size of {@link #EXPECTED_METER_IDS} — see the class javadoc for why it differs under OTLP. */
    private static final int EXPECTED_SIMPLE_METER_COUNT = 192;

    /**
     * A valid production run emits exactly ONE {@code swath.api.calls} series, because {@code
     * ListCommand} sets the strategy before the first API call. The counter-derived {@code
     * summary().apiCalls()} must equal that single series' count — if a decomposition ever
     * reintroduced the fragmentation, this is where the representative run notices.
     */
    @Test
    void aProductionOrderedRunEmitsExactlyOneApiCallSeries(@TempDir Path scratchDir) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics m = RunMetricsCharacterizationWorkload.drive(registry, scratchDir);

        assertThat(meterIds(registry).stream().filter(id -> id.contains("swath.api.calls")).toList())
                .containsExactly("COUNTER|swath.api.calls|{strategy=WORK_STEALING}");
        assertThat(m.summary(Duration.ofSeconds(5), "WORK_STEALING", 2L, 4_096L).apiCalls()).isEqualTo(3L);
    }

    @Test
    void emittedMeterSeriesIdentityIsFrozen(@TempDir Path scratchDir) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetricsCharacterizationWorkload.drive(registry, scratchDir);

        List<String> expected = new ArrayList<>(EXPECTED_METER_IDS);
        assertThat(expected).hasSize(EXPECTED_SIMPLE_METER_COUNT);
        if (ResourceMetrics.processCpuTimeNanos() < 0) {
            expected.remove(CPU_TIME_METER_ID);
        }
        assertThat(meterIds(registry)).containsExactlyElementsOf(expected);
    }

    @Test
    void deterministicMeterCountsAreFrozen(@TempDir Path scratchDir) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetricsCharacterizationWorkload.drive(registry, scratchDir);

        assertThat(deterministicCounts(registry)).containsExactlyElementsOf(EXPECTED_DETERMINISTIC_COUNTS);
    }

    /** {@code TYPE|name|{tags}} for every registered meter, sorted deterministically. */
    private static List<String> meterIds(MeterRegistry registry) {
        return registry.getMeters().stream()
                .map(meter -> meter.getId().getType() + "|" + meter.getId().getName() + "|{" + tags(meter) + "}")
                .sorted()
                .toList();
    }

    /**
     * {@code name{tags}=count} for every event-counting meter — {@link Counter}, {@link Timer} and
     * {@link DistributionSummary}. Gauges (live readings) and {@link FunctionCounter} (the platform
     * CPU clock) are excluded: their values are inherently variable.
     */
    private static List<String> deterministicCounts(MeterRegistry registry) {
        List<String> out = new ArrayList<>();
        for (Meter meter : registry.getMeters()) {
            String key = meter.getId().getName() + "{" + tags(meter) + "}=";
            if (meter instanceof FunctionCounter) {
                continue;
            } else if (meter instanceof Counter counter) {
                out.add(key + (long) counter.count());
            } else if (meter instanceof Timer timer) {
                out.add(key + timer.count());
            } else if (meter instanceof DistributionSummary summary) {
                out.add(key + summary.count());
            }
        }
        return out.stream().sorted().toList();
    }

    private static String tags(Meter meter) {
        return meter.getId().getTags().stream()
                .sorted(Comparator.comparing(Tag::getKey).thenComparing(Tag::getValue))
                .map(tag -> tag.getKey() + "=" + tag.getValue())
                .collect(Collectors.joining(","));
    }

}
