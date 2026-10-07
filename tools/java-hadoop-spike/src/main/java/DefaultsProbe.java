/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;

import java.lang.reflect.Modifier;
import java.util.TreeMap;
import java.util.TreeSet;

/** Dumps options in separate stock-artifact and patched-API JVMs for an exact diff. */
public final class DefaultsProbe {
    public static void main(String[] args) throws Exception {
        for (int scenario = 0; scenario < 2; scenario++) {
            var conf = new PlainParquetConfiguration();
            if (scenario == 1) {
                conf.set("parquet.filter.stats.enabled", "false");
                conf.set("parquet.filter.dictionary.enabled", "false");
                conf.set("parquet.filter.record-level.enabled", "false");
                conf.set("parquet.filter.columnindex.enabled", "false");
                conf.set("parquet.page.verify-checksum.enabled", "true");
                conf.set("parquet.read.allocation.size", "65536");
                conf.set("parquet.read.bad.record.threshold", "0.125");
            }
            var codecs = new ProbeCodecs();
            ParquetReadOptions options =
                    args[0].equals("stock")
                            ? ParquetReadOptions.builder(conf).build()
                            : (ParquetReadOptions)
                                    Class.forName("PatchedOptions")
                                            .getMethod(
                                                    "create",
                                                    ParquetConfiguration.class,
                                                    ProbeCodecs.class)
                                            .invoke(null, conf, codecs);
            try {
                var values = new TreeMap<String, String>();
                for (var method : ParquetReadOptions.class.getMethods()) {
                    if (Modifier.isStatic(method.getModifiers())
                            || method.getParameterCount() != 0
                            || method.getName().equals("getCodecFactory")
                            || method.getDeclaringClass() != ParquetReadOptions.class) continue;
                    Object value = method.invoke(options);
                    String encoded;
                    if (value == null
                            || value instanceof Boolean
                            || value instanceof Number
                            || value instanceof String) {
                        encoded = String.valueOf(value);
                    } else if (value instanceof java.util.Set<?> set) {
                        encoded =
                                new TreeSet<>(set.stream().map(String::valueOf).toList())
                                        .toString();
                    } else {
                        encoded = value.getClass().getName();
                    }
                    values.put(method.getName(), encoded);
                }
                for (String property : options.getPropertyNames()) {
                    values.put("property." + property, options.getProperty(property));
                }
                if (options.getConfiguration() != conf)
                    throw new AssertionError("configuration identity");
                if (scenario == 1
                        && (options.getMaxAllocationSize() != 65536 || options.useStatsFilter())) {
                    throw new AssertionError("configured options did not engage");
                }
                for (var entry : values.entrySet()) {
                    System.out.println(scenario + ":" + entry.getKey() + "=" + entry.getValue());
                }
            } finally {
                options.getCodecFactory().release();
                codecs.release();
            }
        }
    }
}
