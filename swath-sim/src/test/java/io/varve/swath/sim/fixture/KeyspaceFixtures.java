/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.fixture;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Small, deterministic keyspaces shaped like the ones real buckets have.
 *
 * <p>Shape is the whole point. A listing policy's behaviour is decided almost entirely by how keys are
 * distributed — where the directories are, whether mass sits in a few of them or spreads evenly,
 * whether a split can find a populated pivot — and a fixture of uniformly-named keys exercises none of
 * that. Each generator here reproduces one shape a real bucket has, at a size that runs in
 * milliseconds, so a policy's phase behaviour can be watched without a multi-hundred-million-key
 * capture.
 *
 * <p>Every generator emits keys in ascending unsigned byte order, which is also the order they are
 * generated in: fixed-width fields everywhere, so no sort is needed and no locale can reorder them.
 */
public final class KeyspaceFixtures {

    private KeyspaceFixtures() {
    }

    /**
     * <b>Observation archive.</b> Deep date-partitioned directories with a wide, fairly even fan-out
     * at the leaf: {@code obs/<yyyy>/<mm>/<dd>/<hh>/<station>/<seq>}. Structure is everywhere, mass is
     * spread thinly across many directories, and the interesting question is whether the seed descent
     * cuts at a level that gives the fleet real parallelism instead of one range per year.
     */
    public static List<byte[]> observationArchive(int days, int hoursPerDay, int stations, int perStation) {
        List<byte[]> keys = new ArrayList<>();
        for (int day = 0; day < days; day++) {
            for (int hour = 0; hour < hoursPerDay; hour++) {
                for (int station = 0; station < stations; station++) {
                    for (int seq = 0; seq < perStation; seq++) {
                        keys.add(utf8(String.format(Locale.ROOT, "obs/2026/%02d/%02d/%02d/st%04d/%06d.bufr",
                                1 + day / 28, 1 + day % 28, hour, station, seq)));
                    }
                }
            }
        }
        return keys;
    }

    /**
     * <b>Hash-fanned corpus.</b> Two levels of hex directories over a flat body of content-addressed
     * names: {@code data/<hh>/<hh>/<hex>}. Uniform by construction — no skew for a density estimate to
     * find — so a split's pivot placement is decided by the alphabet it observes rather than by where
     * the mass happens to be.
     */
    public static List<byte[]> hashFannedCorpus(int topDirs, int subDirs, int perDir) {
        List<byte[]> keys = new ArrayList<>();
        for (int top = 0; top < topDirs; top++) {
            for (int sub = 0; sub < subDirs; sub++) {
                for (int i = 0; i < perDir; i++) {
                    keys.add(utf8(String.format(Locale.ROOT, "data/%02x/%02x/%08x", top, sub,
                            (top << 20) + (sub << 12) + i)));
                }
            }
        }
        return keys;
    }

    /**
     * <b>One object per directory.</b> The shape that punishes any strategy which treats a directory as
     * a unit of work: enumerating the tree costs one call per object, while a flat scan costs one call
     * per thousand. The seed descent is supposed to recognise it and leave it whole.
     */
    public static List<byte[]> oneObjectPerDirectory(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(utf8(String.format(Locale.ROOT, "tree/%06d/%06d/object", i, i)));
        }
        return keys;
    }

    /**
     * <b>A single dense flat leaf.</b> No directories at all below the prefix, so structure discovery
     * has nothing to find and every split has to be placed by interpolation. This is the shape where a
     * pivot landing in a dead zone — a region of the code-point space no key occupies — costs a whole
     * probe for nothing.
     */
    public static List<byte[]> denseFlatLeaf(int count) {
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(utf8(String.format(Locale.ROOT, "flat/%09d", i)));
        }
        return keys;
    }

    private static byte[] utf8(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }
}
