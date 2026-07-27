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

    /** Two-letter genus groups, ascending. Siblings inside one share bytes 8–9 and differ at byte 10. */
    private static final String[] GENUS_GROUPS = {"Ba", "Ca", "Da", "Er", "Fa", "Ga", "He", "Ic"};

    /** Genus stems, ascending, nine bytes each, with pairwise distinct first letters. */
    private static final String[] GENUS_STEMS = {"cephalura", "docerinus", "gastropha", "learicula",
            "melanurus", "nomoceras", "ryosaurus", "thysaurus"};

    /** Species epithets, nine bytes each. */
    private static final String[] EPITHETS = {"australis", "japonicus", "maritimus", "orientale",
            "regulorum", "virginica"};

    /** The accession's leading class letter, as a genomics archive names its individuals. */
    private static final String[] CLASS_LETTERS = {"a", "b", "f", "m", "r"};

    /** The data tree every accession has, ascending — where a deep-nested keyspace's mass actually is. */
    private static final String[] LEAF_DIRS = {
            "assembly_vgp_standard_1.6/evaluation",
            "assembly_vgp_standard_1.6/intermediates",
            "genomic_data/ont_ul",
            "genomic_data/pacbio_hifi"};

    /**
     * Leaf directories per accession. Derived from {@link #LEAF_DIRS} rather than written down, so
     * adding a directory cannot leave the javadoc that quotes this constant claiming the old number.
     */
    static final int LEAF_DIR_COUNT = LEAF_DIRS.length;

    /**
     * The stride the heavy-tailed mass law deals ranks by. Prime, so it is coprime with every species
     * count these limits allow and the dealing is therefore a permutation: every rank is used once.
     */
    private static final int RANK_STRIDE = 37;

    static final int GENUS_GROUP_LIMIT = 8;
    static final int SPECIES_PER_GROUP_LIMIT = 8;
    static final int ACCESSIONS_PER_SPECIES_LIMIT = 9;

    /** Files per leaf directory, capped by the six-digit field their names are formatted with. */
    static final int FILES_PER_LEAF_DIR_LIMIT = 999_999;

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
     * <b>Deep-nested shared prefix</b> (taxonomy-shaped). Many sibling subtrees under a shallow common
     * structure — {@code species/<Genus_epithet>/<accession>/<dataset>/<stage>/<file>} — where each
     * subtree holds hundreds of keys whose bytes differ only <em>far below</em> the depth at which the
     * subtrees themselves differ. Two adjacent species diverge at byte 10; everything inside either of
     * them varies from byte 39 on.
     *
     * <p>That gap is the shape, and it is the one thing about this fixture that is not free to move.
     * A range's position fraction ({@code StealMath.fracIn}) is measured over the 12 bytes after the
     * longest common prefix of the range's own bounds, so on a range spanning sibling subtrees the
     * measured window is bytes 10–22 — inside the directory name, above every byte a cursor draining
     * that subtree actually changes. The keys move; the window does not. Every layer that steers on
     * that fraction (victim choice, pivot mass floors, the owner's self-split, the density feedback) is
     * therefore reading a sensor this keyspace holds still, which no fixture of uniformly-named or
     * shallowly-nested keys can reproduce.
     *
     * <p>Names are synthetic but structured like a real genomics archive's, because the depths are what
     * matter: a fixed-width binomial directory, a fixed-width accession, then the per-accession data
     * tree the mass actually lives in. Sibling genera within a group share their first two letters and
     * differ at the third, which is what puts the divergence at byte 10.
     *
     * <p><b>How much each subtree holds is a second, separable property</b>, which is why it is a
     * parameter rather than a constant. The geometry above decides what the policies can <em>measure</em>;
     * the mass distribution decides whether being unable to measure it costs anything. A real archive's
     * is heavy-tailed — a few reference-quality species carry most of the files and the long tail carries
     * hundreds each — so {@link SubtreeMass#HEAVY_TAILED} is the realistic setting and
     * {@link SubtreeMass#UNIFORM} is the control that isolates one property from the other.
     *
     * @param genusGroups         two-letter genus groups, at most {@value #GENUS_GROUP_LIMIT}
     * @param speciesPerGroup     species in each group, at most {@value #SPECIES_PER_GROUP_LIMIT}
     * @param accessionsPerSpecies sequenced individuals per species, at most
     *                            {@value #ACCESSIONS_PER_SPECIES_LIMIT}
     * @param filesPerLeafDir     files in each leaf directory of an accession — of every accession
     *                            under {@link SubtreeMass#UNIFORM}, of the largest species under
     *                            {@link SubtreeMass#HEAVY_TAILED}, where the rest hold a fraction of
     *                            it and no species is ever left with none. At most
     *                            {@value #FILES_PER_LEAF_DIR_LIMIT}, the width of the file-name field
     * @param mass                how the file count is distributed across species
     */
    public static List<byte[]> deepNestedSharedPrefix(int genusGroups, int speciesPerGroup,
                                                      int accessionsPerSpecies, int filesPerLeafDir,
                                                      SubtreeMass mass) {
        require(genusGroups, GENUS_GROUP_LIMIT, "genusGroups");
        require(speciesPerGroup, SPECIES_PER_GROUP_LIMIT, "speciesPerGroup");
        require(accessionsPerSpecies, ACCESSIONS_PER_SPECIES_LIMIT, "accessionsPerSpecies");
        // A zero here is asymmetric between the two laws — it would empty a uniform keyspace outright,
        // while the heavy-tailed one's floor of one file would quietly keep generating — so neither is
        // allowed rather than one of them silently meaning something different.
        require(filesPerLeafDir, FILES_PER_LEAF_DIR_LIMIT, "filesPerLeafDir");
        int speciesCount = genusGroups * speciesPerGroup;
        List<byte[]> keys = new ArrayList<>();
        for (int group = 0; group < genusGroups; group++) {
            for (int species = 0; species < speciesPerGroup; species++) {
                String genus = GENUS_GROUPS[group] + GENUS_STEMS[species];
                String epithet = EPITHETS[species % EPITHETS.length];
                int files = fileCount(filesPerLeafDir, mass, group * speciesPerGroup + species,
                        speciesCount);
                for (int accession = 0; accession < accessionsPerSpecies; accession++) {
                    String directory = String.format(Locale.ROOT, "species/%s_%s/%s%s%s%d/",
                            genus, epithet, CLASS_LETTERS[species % CLASS_LETTERS.length],
                            syllable(genus), syllable(epithet), accession + 1);
                    for (String leafDir : LEAF_DIRS) {
                        for (int file = 0; file < files; file++) {
                            keys.add(utf8(String.format(Locale.ROOT, "%s%s/%06d.fastq.gz",
                                    directory, leafDir, file)));
                        }
                    }
                }
            }
        }
        return keys;
    }

    /** How {@link #deepNestedSharedPrefix} spreads its file count across species. */
    public enum SubtreeMass {
        /** Every species holds the same number of files: the control on mass, not on shape. */
        UNIFORM,
        /**
         * Zipf: the species of rank {@code r} holds {@code 1/r} of the largest one's files, which is
         * the distribution file counts in a real archive follow. Ranks are dealt by a fixed stride from
         * a fixed offset, so the heavy species land at reproducible but non-adjacent positions rather
         * than in a block at the keyspace's start, where one seed cut would separate them from
         * everything else for free. The deal is a permutation — every rank is used exactly once — so
         * which species is heaviest is a property of the stride and offset, not of its position.
         */
        HEAVY_TAILED
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

    /**
     * The files one species holds in each of its leaf directories: {@code largest} for every species
     * under a uniform mass, and {@code largest / rank} under a heavy-tailed one.
     *
     * <p>The rank is dealt by a stride coprime with the species count, so the deal is a permutation and
     * the heavy subtrees end up scattered. It starts half way along, which is what keeps rank 1 — the
     * one species that dominates the keyspace — away from position zero: dealt from the start, the
     * heaviest subtree would always be the keyspace's first, and a fixture whose mass sits at one end
     * flatters any seed that happens to cut there.
     */
    private static int fileCount(int largest, SubtreeMass mass, int species, int speciesCount) {
        if (mass == SubtreeMass.UNIFORM) {
            return largest;
        }
        int rank = 1 + (species * RANK_STRIDE + speciesCount / 2) % speciesCount;
        return Math.max(1, largest / rank);
    }

    /** The three-letter, initial-capital syllable an accession name is built from. */
    private static String syllable(String name) {
        return Character.toUpperCase(name.charAt(0)) + name.substring(1, 3);
    }

    private static void require(int value, int limit, String name) {
        if (value < 1 || value > limit) {
            throw new IllegalArgumentException(name + " must be in 1.." + limit + ", got " + value);
        }
    }
}
