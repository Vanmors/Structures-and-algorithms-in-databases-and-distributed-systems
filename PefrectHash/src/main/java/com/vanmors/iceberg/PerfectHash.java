package com.vanmors.iceberg;

import org.instancio.Instancio;

import java.util.*;


public class PerfectHash {
    private static final int MAX_TRIES_BUCKET = 100000;

    private static final int MAX_TRIES_SEED = 100000;

    private final int[] seeds;          // seed для каждой корзины

    private final int bucketCount;

    private final int keyCount;

    private int seed;

    public int getSeed() {
        return seed;
    }

    public PerfectHash(final List<String> keys) {
        this.keyCount = keys.size();
        this.bucketCount = (int) Math.ceil(keyCount * 1000);

        seeds = new int[bucketCount];
        build(keys);
    }

    private void build(final List<String> keys) {
        final Random rnd = new Random(42);

        // Группируем ключи по первому хешу (bucket)
        final List<String>[] buckets = new List[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new ArrayList<>();
        }

        outer:
        for (int attempt = 0; attempt < MAX_TRIES_SEED; attempt++) {
            final int globalSeed = rnd.nextInt();

            // чистим корзины
            for (final List<String> b : buckets) {
                b.clear();
            }

            for (final String key : keys) {
                final int bucket = Math.floorMod(hash(key, globalSeed), bucketCount);
                buckets[bucket].add(key);
            }

            // Пытаемся разместить каждую корзину
            Arrays.fill(seeds, 0);
            final Set<Integer> positions = new HashSet<>();
            for (int b = 0; b < bucketCount; b++) {
                final List<String> bucketKeys = buckets[b];
                if (bucketKeys.isEmpty()) {
                    continue;
                }

                boolean placed = false;
                for (int s = 1; s < MAX_TRIES_BUCKET; s++) {
                    boolean ok = true;

                    for (final String k : bucketKeys) {
                        final int pos = Math.floorMod(displaceHash(k, globalSeed, s, b), keyCount);
                        if (!positions.add(pos)) {
                            ok = false;
                            break;
                        }
                    }

                    if (ok) {
                        seeds[b] = s;
                        placed = true;
                        break;
                    }
                }
                if (!placed) {
                    continue outer; // неудача -> новый global seed
                }
            }

            System.out.println("Успешно построено за " + attempt + " попыток глобального сида");
            this.seed = globalSeed;
            return;
        }

        throw new IllegalStateException("Не удалось построить perfect hash после " + MAX_TRIES_SEED + " попыток");
    }

    public int getIndex(final String key, final int seed) {
        final int bucket = Math.floorMod(hash(key, seed), bucketCount);
        final int displaceSeed = seeds[bucket];
        if (displaceSeed == 0) {
            return -1;
        }
        return Math.floorMod(displaceHash(key, seed, displaceSeed, bucket), keyCount);
    }

    private static int hash(final String s, final int seed) {
        long h = 0x517cc1b727220a95L ^ seed;
        for (int i = 0; i < s.length(); i++) {
            h ^= s.charAt(i);
            h *= 0x5bd1e995L;
            h ^= h >>> 47;
        }
        return (int) h;
    }

    private static int displaceHash(final String s, final int globalSeed, final int displaceSeed, final int bucket) {
        long h = hash(s, globalSeed);
        h ^= displaceSeed;
        h ^= bucket * 0x9E3779B97F4A7C15L;
        h *= 0xBF58476D1CE4E5B9L;
        h ^= h >>> 30;
        h *= 0x94D049BB133111EBL;
        h ^= h >>> 27;
        return (int) h;
//        h *= 0x85ebca6bL;
//        h ^= h >>> 13;
//        h *= 0xc2b2ae35L;
//        h ^= h >>> 16;
//        return (int) h;
    }

    public static void main(final String[] args) {

        final List<String> largeKeys = Instancio.ofList(String.class)
                .size(10000)
                .create();

        final var phf = new PerfectHash(largeKeys);

//        for (final var k : largeKeys) {
//            System.out.printf("%s → %d%n", k, phf.getIndex(k, phf.getSeed()));
//        }
    }
}