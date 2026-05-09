package com.vanmors.perfecthash;

import com.google.common.hash.Hashing;
import org.instancio.Instancio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;


public class PerfectHash {

    private static final Logger log = LoggerFactory.getLogger(PerfectHash.class);

    // --- настраиваемые параметры ---
    private final int maxTriesBucket;
    private final int maxTriesSeed;
    private final int bucketCountMultiplier;
    private final long randomSeed;

    private final int[] seeds;          // seed для каждой корзины

    private final int bucketCount;

    private final int keyCount;

    private int seed;

    public static class Builder {
        private int maxTriesBucket = 100_000;
        private int maxTriesSeed = 100_000;
        private int bucketCountMultiplier = 1000;
        private long randomSeed = 42;
        private final List<String> keys;

        public Builder(final List<String> keys) {
            this.keys = keys;
        }

        public Builder maxTriesBucket(final int maxTriesBucket) {
            this.maxTriesBucket = maxTriesBucket;
            return this;
        }

        public Builder maxTriesSeed(final int maxTriesSeed) {
            this.maxTriesSeed = maxTriesSeed;
            return this;
        }

        public Builder bucketCountMultiplier(final int bucketCountMultiplier) {
            this.bucketCountMultiplier = bucketCountMultiplier;
            return this;
        }

        public Builder randomSeed(final long randomSeed) {
            this.randomSeed = randomSeed;
            return this;
        }

        public PerfectHash build() {
            return new PerfectHash(this);
        }
    }

    public static Builder builder(final List<String> keys) {
        return new Builder(keys);
    }

    public int getSeed() {
        return seed;
    }

    /**
     * Конструктор с параметрами по умолчанию (обратная совместимость).
     */
    public PerfectHash(final List<String> keys) {
        this(new Builder(keys));
    }

    private PerfectHash(final Builder b) {
        this.maxTriesBucket = b.maxTriesBucket;
        this.maxTriesSeed = b.maxTriesSeed;
        this.bucketCountMultiplier = b.bucketCountMultiplier;
        this.randomSeed = b.randomSeed;

        this.keyCount = b.keys.size();
        this.bucketCount = (int) Math.ceil(keyCount * bucketCountMultiplier);

        seeds = new int[bucketCount];
        build(b.keys);
    }

    private void build(final List<String> keys) {
        final Random rnd = new Random(randomSeed);

        // Группируем ключи по первому хешу (bucket)
        final List<String>[] buckets = new List[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new ArrayList<>();
        }

        outer:
        for (int attempt = 0; attempt < maxTriesSeed; attempt++) {
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
                for (int s = 1; s < maxTriesBucket; s++) {
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

            log.info("Успешно построено за {} попыток глобального сида", attempt);
            this.seed = globalSeed;
            return;
        }

        throw new IllegalStateException("Не удалось построить perfect hash после " + maxTriesSeed + " попыток");
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
        return Hashing.murmur3_128(seed)
                .hashString(s, StandardCharsets.UTF_8)
                .asInt();
    }

    private static int displaceHash(final String s, final int globalSeed,
                                    final int displaceSeed, final int bucket) {
        final int combinedSeed = globalSeed ^ (displaceSeed * 31) ^ bucket;

        return Hashing.murmur3_128(combinedSeed)
                .hashString(s, StandardCharsets.UTF_8)
                .asInt();
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