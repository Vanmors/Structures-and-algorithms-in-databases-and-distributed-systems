package com.vanmors.lsh;


import java.util.*;


public class LSH {
    private final int numHashFunctions;

    private final int rowsPerBand;      // r — строк в одной полосе

    private final int numBands;         // b — количество полос

    private final Map<String, List<String>> buckets;  // hash(band) → список docId

    public LSH(final int numHashFunctions, final double threshold) {
        this.numHashFunctions = numHashFunctions;
        this.rowsPerBand = 3;
        this.numBands = numHashFunctions / rowsPerBand;
        this.buckets = new HashMap<>();
    }

    public void insert(final String docId, final MinHashSignature sig) {
        final int[] values = sig.getSignature();

        for (int band = 0; band < numBands; band++) {
            final StringBuilder bandKeyBuilder = new StringBuilder();
            for (int row = 0; row < rowsPerBand; row++) {
                final int idx = band * rowsPerBand + row;
                bandKeyBuilder.append(values[idx]).append(",");
            }
            final String bandKey = bandKeyBuilder.toString();

            buckets.computeIfAbsent(bandKey, k -> new ArrayList<>()).add(docId);
        }
    }

    public Set<String> query(final String docId, final MinHashSignature sig) {
        final Set<String> candidates = new HashSet<>();

        final int[] values = sig.getSignature();

        for (int band = 0; band < numBands; band++) {
            final StringBuilder bandKeyBuilder = new StringBuilder();
            for (int row = 0; row < rowsPerBand; row++) {
                final int idx = band * rowsPerBand + row;
                bandKeyBuilder.append(values[idx]).append(",");
            }
            final String bandKey = bandKeyBuilder.toString();

            final List<String> matches = buckets.getOrDefault(bandKey, Collections.emptyList());
            candidates.addAll(matches);
        }

        candidates.remove(docId);
        return candidates;
    }

    public static Set<String> shingleGenerator(final String text, final int k) {
        final String cleaned = text.toLowerCase().replaceAll("[^\\p{L}\\p{N}\\s]", " ");
        final Set<String> shingles = new HashSet<>();
        for (int i = 0; i <= cleaned.length() - k; i++) {
            final StringBuilder sb = new StringBuilder();
            for (int j = 0; j < k; j++) {
                if (j > 0) {
                    sb.append(" ");
                }
                sb.append(cleaned.charAt(i + j));
            }
            shingles.add(sb.toString());
        }
        return shingles;
    }

    public static MinHashSignature computeMinHash(final Set<String> shingles, final int numHashFunctions) {
        final MinHashSignature signature = new MinHashSignature(numHashFunctions);
        for (final String shingle : shingles) {
            signature.update(shingle);
        }
        return signature;
    }

}

