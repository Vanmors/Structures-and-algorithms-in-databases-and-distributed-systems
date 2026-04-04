package com.vanmors.lsh;

import java.util.Arrays;


public class MinHashSignature {
    private final int[] signature;
    private final int numHashFunctions;

    public MinHashSignature(final int numHashFunctions) {
        this.numHashFunctions = numHashFunctions;
        this.signature = new int[numHashFunctions];
        Arrays.fill(signature, Integer.MAX_VALUE);
    }

    public void update(final String shingle) {
        for (int i = 0; i < numHashFunctions; i++) {

            final int shingleHash = Hash.hash(shingle, i);
            signature[i] = Math.min(signature[i], shingleHash);
        }
    }

    public int[] getSignature() {
        return signature;
    }

    public double jaccard(final MinHashSignature other) {
        int matches = 0;
        for (int i = 0; i < numHashFunctions; i++) {
            if (signature[i] == other.signature[i]) matches++;
        }
        return (double) matches / numHashFunctions;
    }
}
