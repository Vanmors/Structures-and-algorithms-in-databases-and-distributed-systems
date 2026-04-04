package com.vanmors.lsh;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;


public class MinHashSignature {
    private final int[] signature;
    private final int numHashFunctions;

    private final HashFunction[] HASH_FUNCTIONS;


    public MinHashSignature(final int numHashFunctions) {
        this.numHashFunctions = numHashFunctions;
        this.signature = new int[numHashFunctions];
        Arrays.fill(signature, Integer.MAX_VALUE);
        HASH_FUNCTIONS = new HashFunction[numHashFunctions];
        for (int i = 0; i < HASH_FUNCTIONS.length; i++) {
            HASH_FUNCTIONS[i] = Hashing.murmur3_128(i);
        }
    }

    public void update(final String shingle) {
        final byte[] bytes = shingle.getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < numHashFunctions; i++) {
            final int shingleHash = HASH_FUNCTIONS[i]
                    .hashBytes(bytes)
                    .asInt();

            if (shingleHash < signature[i]) {
                signature[i] = shingleHash;
            }
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
