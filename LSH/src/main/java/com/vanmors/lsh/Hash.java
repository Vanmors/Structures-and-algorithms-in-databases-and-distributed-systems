package com.vanmors.lsh;

public class Hash {
    public static int hash(final String s, final int seed) {
        long h = 0x517cc1b727220a95L ^ seed;
        for (int i = 0; i < s.length(); i++) {
            h ^= s.charAt(i);
            h *= 0x5bd1e995L;
            h ^= h >>> 47;
        }
        return (int) h;
    }
}
