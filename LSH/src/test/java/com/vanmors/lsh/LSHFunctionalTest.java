package com.vanmors.lsh;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class LSHFunctionalTest {

    private LSH lsh;

    private Map<String, MinHashSignature> signatures;

    private final int numHashFunctions = 100;

    @BeforeEach
    void setUp() {
        lsh = new LSH(numHashFunctions, 0.75);
        signatures = new HashMap<>();
    }

    @Test
    void shouldFindNearDuplicates() {
        final String doc1 = "кошка ловит мышь в доме каждый день";
        final String doc2 = "кошка ловит мышку дома каждый вечер"; // почти дубликат
        final String doc3 = "кошка ловит мышку дома каждый день";
        final String doc4 = "собака бегает по парку с мячом";

        insertDoc("doc1", doc1);
        insertDoc("doc2", doc2);
        insertDoc("doc3", doc3);
        insertDoc("doc4", doc4);

        final Set<String> candidates = query("doc1");

        assertTrue(candidates.contains("doc2"), "Должен найти почти-дубликат");
        assertFalse(candidates.contains("doc4"), "Не должен находить совсем другой документ");
    }

    @Test
    void emptyText() {
        insertDoc("empty", "");
        assertTrue(query("empty").isEmpty());
    }

    @Test
    void similarTexts() {
        // Полные дубликаты
        insertDoc("dup1", "это одинаковый текст");
        insertDoc("dup2", "это одинаковый текст");
        assertTrue(query("dup1").contains("dup2"));
    }

    private void insertDoc(final String id, final String text) {
        final Set<String> shingles = LSH.shingleGenerator(text, 3);
        final MinHashSignature sig = LSH.computeMinHash(shingles, numHashFunctions);
        signatures.put(id, sig);
        lsh.insert(id, sig);
    }

    private Set<String> query(final String docId) {
        final MinHashSignature querySig = signatures.get(docId);
        return lsh.query(docId, querySig);
    }
}