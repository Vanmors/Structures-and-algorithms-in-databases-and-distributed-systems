package com.vanmors.benchmark;

import com.vanmors.iceberg.PerfectHash;
import org.instancio.Instancio;
import org.instancio.Select;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PerfectHashFunctionalTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 1000, 10000})
    void shouldReturnUniqueIndicesFrom0ToN(final int size) {
        final List<String> keys = Instancio.ofList(String.class)
                .size(size)
                .generate(Select.allStrings(), gen -> gen.string().length(5, 50))
                .create();

        final PerfectHash phf = new PerfectHash(keys);

        final Set<Integer> indices = new HashSet<>();
        for (final String key : keys) {
            final int idx = phf.getIndex(key, phf.getSeed());
            assertTrue(idx >= 0 && idx < keys.size(), "Индекс вне диапазона: " + idx);
            assertTrue(indices.add(idx), "Коллизия. Индекс " + idx + " уже использован");
        }

        assertEquals(keys.size(), indices.size());
    }

    @Test
    void emptyListAndOneElementList() {
        assertDoesNotThrow(() -> new PerfectHash(List.of()));
    }


    @Test
    void onElementList() {
        final PerfectHash ph = new PerfectHash(List.of("single"));
        assertEquals(0, ph.getIndex("single", ph.getSeed()));
    }

    @Test
    void unicodeAndLongStrings() {
        final List<String> weird = Instancio.ofList(String.class)
                .size(500)
                .generate(Select.allStrings(), gen -> gen.string().length(100, 500))
                .create();

        final PerfectHash phf = new PerfectHash(weird);
        assertDoesNotThrow(() -> phf.getIndex(weird.get(0), phf.getSeed()));
    }
}