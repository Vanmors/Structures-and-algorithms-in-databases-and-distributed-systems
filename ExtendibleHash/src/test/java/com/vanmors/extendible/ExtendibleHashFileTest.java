package com.vanmors.extendible;

import org.instancio.Instancio;
import org.instancio.Select;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class ExtendibleHashFileTest {

    private static final Path TEST_DB = Path.of("extendible_hash.db");

    @BeforeEach
    void cleanup() throws IOException {
        Files.deleteIfExists(TEST_DB);
    }

    @AfterEach
    void cleanupAfter() throws IOException {
        Files.deleteIfExists(TEST_DB);
    }

    @Test
    void putAndGetSingleEntry() throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            final String key = Instancio.of(String.class)
                    .generate(Select.allStrings(), gen -> gen.string().length(5, 20))
                    .create();
            final String value = "value-" + UUID.randomUUID();

            db.put(key, value);

            assertEquals(value, db.get(key));
            assertNull(db.get("non-existent-key"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 10, 20, 50})
    void putAndGetMultipleEntries(final int count) throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            final Map<String, String> expected = new HashMap<>();

            // Генерируем уникальные случайные ключи и значения
            final List<String> keys = Instancio.ofList(String.class)
                    .size(count)
                    .generate(Select.allStrings(), gen -> gen.string().length(8, 25))
                    .withUnique(Select.all(String.class))
                    .create();

            for (int i = 0; i < keys.size(); i++) {
                final String key = keys.get(i);
                final String value = "value-" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
                expected.put(key, value);
                db.put(key, value);
            }

            // Проверяем, что все сохранённые значения читаются правильно
            for (final Map.Entry<String, String> entry : expected.entrySet()) {
                assertEquals(entry.getValue(), db.get(entry.getKey()),
                        "Value mismatch for key: " + entry.getKey());
            }

            // Проверяем несуществующий ключ
            assertNull(db.get("completely-random-nonexistent-key-xyz"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 70, 150}) // значения, где уже должен происходить split
    void putManyEntries_shouldTriggerSplit(final int count) throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            final Map<String, String> expected = new HashMap<>();

            final List<String> keys = Instancio.ofList(String.class)
                    .size(count)
                    .generate(Select.allStrings(), gen -> gen.string().length(5, 15))
                    .withUnique(Select.all(String.class))
                    .create();

            for (int i = 0; i < keys.size(); i++) {
                final String key = keys.get(i);
                final String value = "v" + i;
                expected.put(key, value);
                db.put(key, value);
            }

            // Проверяем все ключи
            for (final Map.Entry<String, String> entry : expected.entrySet()) {
                assertEquals(entry.getValue(), db.get(entry.getKey()),
                        "Mismatch after many inserts for key: " + entry.getKey());
            }

            assertNull(db.get("non-existent-after-many"));
        }
    }

    @Test
    void putExistingKey_shouldOverwriteValue() throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            final String key = Instancio.create(String.class);
            final String oldValue = "old-" + UUID.randomUUID();
            final String newValue = "new-" + UUID.randomUUID();

            db.put(key, oldValue);
            assertEquals(oldValue, db.get(key));

            db.put(key, newValue);
            assertEquals(newValue, db.get(key));
        }
    }

    @Test
    void getAfterRestart_shouldKeepData() throws IOException {
        final String k1 = Instancio.create(String.class);
        final String k2 = Instancio.create(String.class);
        final String k3 = Instancio.create(String.class);

        final String v1 = "v1-" + UUID.randomUUID();
        final String v2 = "v2-" + UUID.randomUUID();
        final String v3 = "v3-" + UUID.randomUUID();

        // Первая сессия — запись
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            db.put(k1, v1);
            db.put(k2, v2);
            db.put(k3, v3);
        }

        // Вторая сессия — чтение
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            assertEquals(v1, db.get(k1));
            assertEquals(v2, db.get(k2));
            assertEquals(v3, db.get(k3));
            assertNull(db.get("missing-key-" + UUID.randomUUID()));
        }
    }

    @Test
    void removeOneElement() throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {
            final String key = Instancio.create(String.class);
            final String value = "value-" + UUID.randomUUID();

            db.put(key, value);
            assertEquals(value, db.get(key));

            db.remove(key);
            assertNull(db.get(key));
        }
    }

}