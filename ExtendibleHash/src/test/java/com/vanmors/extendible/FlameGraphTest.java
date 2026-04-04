package com.vanmors.extendible;

import org.instancio.Instancio;
import org.instancio.Select;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


public class FlameGraphTest {
    public static void main(String[] args) throws IOException {
        try (final ExtendibleHashFile db = new ExtendibleHashFile()) {

            final List<String> keys = Instancio.ofList(String.class)
                    .size(150)
                    .generate(Select.allStrings(), gen -> gen.string().length(8, 25))
                    .withUnique(Select.all(String.class))
                    .create();

            for (int i = 0; i < keys.size(); i++) {
                final String key = keys.get(i);
                final String value = "value-" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
                db.put(key, value);
            }
        }
    }
}
