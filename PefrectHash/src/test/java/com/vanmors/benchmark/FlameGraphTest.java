package com.vanmors.benchmark;

import com.vanmors.iceberg.PerfectHash;
import org.instancio.Instancio;
import org.instancio.Select;

import java.util.List;


public class FlameGraphTest {

    public static void main(final String[] args) {

        final List<String> largeKeys = Instancio.ofList(String.class)
                .size(1000)
                .generate(Select.allStrings(), gen -> gen.string().length(5, 50))
                .create();

        final var phf = new PerfectHash(largeKeys);

        for (final var k : largeKeys) {
            System.out.printf("%s → %d%n", k, phf.getIndex(k, phf.getSeed()));
        }
    }
}
