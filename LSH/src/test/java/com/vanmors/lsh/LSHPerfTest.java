package com.vanmors.lsh;

import org.instancio.Instancio;
import org.instancio.Select;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 3, jvmArgs = {"-Xms2g", "-Xmx4g"})
public class LSHPerfTest {

    @Param({"1000", "5000", "10000"})
    private int documentCount;

    private LSH lsh;

    private List<MinHashSignature> signatures;

    private List<String> docIds;

    @Setup(Level.Trial)
    public void setup() {
        lsh = new LSH(120, 0.75);

        // Генерируем документы
        final List<String> texts = Instancio.ofList(String.class)
                .size(documentCount)
                .generate(Select.allStrings(), gen -> gen.string().length(20, 80))
                .create();

        signatures = new ArrayList<>(documentCount);
        docIds = new ArrayList<>(documentCount);

        for (int i = 0; i < documentCount; i++) {
            final Set<String> shingles = LSH.shingleGenerator(texts.get(i), 3);
            final MinHashSignature sig = LSH.computeMinHash(shingles, 120);
            signatures.add(sig);
            docIds.add("doc" + i);
        }

        // Вставляем все документы
        for (int i = 0; i < documentCount; i++) {
            lsh.insert(docIds.get(i), signatures.get(i));
        }
    }

    @Benchmark
    public void insertAll(final Blackhole bh) {
        // Тест вставки
        final LSH tempLsh = new LSH(120, 0.75);
        for (int i = 0; i < documentCount; i++) {
            tempLsh.insert(docIds.get(i), signatures.get(i));
        }
        bh.consume(tempLsh);
    }

    @Benchmark
    public void queryRandom(final Blackhole bh) {
        final Random rnd = new Random(42);
        final int idx = rnd.nextInt(documentCount);
        final Set<String> candidates = lsh.query(docIds.get(idx), signatures.get(idx));
        bh.consume(candidates);
    }

    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(LSHPerfTest.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
