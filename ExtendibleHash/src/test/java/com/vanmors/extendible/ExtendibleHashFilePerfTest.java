package com.vanmors.extendible;

import org.instancio.Instancio;
import org.instancio.Select;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(value = 3, jvmArgsPrepend = {"-Xms2g", "-Xmx4g"})
public class ExtendibleHashFilePerfTest {

    private static final Path DB_PATH = Path.of("bench_extendable.db");

//    @Param({"1000", "10000", "50000", "100000"})
    @Param({"1000", "10000", "50000"})
    private int size;

    private ExtendibleHashFile db;
    private List<String> keys;
    private List<String> values;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Files.deleteIfExists(DB_PATH);

        keys = Instancio.ofList(String.class)
                .size(size)
                .generate(Select.allStrings(), gen -> gen.string().length(8, 30).digits())
                .withUnique(Select.all(String.class))
                .create();

        values = Instancio.ofList(String.class)
                .size(size)
                .generate(Select.allStrings(), gen -> gen.string().length(10, 50))
                .create();

        db = new ExtendibleHashFile();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        db.close();
        Files.deleteIfExists(DB_PATH);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertAll() throws IOException {
        for (int i = 0; i < size; i++) {
            db.put(keys.get(i), values.get(i));
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public void getRandom(final Blackhole bh) throws IOException {
        final String key = keys.get(ThreadLocalRandom.current().nextInt(size));
        final String value = db.get(key);
        bh.consume(value);
    }

    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(ExtendibleHashFilePerfTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}