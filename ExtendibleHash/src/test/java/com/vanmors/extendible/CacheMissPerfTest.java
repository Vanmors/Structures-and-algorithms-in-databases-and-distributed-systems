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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsPrepend = {"-Xms2g", "-Xmx4g"})
public class CacheMissPerfTest {

    private static final Path DB_PATH = Path.of("cache_miss_bench.db");
    private static final int MAX_ENTRIES_PER_BUCKET = 32;
    private static final int DATA_SIZE = 3000;

    @Param({"1", "16", "128", "1024"})
    private int cacheSize;

    private ExtendibleHashFile db;
    private List<String> keys;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        Files.deleteIfExists(DB_PATH);

        keys = Instancio.ofList(String.class)
                .size(DATA_SIZE)
                .generate(Select.allStrings(), gen -> gen.string().length(5, 15).digits())
                .withUnique(Select.all(String.class))
                .create();

        db = ExtendibleHashFile.builder()
                .dbFile(DB_PATH.toString())
                .maxEntriesPerBucket(MAX_ENTRIES_PER_BUCKET)
                .cacheSize(cacheSize)
                .build();

        for (int i = 0; i < DATA_SIZE; i++) {
            db.put(keys.get(i), "v" + keys.get(i));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        db.close();
        Files.deleteIfExists(DB_PATH);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public void getRandom(final Blackhole bh) throws IOException {
        final String key = keys.get(ThreadLocalRandom.current().nextInt(DATA_SIZE));
        final String value = db.get(key);
        bh.consume(value);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public void insert() throws IOException {
        final int idx = ThreadLocalRandom.current().nextInt(DATA_SIZE);
        db.put(keys.get(idx), "updated-" + keys.get(idx));
    }

    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(CacheMissPerfTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
