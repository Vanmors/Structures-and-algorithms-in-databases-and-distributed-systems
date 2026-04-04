package com.vanmors.benchmark;

import com.vanmors.iceberg.PerfectHash;
import org.instancio.Instancio;
import org.instancio.Select;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 3, jvmArgs = {"-Xms2G", "-Xmx2G"})
@State(Scope.Benchmark)
public class PerfectHashBenchmark {

    @Param({"100", "1000", "5000", "10000"})
    private int keyCount;

    private List<String> largeKeys;

    private PerfectHash phf;

    @Setup(Level.Trial)
    public void setup() {
        largeKeys = Instancio.ofList(String.class)
                .size(keyCount)
                .generate(Select.allStrings(), gen -> gen.string().length(5, 50))
                .create();

        final long start = System.nanoTime();
        phf = new PerfectHash(largeKeys);
        System.out.printf("Build time for %d keys: %.2f ms%n",
                keyCount, (System.nanoTime() - start) / 1_000_000.0);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void lookup(final Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < 1000; i++) {
            sum += phf.getIndex(largeKeys.get(i % keyCount), phf.getSeed());
        }
        bh.consume(sum);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void buildOnly() {
        new PerfectHash(largeKeys);
    }


    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(PerfectHashBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}