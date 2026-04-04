package com.vanmors.quadtree;

import ch.hsr.geohash.GeoHash;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 3, jvmArgs = {"-Xms2g", "-Xmx4g"})
public class QuadTreeInsertPerfTest {

    @Param({"1000", "10000", "50000", "100000", "500000"})
    private int size;

    @Benchmark
    public void insertAll_QuadTree() {
        final List<Point> points = generateClusteredPoints(size);

        final QuadTree temp = new QuadTree(new Rectangle(-180, -90, 360, 180), 8);

        for (final Point p : points) {
            temp.insert(p);
        }
    }

    @Benchmark
    public void insert_GeohashTreeMap() {
        final TreeMap<String, List<Point>> index = new TreeMap<>();
        final List<Point> points = generateClusteredPoints(size);
        for (final Point p : points) {
            final String hash = GeoHash.withCharacterPrecision(p.lat, p.lng, 7).toBase32();
            index.computeIfAbsent(hash, k -> new ArrayList<>()).add(p);
        }
    }

    private List<Point> generateClusteredPoints(final int count) {
        final Random rnd = new Random(42);
        final List<Point> points = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            final double lat = 55.7558 + (rnd.nextDouble() - 0.5) * 0.5;
            final double lng = 37.6173 + (rnd.nextDouble() - 0.5) * 0.7;
            points.add(new Point(lng, lat));
        }
        return points;
    }

    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(QuadTreeInsertPerfTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}