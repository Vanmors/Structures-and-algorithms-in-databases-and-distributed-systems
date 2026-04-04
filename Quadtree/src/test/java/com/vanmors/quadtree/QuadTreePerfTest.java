package com.vanmors.quadtree;

import ch.hsr.geohash.GeoHash;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 4)
@Fork(value = 3, jvmArgs = {"-Xms2g", "-Xmx4g"})
public class QuadTreePerfTest {

    @Param({"1000", "10000", "50000", "100000"})
    private int size;

    private QuadTree quadTree;
    private List<Point> allPoints;
    private Point queryPoint;
    List<Point> points;

    private TreeMap<String, List<Point>> geohashIndex;

    @Setup(Level.Trial)
    public void setup() {
        List<Point> points = generateClusteredPoints(size);

        // 1. QuadTree
        quadTree = new QuadTree(new Rectangle(-180, -90, 360, 180), 8);
        for (Point p : points) {
            quadTree.insert(p);
        }

        // 2. Geohash + TreeMap
        geohashIndex = new TreeMap<>();
        for (Point p : points) {
            String hash = GeoHash.withCharacterPrecision(p.lat, p.lng, 7).toBase32();
            geohashIndex.computeIfAbsent(hash, k -> new ArrayList<>()).add(p);
        }

        queryPoint = points.get(points.size() / 2);
    }

    @Benchmark
    public void queryRadius_QuadTree(final Blackhole bh) {
        final List<Point> result = quadTree.queryRadius(queryPoint.lat, queryPoint.lng, 10.0);
        bh.consume(result);
    }

    @Benchmark
    public void geohashQuery(final Blackhole bh) {
        final String prefix = GeoHash.withCharacterPrecision(queryPoint.lat, queryPoint.lng, 7).toBase32();
        final String end = prefix + "~";

        final NavigableMap<String, List<Point>> range = geohashIndex.subMap(prefix, true, end, false);

        final List<Point> result = new ArrayList<>();
        for (final List<Point> list : range.values()) {
            result.addAll(list);
        }
        bh.consume(result);
    }

    private List<Point> generateClusteredPoints(final int count) {
        final Random rnd = new Random(42);
        final List<Point> points = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            final double lat = 55.7558 + (rnd.nextDouble() - 0.5) * 0.5;
            final double lng = 37.6173 + (rnd.nextDouble() - 0.5) * 0.7;
            points.add(new Point(lng, lat));
        }
        return points;
    }

    public static class Main {
        public static void main(final String[] args) throws Exception {
            final Options opt = new OptionsBuilder()
                    .include(QuadTreePerfTest.class.getSimpleName())
                    .build();

            new Runner(opt).run();
        }
    }
}
