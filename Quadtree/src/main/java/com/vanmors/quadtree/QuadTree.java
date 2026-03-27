package com.vanmors.quadtree;

import java.util.ArrayList;
import java.util.List;


public class QuadTree {

    private final Rectangle boundary;

    private final int capacity; // максимум точек в листе

    private final List<Point> points = new ArrayList<>();

    private boolean divided = false;

    private QuadTree northWest;

    private QuadTree northEast;

    private QuadTree southWest;

    private QuadTree southEast;


    public QuadTree(final Rectangle boundary, final int capacity) {
        this.boundary = boundary;
        this.capacity = capacity;
    }


    public boolean insert(final Point point) {
        if (!boundary.contains(point)) {
            return false;
        }

        if (points.size() < capacity && !divided) {
            points.add(point);
            return true;
        }

        if (!divided) {
            subdivide();
        }

        return (northWest.insert(point) ||
                northEast.insert(point) ||
                southWest.insert(point) ||
                southEast.insert(point));

    }

    public List<Point> queryRange(final Rectangle range) {
        final List<Point> found = new ArrayList<>();

        if (!boundary.intersects(range)) {
            return found;
        }

        if (divided) {
            found.addAll(northWest.queryRange(range));
            found.addAll(northEast.queryRange(range));
            found.addAll(southWest.queryRange(range));
            found.addAll(southEast.queryRange(range));
        } else {
            for (final Point p : points) {
                if (range.contains(p)) {
                    found.add(p);
                }
            }
        }
        return found;
    }

    private void subdivide() {
        final double x = boundary.x;
        final double y = boundary.y;
        final double w = boundary.width / 2;
        final double h = boundary.height / 2;

        northWest = new QuadTree(new Rectangle(x, y + h, w, h), capacity);
        northEast = new QuadTree(new Rectangle(x + w, y + h, w, h), capacity);
        southWest = new QuadTree(new Rectangle(x, y, w, h), capacity);
        southEast = new QuadTree(new Rectangle(x + w, y, w, h), capacity);

        divided = true;

        // Перераспределяем существующие точки
        for (final Point p : points) {
            northWest.insert(p);
            northEast.insert(p);
            southWest.insert(p);
            southEast.insert(p);
        }

        points.clear();
    }


    public List<Point> queryRadius(final double lat, final double lng, final double radiusKm) {
        final double deltaLat = radiusKm / 111.0;
        final double deltaLng = radiusKm / (111.0 * Math.cos(Math.toRadians(lat)));

        final Rectangle approxRange = new Rectangle(
                lng - deltaLng,
                lat - deltaLat,
                deltaLng * 2,
                deltaLat * 2
        );

        final List<Point> candidates = queryRange(approxRange);

        final List<Point> result = new ArrayList<>();
        for (final Point p : candidates) {
            if (haversine(lat, lng, p.lat, p.lng) <= radiusKm) {
                result.add(p);
            }
        }
        return result;
    }

    private double haversine(final double lat1, final double lon1, final double lat2, final double lon2) {
        final int R = 6371;
        final double dLat = Math.toRadians(lat2 - lat1);
        final double dLon = Math.toRadians(lon2 - lon1);
        final double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
