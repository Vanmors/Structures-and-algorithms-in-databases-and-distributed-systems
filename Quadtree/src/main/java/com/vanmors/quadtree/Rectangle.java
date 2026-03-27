package com.vanmors.quadtree;

public class Rectangle {
    final double x;

    final double y;

    final double width;

    final double height;

    public Rectangle(final double x, final double y, final double width, final double height) {
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }

    public boolean contains(final Point point) {
        return ((point.lng <= x + width && point.lng >= x) && (point.lat <= y + height && point.lat >= y));
    }


    public boolean intersects(final Rectangle other) {
        return !(other.x > x + width || other.x + other.width < x ||
                other.y > y + height || other.y + other.height < y);
    }

}
