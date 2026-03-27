package com.vanmors.quadtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;


public class QuadTreeTest {

    private QuadTree quadTree;

    @BeforeEach
    void setUp() {
        // Создаём QuadTree на всю Землю с capacity = 4
        final Rectangle world = new Rectangle(-180, -90, 360, 180);
        quadTree = new QuadTree(world, 4);
    }

    @Test
    void shouldInsertPoint() {
        final Point p = new Point(37.6173, 55.7558);

        final boolean inserted = quadTree.insert(p);

        assertTrue(inserted, "Точка должна быть успешно вставлена");
    }

    @Test
    void shouldFindPointInSmallRadius() {
        final Point moscow = new Point(37.6173, 55.7558);
        quadTree.insert(moscow);

        // Ищем в радиусе 1 км
        final List<Point> found = quadTree.queryRadius(55.7558, 37.6173, 1.0);

        assertEquals(1, found.size(), "Должна быть найдена ровно одна точка");
        assertEquals(moscow.lat, found.get(0).lat, 0.0001);
        assertEquals(moscow.lng, found.get(0).lng, 0.0001);
    }

    @Test
    void shouldFindNearbyAndIgnoreFarPoints() {
        quadTree.insert(new Point(37.6173, 55.7558));
        quadTree.insert(new Point(37.6250, 55.7610));
        quadTree.insert(new Point(37.6225, 55.7532));
        quadTree.insert(new Point(30.3141, 59.9386));

        final List<Point> found = quadTree.queryRadius(55.7558, 37.6173, 2.0); // 2 км

        assertEquals(3, found.size(), "Должны быть найдены 3 точки в радиусе 2 км");

        final boolean hasLatPoint = found.stream()
                .anyMatch(p -> Math.abs(p.lat - 59.9386) < 0.01);

        assertFalse(hasLatPoint, "Точка 30.3141, 59.9386 не должна попасть в радиус 2 км");
    }

    @Test
    void shouldReturnEmptyListWhenNoPointsInRadius() {
        quadTree.insert(new Point(30.3141, 59.9386));

        final List<Point> found = quadTree.queryRadius(55.7558, 37.6173, 10.0);

        assertTrue(found.isEmpty(), "Список должен быть пустым");
    }

    @Test
    void shouldHandleManyPoints() {
        final Random random = new Random(42);

        for (int i = 0; i < 500; i++) {
            final double lat = -90 + random.nextDouble() * 180;
            final double lng = -180 + random.nextDouble() * 360;
            quadTree.insert(new Point(lng, lat));
        }

        // Проверяем, что поиск не падает и возвращает какое-то количество точек
        final List<Point> found = quadTree.queryRadius(55.7558, 37.6173, 100.0);

        assertNotNull(found);
        assertTrue(found.size() <= 500);
        System.out.println("Найдено точек в радиусе 100 км: " + found.size());
    }

    @Test
    void shouldWorkWithCapacityOne() {
        final QuadTree smallCapacityTree = new QuadTree(
                new Rectangle(-180, -90, 360, 180), 1);

        smallCapacityTree.insert(new Point(37.6173, 55.7558));
        smallCapacityTree.insert(new Point(37.6250, 55.7610));

        final List<Point> found = smallCapacityTree.queryRadius(55.7558, 37.6173, 5.0);

        assertEquals(2, found.size());
    }
}

