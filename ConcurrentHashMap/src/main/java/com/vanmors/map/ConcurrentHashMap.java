package com.vanmors.map;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;


public class ConcurrentHashMap<K, V> {
    private static final int DEFAULT_CAPACITY = 16;
    private static final int DEFAULT_SEGMENTS = 16;
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private Node<K, V>[] table;
    private final Segment[] segments;
    private final LongAdder[] counters;

    private final ReentrantLock lock = new ReentrantLock();

    private static final VarHandle NODE_ARRAY_HANDLE;

    private static class Segment {
        final ReentrantLock lock = new ReentrantLock();
    }

    static {
        try {
            NODE_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Node[].class);
        } catch (final Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public ConcurrentHashMap() {
        this(DEFAULT_CAPACITY);
    }

    public ConcurrentHashMap(final int initialCapacity) {
        final int capacity = tableSizeFor(initialCapacity);
        this.table = new Node[capacity];
        this.segments = new Segment[DEFAULT_SEGMENTS];
        this.counters = new LongAdder[DEFAULT_SEGMENTS];

        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment();
            counters[i] = new LongAdder();
        }
    }

    static class Node<K, V> {
        final int hash;

        final K key;

        volatile V value;

        Node<K, V> next;

        Node(final int hash, final K key, final V value) {
            this.hash = hash;
            this.key = key;
            this.value = value;
        }

        Node(final int hash, final K key, final V value, final Node<K, V> next) {
            this(hash, key, value);
            this.next = next;
        }
    }

    public V get(final K key) {
        final Node<K,V>[] tab = table;
        if (tab == null) return null;
        final int hash = spread(key.hashCode());

        Node<K,V> node = tabAt(tab, hash & (tab.length - 1));

        while (node != null) {
            if (node.hash == hash && key.equals(node.key)) {
                return node.value;
            }
            node = node.next;
        }
        return null;
    }

    public V put(final K key, final V value) {
        // TODO implement put with lock on segment
        if (key == null || value == null) throw new NullPointerException();
        final int hash = spread(key.hashCode());
        int segmentIndex = hash & (segments.length - 1);
        Segment segment = segments[segmentIndex];

        V oldValue = null;

        segment.lock.lock();
        try {
            if (table == null) {
                table = new Node[DEFAULT_CAPACITY];
            }
            int index = hash & (table.length - 1);
            Node<K, V> node = tabAt(table, index);

            while (node != null) {
                if (node.hash == hash && node.key.equals(key)) {
                    oldValue = node.value;
                    node.value = value;
                    return oldValue;
                }
            }


        } finally {
            segment.lock.unlock();
        }
        return oldValue;
    }

    public long size() {
        long count = 0;
        for (final LongAdder counter: counters) {
            count += counter.sum();
        }
        return count;
    }

    /**
     * Аналог tabAt() — атомарное чтение элемента массива
     * (с Acquire семантикой — как volatile read)
     */
    @SuppressWarnings("unchecked")
    private static <K,V> Node<K,V> tabAt(final Node<K, V>[] table, final int i) {
        return (Node<K,V>) NODE_ARRAY_HANDLE.getAcquire(table, i);
    }

    /**
     * Аналог casTabAt() — Compare-And-Swap
     */
    private static <K,V> boolean casTabAt(final Node<K, V>[] table, final int i, final Node<K, V> expected, final Node<K, V> update) {
        return NODE_ARRAY_HANDLE.compareAndSet(table, i, expected, update);
    }

    /**
     * Аналог setTabAt() — запись с Release семантикой
     * (используется после того, как мы взяли блокировку)
     */
    private static <K,V> void setTabAt(final Node<K, V>[] table, final int i, final Node<K, V> value) {
        NODE_ARRAY_HANDLE.setRelease(table, i, value);
    }

    private static int spread(final int h) {
        return (h ^ (h >>> 16)) & 0x7fffffff;
    }

    private static int tableSizeFor(final int c) {
        final int n = -1 >>> Integer.numberOfLeadingZeros(c - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    private void incrementSize(final int segmentIndex) {
        counters[segmentIndex].increment();
    }

    private void decrementSize(final int segmentIndex) {
        counters[segmentIndex].decrement();
    }

}
