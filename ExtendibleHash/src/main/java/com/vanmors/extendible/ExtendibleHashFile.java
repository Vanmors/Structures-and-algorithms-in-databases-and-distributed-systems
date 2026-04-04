package com.vanmors.extendible;

import com.google.common.hash.Hashing;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class ExtendibleHashFile implements AutoCloseable {

    private static final int PAGE_SIZE = 4096;                // размер страницы

    private static final int MAX_ENTRIES_PER_BUCKET = 32;

    private static final int HEADER_PAGE_ID = 0;

    private static final String DB_FILE = "extendible_hash.db";

    static class Header {
        int globalDepth = 1;

        int nextFreePageId = 1;

        void writeTo(final ByteBuffer buf) {
            buf.putInt(globalDepth);
            buf.putInt(nextFreePageId);
        }

        void readFrom(final ByteBuffer buf) {
            globalDepth = buf.getInt();
            nextFreePageId = buf.getInt();
        }
    }

    static class BucketPage {
        int pageId;

        int localDepth;

        int entryCount;

        List<Entry> entries = new ArrayList<>();

        BucketPage(final int pageId, final int localDepth) {
            this.pageId = pageId;
            this.localDepth = localDepth;
        }

        boolean hasSpace() {
            return entryCount < MAX_ENTRIES_PER_BUCKET;
        }

        void add(final Entry e) {
            entries.add(e);
            entryCount++;
        }

        ByteBuffer serialize() {
            final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
            buf.putInt(localDepth);
            buf.putInt(entryCount);

            for (final Entry e : entries) {
                final byte[] kBytes = e.key.getBytes();
                final byte[] vBytes = e.value.getBytes();
                buf.putInt(kBytes.length);
                buf.put(kBytes);
                buf.putInt(vBytes.length);
                buf.put(vBytes);
            }
            buf.flip();
            return buf;
        }

        static BucketPage deserialize(final int pageId, final ByteBuffer buf) {
            final int localDepth = buf.getInt();
            final int count = buf.getInt();

            final BucketPage bp = new BucketPage(pageId, localDepth);
            bp.entryCount = count;

            for (int i = 0; i < count; i++) {
                final int kLen = buf.getInt();
                final byte[] k = new byte[kLen];
                buf.get(k);
                final int vLen = buf.getInt();
                final byte[] v = new byte[vLen];
                buf.get(v);

                bp.entries.add(new Entry(new String(k), new String(v)));
            }
            return bp;
        }
    }

    static class Entry {
        String key;

        String value;

        Entry(final String k, final String v) {
            this.key = k;
            this.value = v;
        }
    }


    private final RandomAccessFile raf;

    private final FileChannel channel;

    private Header header;

    private int[] directory;

    private final Map<Integer, BucketPage> pageCache = new HashMap<>();

    public ExtendibleHashFile() throws IOException {
        final File file = new File(DB_FILE);
        final boolean newFile = !file.exists();

        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();

        if (newFile) {
            initializeNewFile();
        } else {
            loadHeader();
        }
    }

    private void initializeNewFile() throws IOException {
        header = new Header();
        header.globalDepth = 1;
        header.nextFreePageId = 2;

        directory = new int[1 << header.globalDepth];
        Arrays.fill(directory, 1);

        final BucketPage firstBucket = new BucketPage(1, 0);
        writePage(firstBucket);

        writeHeaderAndDirectory();
    }

    private void loadHeader() throws IOException {
        final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        channel.read(buf, HEADER_PAGE_ID * PAGE_SIZE);
        buf.flip();
        header = new Header();
        header.readFrom(buf);
        final int directorySize = buf.getInt();
        directory = new int[directorySize];
        for (int i = 0; i < directorySize; i++) {
            directory[i] = buf.getInt();
        }
    }

    private void writeHeaderAndDirectory() throws IOException {
        final int neededSize = 8 + 4 + directory.length * 4;   // header + length + directory

        final ByteBuffer buf = ByteBuffer.allocate(neededSize);

        header.writeTo(buf);           // globalDepth + nextFreePageId
        buf.putInt(directory.length);  // размер директории
        for (final int j : directory) {
            buf.putInt(j);
        }

        buf.flip();
        channel.write(buf, HEADER_PAGE_ID * PAGE_SIZE);
    }

    private void writePage(final BucketPage bp) throws IOException {
        final ByteBuffer buf = bp.serialize();
        final long pos = (long) bp.pageId * PAGE_SIZE;
        channel.write(buf, pos);
        pageCache.put(bp.pageId, bp);
    }

    private BucketPage readPage(final int pageId) throws IOException {
        final BucketPage cached = pageCache.get(pageId);
        if (cached != null) {
            return cached;
        }

        final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        final long pos = (long) pageId * PAGE_SIZE;
        channel.read(buf, pos);
        buf.flip();

        final BucketPage bp = BucketPage.deserialize(pageId, buf);
        pageCache.put(pageId, bp);
        return bp;
    }

    private int getBucketIndex(final String key) {

        final long hash = Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asLong();
        final int mask = (1 << header.globalDepth) - 1;
        return (int) hash & mask;   // младшие биты как индекс
    }

    public void put(final String key, final String value) throws IOException {
        final int idx = getBucketIndex(key);
        final int bucketId = directory[idx];
        final BucketPage bucket = readPage(bucketId);

        if (bucket.hasSpace()) {
            bucket.entries.removeIf(e -> e.key.equals(key));
            bucket.add(new Entry(key, value));
            writePage(bucket);
            return;
        }

        // нужно расщепление
        split(bucket);
        // после расщепления повторяем вставку
        put(key, value);
    }

    public void remove(final String key) throws IOException {
        final int idx = getBucketIndex(key);
        final int bucketId = directory[idx];
        final BucketPage bucket = readPage(bucketId);

        bucket.entries.removeIf(e -> e.key.equals(key));
    }

    private void split(final BucketPage bucket) throws IOException {
        final int oldLocalDepth = bucket.localDepth;

        if (oldLocalDepth < header.globalDepth) {
            // простой split — только перераспределяем
            final BucketPage newBucket = createNewBucket(oldLocalDepth + 1);

            redistributeEntries(bucket, newBucket);

            final int mask = 1 << oldLocalDepth;  // бит, по которому расщепляем
            for (int i = 0; i < directory.length; i++) {
                if (directory[i] == bucket.pageId) {
                    // Смотрим на бит oldLocalDepth в индексе i
                    if ((i & mask) != 0) {  // бит = 1 → новая страница
                        directory[i] = newBucket.pageId;
                    }
                }
            }

            bucket.localDepth++;
            writePage(bucket);
            writePage(newBucket);
        } else {
            // удвоение directory
            doubleDirectory();
            // после удвоения индекс может измениться — пересчитываем
            split(bucket);
        }
    }

    private void doubleDirectory() {
        final int oldSize = 1 << header.globalDepth;
        final int[] newDir = new int[oldSize * 2];
        for (int i = 0; i < oldSize; i++) {
            newDir[i] = directory[i];
            newDir[i + oldSize] = directory[i];
        }
        directory = newDir;
        header.globalDepth++;
    }

    private BucketPage createNewBucket(final int localDepth) throws IOException {
        final int newPageId = header.nextFreePageId++;
        final BucketPage bp = new BucketPage(newPageId, localDepth);
        writePage(bp);
        return bp;
    }

    private void redistributeEntries(final BucketPage oldBucket, final BucketPage newBucket) {
        final Iterator<Entry> it = oldBucket.entries.iterator();
        while (it.hasNext()) {
            final Entry e = it.next();
            final int h = getBucketIndex(e.key);
            if ((h >> oldBucket.localDepth) % 2 == 1) {
                newBucket.add(e);
                it.remove();
                oldBucket.entryCount--;
            }
        }
    }

    public String get(final String key) throws IOException {
        final int idx = getBucketIndex(key);
        final int bucketId = directory[idx];
        final BucketPage bucket = readPage(bucketId);

        for (final Entry e : bucket.entries) {
            if (e.key.equals(key)) {
                return e.value;
            }
        }
        return null;
    }

    public void clearCache() {
        pageCache.clear();
    }

    public void close() throws IOException {
        writeHeaderAndDirectory();
        raf.close();
    }
}