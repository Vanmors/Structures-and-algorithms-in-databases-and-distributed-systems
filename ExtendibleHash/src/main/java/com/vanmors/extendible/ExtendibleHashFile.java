package com.vanmors.extendible;

import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class ExtendibleHashFile implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExtendibleHashFile.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int HEADER_PAGE_ID = 0;

    // --- настраиваемые параметры ---
    private final int pageSize;

    private final int maxEntriesPerBucket;

    private final int cacheSize;

    private final String dbFile;

    private final int initialGlobalDepth;

    public static class Builder {
        private int pageSize = 16384;

        private int maxEntriesPerBucket = 128;

        private int cacheSize = 128;

        private String dbFile = "extendible_hash.db";

        private int initialGlobalDepth = 1;

        public Builder pageSize(final int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder maxEntriesPerBucket(final int maxEntriesPerBucket) {
            this.maxEntriesPerBucket = maxEntriesPerBucket;
            return this;
        }

        public Builder cacheSize(final int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder dbFile(final String dbFile) {
            this.dbFile = dbFile;
            return this;
        }

        public Builder initialGlobalDepth(final int initialGlobalDepth) {
            this.initialGlobalDepth = initialGlobalDepth;
            return this;
        }

        public ExtendibleHashFile build() throws IOException {
            return new ExtendibleHashFile(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

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

        int entryCount = 0;

        List<Entry> entries = new ArrayList<>();

        BucketPage(final int pageId, final int localDepth) {
            this.pageId = pageId;
            this.localDepth = localDepth;
        }

        boolean hasSpace(final int maxEntriesPerBucket) {
            return entryCount < maxEntriesPerBucket;
        }

        void add(final Entry e) {
            entries.add(e);
            entryCount++;
        }

        ByteBuffer serialize(final int pageSize) {
            final ByteBuffer buf = ByteBuffer.allocate(pageSize);
            buf.putInt(localDepth);
            buf.putInt(entryCount);

            for (final Entry e : entries) {
                final byte[] kBytes = e.key.getBytes();
                final byte[] vBytes = e.value.getBytes();
                final int needed = Integer.BYTES * 2 + kBytes.length + vBytes.length;
                if (buf.remaining() < needed) {
                    throw new IllegalStateException(
                            "Страница переполнена: pageSize=%d, remaining=%d, needed=%d, key=%s"
                                    .formatted(pageSize, buf.remaining(), needed, e.key));
                }
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

    private final Map<Integer, BucketPage> pageCache;

    /**
     * Конструктор с параметрами по умолчанию.
     */
    public ExtendibleHashFile() throws IOException {
        this(new Builder());
    }

    private ExtendibleHashFile(final Builder b) throws IOException {
        this.pageSize = b.pageSize;
        this.maxEntriesPerBucket = b.maxEntriesPerBucket;
        this.cacheSize = b.cacheSize;
        this.dbFile = b.dbFile;
        this.initialGlobalDepth = b.initialGlobalDepth;

        final File file = new File(dbFile);
        final boolean newFile = !file.exists();

        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();

        this.pageCache = new LinkedHashMap<>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Integer, BucketPage> eldest) {
                return size() > cacheSize;
            }
        };

        try (final var fileLock = channel.lock()) {
            final boolean empty = channel.size() == 0;
            if (empty) {
                initializeNewFile();
            } else {
                loadHeader();
            }
        }
    }

    private void initializeNewFile() throws IOException {
        header = new Header();
        header.globalDepth = initialGlobalDepth;
        header.nextFreePageId = 2;

        directory = new int[1 << header.globalDepth];
        Arrays.fill(directory, 1);

        final BucketPage firstBucket = new BucketPage(1, 0);
        writePage(firstBucket);

        writeHeaderAndDirectory();
    }

    private void loadHeader() throws IOException {
        final ByteBuffer buf = ByteBuffer.allocate(pageSize);
        channel.read(buf, HEADER_PAGE_ID * pageSize);
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
        channel.write(buf, HEADER_PAGE_ID * pageSize);
    }

    private void writePage(final BucketPage bp) throws IOException {
        final ByteBuffer buf = bp.serialize(pageSize);
        final long pos = (long) bp.pageId * pageSize;
        channel.write(buf, pos);
        pageCache.put(bp.pageId, bp);
    }

    private BucketPage readPage(final int pageId) throws IOException {
        final BucketPage cached = pageCache.get(pageId);
        if (cached != null) {
            return cached;
        }

        final ByteBuffer buf = ByteBuffer.allocate(pageSize);
        final long pos = (long) pageId * pageSize;
        channel.read(buf, pos);
        buf.flip();

        final BucketPage bp = BucketPage.deserialize(pageId, buf);
        pageCache.put(pageId, bp);
        return bp;
    }


    public void put(final String key, final String value) throws IOException {
        final int idx = getBucketIndex(key);
        final int bucketId = directory[idx];
        final BucketPage bucket = readPage(bucketId);

        if (bucket.hasSpace(maxEntriesPerBucket)) {
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

        if (oldLocalDepth == header.globalDepth) {
            doubleDirectory();
        }

        // увеличиваем depth
        bucket.localDepth++;

        // создаём новый bucket
        final BucketPage newBucket = createNewBucket(bucket.localDepth);

        // обновляем directory
        final int mask = 1 << (bucket.localDepth - 1);

        for (int i = 0; i < directory.length; i++) {
            if (directory[i] == bucket.pageId && (i & mask) != 0) {
                directory[i] = newBucket.pageId;
            }
        }

        // redistribute
        redistributeEntries(bucket, newBucket);

        writePage(bucket);
        writePage(newBucket);
    }

    private long hash(final String key) {
        return Hashing.murmur3_128()
                .hashString(key, StandardCharsets.UTF_8)
                .asLong();
    }

    private int getBucketIndex(final String key) {
        final long h = hash(key);
        final int mask = (1 << header.globalDepth) - 1;
        return (int) h & mask;
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
        final int splitBit = oldBucket.localDepth - 1;

        final Iterator<Entry> it = oldBucket.entries.iterator();
        while (it.hasNext()) {
            final Entry e = it.next();

            final long h = hash(e.key);

            if ((h & (1L << splitBit)) != 0) {
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

    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            writeHeaderAndDirectory();
            raf.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}