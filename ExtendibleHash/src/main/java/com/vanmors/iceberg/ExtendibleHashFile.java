package com.vanmors.iceberg;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class ExtendibleHashFile {

    // ========================================================================
    //  Параметры (можно менять)
    // ========================================================================
    private static final int PAGE_SIZE = 4096;                // размер страницы
    private static final int MAX_ENTRIES_PER_BUCKET = 100;    // макс. записей в бакете
    private static final int HEADER_PAGE_ID = 0;
    private static final String DB_FILE = "extendible_hash.db";

    // ========================================================================
    //  Структура заголовка (первая страница файла)
    // ========================================================================
    static class Header {
        int globalDepth = 1;
        int nextFreePageId = 1;           // следующая свободная страница
        int directoryStartPageId = -1;    // пока не используется (directory в RAM)

        void writeTo(ByteBuffer buf) {
            buf.putInt(globalDepth);
            buf.putInt(nextFreePageId);
        }

        void readFrom(ByteBuffer buf) {
            globalDepth = buf.getInt();
            nextFreePageId = buf.getInt();
        }
    }

    // ========================================================================
    //  Бакет (страница на диске)
    // ========================================================================
    static class BucketPage {
        int pageId;
        int localDepth;
        int entryCount;
        List<Entry> entries = new ArrayList<>();

        BucketPage(int pageId, int localDepth) {
            this.pageId = pageId;
            this.localDepth = localDepth;
        }

        boolean hasSpace() {
            return entryCount < MAX_ENTRIES_PER_BUCKET;
        }

        void add(Entry e) {
            entries.add(e);
            entryCount++;
        }

        // сериализация в байты (упрощённая)
        ByteBuffer serialize() {
            ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
            buf.putInt(localDepth);
            buf.putInt(entryCount);

            for (Entry e : entries) {
                byte[] kBytes = e.key.getBytes();
                byte[] vBytes = e.value.getBytes();
                buf.putInt(kBytes.length);
                buf.put(kBytes);
                buf.putInt(vBytes.length);
                buf.put(vBytes);
            }
            buf.flip();
            return buf;
        }

        static BucketPage deserialize(int pageId, ByteBuffer buf) {
            int localDepth = buf.getInt();
            int count = buf.getInt();

            BucketPage bp = new BucketPage(pageId, localDepth);
            bp.entryCount = count;

            for (int i = 0; i < count; i++) {
                int kLen = buf.getInt();
                byte[] k = new byte[kLen];
                buf.get(k);
                int vLen = buf.getInt();
                byte[] v = new byte[vLen];
                buf.get(v);

                bp.entries.add(new Entry(new String(k), new String(v)));
            }
            return bp;
        }
    }

    static class Entry {
        String key;
        String value;

        Entry(String k, String v) {
            this.key = k;
            this.value = v;
        }
    }

    // ========================================================================
    //  Основной класс
    // ========================================================================
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private Header header;
    private int[] directory;               // pageId для каждого префикса
    private final Map<Integer, BucketPage> pageCache = new HashMap<>();  // простой кэш

    public ExtendibleHashFile() throws IOException {
        File file = new File(DB_FILE);
        boolean newFile = !file.exists();

        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();

        if (newFile) {
            initializeNewFile();
        } else {
            loadHeader();
//            loadDirectoryFromHeader();  // в варианте 1 directory в RAM → можно сохранять/загружать
        }
    }

    private void initializeNewFile() throws IOException {
        header = new Header();
        header.globalDepth = 1;
        header.nextFreePageId = 2;  // 0 - header, 1 - первый бакет

        directory = new int[1 << header.globalDepth];
        Arrays.fill(directory, 1);  // оба указателя → на первый бакет

        // создаём первый бакет
        BucketPage firstBucket = new BucketPage(1, 0);
        writePage(firstBucket);

        writeHeaderAndDirectory();
    }

    private void loadHeader() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        channel.read(buf, HEADER_PAGE_ID * PAGE_SIZE);
        buf.flip();
        header = new Header();
        header.readFrom(buf);
    }

    // В варианте 1 directory хранится в памяти, но для восстановления можно сохранять
    private void writeHeaderAndDirectory() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        header.writeTo(buf);

        // можно добавить сохранение directory, если нужно персистентность
        // здесь упрощаем — при перезапуске пересоздаём из бакетов (не идеально)

        buf.flip();
        channel.write(buf, HEADER_PAGE_ID * PAGE_SIZE);
    }

    private void writePage(BucketPage bp) throws IOException {
        ByteBuffer buf = bp.serialize();
        long pos = (long) bp.pageId * PAGE_SIZE;
        channel.write(buf, pos);
        pageCache.put(bp.pageId, bp);
    }

    private BucketPage readPage(int pageId) throws IOException {
        BucketPage cached = pageCache.get(pageId);
        if (cached != null) return cached;

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        long pos = (long) pageId * PAGE_SIZE;
        channel.read(buf, pos);
        buf.flip();

        BucketPage bp = BucketPage.deserialize(pageId, buf);
        pageCache.put(pageId, bp);
        return bp;
    }

    private int getBucketIndex(String key) {
        int hash = key.hashCode();
        int mask = (1 << header.globalDepth) - 1;
        return hash & mask;   // младшие биты как индекс (можно и старшие)
    }

    public void put(String key, String value) throws IOException {
        int idx = getBucketIndex(key);
        int bucketId = directory[idx];
        BucketPage bucket = readPage(bucketId);

        if (bucket.hasSpace()) {
            bucket.add(new Entry(key, value));
            writePage(bucket);
            return;
        }

        // нужно расщепление
        split(bucket, idx);
        // после расщепления повторяем вставку
        put(key, value);
    }

    private void split(BucketPage bucket, int directoryIndex) throws IOException {
        int oldLocalDepth = bucket.localDepth;

        if (oldLocalDepth < header.globalDepth) {
            // простой split — только перераспределяем
            BucketPage newBucket = createNewBucket(oldLocalDepth + 1);

            redistributeEntries(bucket, newBucket);

            // обновляем directory — все указатели с префиксом, где следующий бит = 1
            int step = 1 << (header.globalDepth - oldLocalDepth - 1);
            for (int i = directoryIndex; i < (1 << header.globalDepth); i += (step << 1)) {
                for (int j = 0; j < step; j++) {
                    if ((i + j) < directory.length && directory[i + j] == bucket.pageId) {
                        if ((i + j) % (step << 1) >= step) {
                            directory[i + j] = newBucket.pageId;
                        }
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
            split(bucket, getBucketIndex(keyFromBucket(bucket))); // упрощённо
        }
    }

    private void doubleDirectory() {
        int oldSize = 1 << header.globalDepth;
        int[] newDir = new int[oldSize * 2];
        for (int i = 0; i < oldSize; i++) {
            newDir[i] = directory[i];
            newDir[i + oldSize] = directory[i];
        }
        directory = newDir;
        header.globalDepth++;
    }

    private BucketPage createNewBucket(int localDepth) throws IOException {
        int newPageId = header.nextFreePageId++;
        BucketPage bp = new BucketPage(newPageId, localDepth);
        writePage(bp);
        return bp;
    }

    private void redistributeEntries(BucketPage oldBucket, BucketPage newBucket) {
        Iterator<Entry> it = oldBucket.entries.iterator();
        while (it.hasNext()) {
            Entry e = it.next();
            int h = getBucketIndex(e.key);
            if ((h >> oldBucket.localDepth) % 2 == 1) {
                newBucket.add(e);
                it.remove();
                oldBucket.entryCount--;
            }
        }
    }

    // вспомогательный метод для поиска ключа из бакета (для примера)
    private String keyFromBucket(BucketPage b) {
        return b.entries.isEmpty() ? "" : b.entries.get(0).key;
    }

    public String get(String key) throws IOException {
        int idx = getBucketIndex(key);
        int bucketId = directory[idx];
        BucketPage bucket = readPage(bucketId);

        for (Entry e : bucket.entries) {
            if (e.key.equals(key)) {
                return e.value;
            }
        }
        return null;
    }

    public void close() throws IOException {
        writeHeaderAndDirectory();
        raf.close();
    }

    // Тест
    public static void main(String[] args) throws IOException {
        ExtendibleHashFile db = new ExtendibleHashFile();

        db.put("apple", "fruit");
        db.put("car", "vehicle");
        db.put("dog", "animal");
        db.put("cat", "pet");

        System.out.println(db.get("apple"));  // fruit
        System.out.println(db.get("dog"));    // animal

        db.close();
    }
}