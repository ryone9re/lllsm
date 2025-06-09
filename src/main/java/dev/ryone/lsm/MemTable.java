package dev.ryone.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public class MemTable {
    private static final String SSTABLE_FILE_NAME_PREFIX = "sstable";
    private static final String SSTABLE_FILE_NAME_INFIX = "-";
    private static final String SSTABLE_FILE_NAME_EXT = ".db";
    private static final String KEY_VALUE_JOINER = "\t";

    private final NavigableMap<String, String> table;

    public MemTable() {
        table = new TreeMap<>();
    }

    public final int size() {
        return table.size();
    }

    public final Optional<String> get(String key) {
        var item = table.get(key);
        return Optional.ofNullable(item);
    }

    public final synchronized boolean put(String key, String value) {
        table.put(key, value);
        return true;
    }

    public final synchronized void flush() throws IOException {
        if (size() > 0) {
            writeToSSTable(snapshot());
        }
        table.clear();
    }

    private synchronized NavigableMap<String, String> snapshot() {
        return new TreeMap<>(table);
    }

    private synchronized void writeToSSTable(NavigableMap<String, String> snapshot) throws IOException {
        var now = Instant.now();
        var second = now.getEpochSecond();
        var nano = now.getNano();

        var path = Paths.get(
                SSTABLE_FILE_NAME_PREFIX +
                        SSTABLE_FILE_NAME_INFIX +
                        second +
                        SSTABLE_FILE_NAME_INFIX +
                        nano +
                        SSTABLE_FILE_NAME_EXT
        );
        if (Files.notExists(path)) {
            Files.createFile(path);
        }

        try (var writer = Files.newBufferedWriter(path)) {
            for (var entry : snapshot.entrySet()) {
                writer.write(entry.getKey() + KEY_VALUE_JOINER + entry.getValue());
                writer.newLine();
            }
        }
    }

    public final synchronized void clear() {
        table.clear();
    }
}
