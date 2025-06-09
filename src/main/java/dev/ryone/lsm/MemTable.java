package dev.ryone.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MemTable {
    private static final String SSTABLE_FILE_NAME_PREFIX = "sstable";
    private static final String SSTABLE_FILE_NAME_INFIX = "-";
    private static final String SSTABLE_FILE_NAME_EXT = ".db";
    private static final String KEY_VALUE_JOINER = "\t";

    private final NavigableMap<String, String> table = new TreeMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public final int size() {
        return table.size();
    }

    public final Optional<String> get(String key) {
        var item = table.get(key);
        return Optional.ofNullable(item);
    }

    public final synchronized void put(String key, String value) {
        table.put(key, value);
    }

    public final synchronized void flush() throws ExecutionException, InterruptedException {
        if (size() > 0) {
            writeToSSTable(snapshot()).get();
        }
        clear();
    }

    private synchronized NavigableMap<String, String> snapshot() {
        return new TreeMap<>(table);
    }

    private synchronized Future<?> writeToSSTable(NavigableMap<String, String> snapshot) {
        Logger logger = Logger.getLogger(MemTable.class.getName());

        return executor.submit(() -> {
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

            try {
                if (Files.notExists(path)) {
                    Files.createFile(path);
                }

                try (var writer = Files.newBufferedWriter(path)) {
                    for (var entry : snapshot.entrySet()) {
                        writer.write(entry.getKey() + KEY_VALUE_JOINER + entry.getValue());
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, String.format("Failed to write SSTable: %s", e.getMessage()));
            }
        });
    }

    public final synchronized void clear() {
        table.clear();
    }
}
