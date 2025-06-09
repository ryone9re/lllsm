package dev.ryone.lsm;

import dev.ryone.lib.KVS;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

public class LSMTree implements KVS<String, String>, AutoCloseable {
    private static final String WAL_FILE_NAME = "wa.log";
    private static final String KEY_VALUE_JOINER = "\t";
    private static final int TABLE_LIMIT = 1;

    private final MemTable table;
    private final BufferedWriter walWriter;

    public LSMTree() throws IOException {
        table = new MemTable();

        // WALを読み込んでtableに格納する
        var path = Paths.get(WAL_FILE_NAME);
        if (Files.notExists(path)) {
            Files.createFile(path);
        } else {
            try (Stream<String> lines = Files.lines(path)) {
                lines.forEach(s -> {
                    var split = s.split(KEY_VALUE_JOINER);
                    table.put(split[0], split[1]);
                });
            }
        }

        // WAL書き込みStreamを開く
        walWriter = new BufferedWriter(new FileWriter(WAL_FILE_NAME, true));
    }

    @Override
    public Optional<String> get(String key) {
        return table.get(key);
    }

    @Override
    public synchronized void put(String key, String value) throws IOException {
        walWriter.write(key + KEY_VALUE_JOINER + value);
        walWriter.newLine();
        walWriter.flush();

        table.put(key, value);

        if (table.size() >= TABLE_LIMIT) {
            tableFlush();
        }
    }

    private synchronized void tableFlush() throws IOException {
        table.flush();
    }

    @Override
    public void close() throws IOException {
        walWriter.close();
        tableFlush();
    }
}
