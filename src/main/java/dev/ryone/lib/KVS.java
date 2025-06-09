package dev.ryone.lib;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface KVS<K, V> {
    Optional<V> get(K key);

    void put(K key, V value) throws IOException, ExecutionException, InterruptedException;
}
