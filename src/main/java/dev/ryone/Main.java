package dev.ryone;

import dev.ryone.lsm.LSMTree;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try (var db = new LSMTree()) {
            db.put("name", "Alice");
            db.put("email", "alice@example.com");

            System.out.println(db.get("name"));   // Alice
            System.out.println(db.get("email"));  // alice@example.com
            System.out.println(db.get("unknown")); // null
        }
    }
}
