package dev.ryone;

import dev.ryone.lsm.LSMTree;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        try (var db = new LSMTree()) {
            db.put("name", "Alice");
            db.put("email", "alice@example.com");
            
            System.out.println(db.get("name"));   // Alice
            System.out.println(db.get("email"));  // alice@example.com
            System.out.println(db.get("unknown")); // null
        }
    }
}
