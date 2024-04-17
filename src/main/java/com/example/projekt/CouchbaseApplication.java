package com.example.projekt;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
@SpringBootApplication
@RestController
public class CouchbaseApplication {
    public static void main(String[] args) {
        // Konfiguracja połączenia z bazą danych Couchbase
        Cluster cluster = Cluster.connect("localhost", "root", "root");
        Collection collection = cluster.bucket("default").defaultCollection();

        // Ścieżka do pliku JSON z danymi
        String jsonFilePath = "MOCK_DATA.json";

        try {
            // Odczytaj zawartość pliku JSON
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(new File(jsonFilePath));

            // Iteruj przez każdy element w pliku JSON
            for (JsonNode node : rootNode) {
                // Konwertuj JsonNode na JsonObject
                JsonObject jsonObject = JsonObject.fromJson(node.toString());

                // Wstaw dane do bazy danych
                MutationResult result = collection.insert("person_" + jsonObject.get("id"), jsonObject);

                // Sprawdź wynik operacji
                if (result != null) {
                    System.out.println("Dane dla ID " + jsonObject.get("id") + " zostały zaimportowane pomyślnie.");
                } else {
                    System.out.println("Import danych dla ID " + jsonObject.get("id") + " nie powiódł się.");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Zamknij połączenie z bazą danych
            cluster.disconnect();
        }
    }
}
