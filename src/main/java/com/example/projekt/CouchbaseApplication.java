package com.example.projekt;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.couchbase.client.core.error.CouchbaseException;

import java.io.IOException;


@SpringBootApplication
@RestController
public class CouchbaseApplication {
    public static void main(String[] args) {
        String connectionString = "couchbase://127.0.0.1";
        String username = "test";
        String password = "password";
        String bucketName = "default";
        String collectionName = "_default";
        Cluster cluster = Cluster.connect(connectionString, username, password);
        Collection collection = cluster.bucket(bucketName).defaultCollection();

        try {
            FileWriter importWriter = new FileWriter("Couchbase_results/import_times.csv");
            FileWriter updateWriter = new FileWriter("Couchbase_results/update_times.csv");
            FileWriter deleteWriter = new FileWriter("Couchbase_results/delete_times.csv");
            importWriter.append("Iteration,Time (s)\n");
            updateWriter.append("Iteration,Time (s)\n");
            deleteWriter.append("Iteration,Time (s)\n");

            for (int i = 0; i < 10; i++) {
                System.out.println("Iteration: " + (i + 1));

                File jsonFile = new File("Couchbase/insert_1000.json");
                long importStartTime = System.nanoTime();
                importJsonData(collection, jsonFile);
                long importEndTime = System.nanoTime();
                float importElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(importEndTime - importStartTime) / 1000.0);
                importWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(importElapsedTimeMs)).append("\n");
                // System.out.println("Import czas: " + importElapsedTimeMs + " ms");
                System.out.println("zaiportowano");

                // Aktualizuj dane
                File jsonFileUpdate = new File("Couchbase/update_1000.json");
                long updateStartTime = System.nanoTime();
                updateJsonData(collection, jsonFileUpdate);
                long updateEndTime = System.nanoTime();
                float updateElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateStartTime) / 1000.0);
                updateWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(updateElapsedTimeMs)).append("\n");
                //System.out.println("Aktualizacja czas: " + updateElapsedTimeMs + " ms");
                System.out.println("zaktualizowano");

                // Usuń dane
                long deleteStartTime = System.nanoTime();
                deleteAllData(collection, cluster);
                long deleteEndTime = System.nanoTime();
                float deleteElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(deleteEndTime - deleteStartTime) / 1000.0);
                deleteWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(deleteElapsedTimeMs)).append("\n");
                //System.out.println("Usuwanie czas: " + deleteElapsedTimeMs + " ms");

                System.out.println("usuniento");
            }

            importWriter.flush();
            importWriter.close();

            updateWriter.flush();
            updateWriter.close();

            deleteWriter.flush();
            deleteWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Zamknij połączenie z klastrzem Couchbase
        cluster.disconnect();
    }

    private static void importJsonData(Collection collection, File jsonFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonFile);
            if (rootNode.isArray()) {
                Iterator<JsonNode> iterator = rootNode.iterator();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    JsonObject jsonObject = JsonObject.fromJson(node.toString());
                    MutationResult result = collection.upsert(String.valueOf(jsonObject.getInt("ID")), jsonObject);
                    //System.out.println("Dane z ID " + jsonObject.getInt("ID") + " zaimportowane.");
                }
            } else {
                System.out.println("Plik JSON nie zawiera tablicy obiektów.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void updateJsonData(Collection collection, File jsonFile) {
        try {
            // Wczytaj plik JSON za pomocą Jackson ObjectMapper
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonFile);

            // Sprawdź, czy plik JSON jest tablicą
            if (rootNode.isArray()) {
                // Iteruj przez każdy obiekt w tablicy
                Iterator<JsonNode> iterator = rootNode.iterator();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    // Konwertuj obiekt JSON na JsonObject z Couchbase
                    JsonObject jsonObject = JsonObject.fromJson(node.toString());
                    // Pobierz ID obiektu
                    String id = String.valueOf(jsonObject.getInt("ID"));
                    // Sprawdź, czy obiekt istnieje w bazie danych
                    if (collection.exists(id).exists()) {
                        // Aktualizuj obiekt w bazie danych
                        MutationResult result = collection.replace(id, jsonObject);
                      //  System.out.println("Dane z ID " + id + " zaktualizowane.");
                    } else {
                        System.out.println("Nie można zaktualizować danych z ID " + id + ", ponieważ obiekt nie istnieje w bazie.");
                    }
                }
            } else {
                System.out.println("Plik JSON nie zawiera tablicy obiektów.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void deleteAllData(Collection collection, Cluster cluster) {
        // Zapytanie N1QL DELETE do usunięcia wszystkich dokumentów z kolekcji
        String query = "DELETE FROM `" + collection.bucketName() + "`";

        try {
            // Wykonaj zapytanie N1QL DELETE
            QueryResult queryResult = cluster.query(query);

            // Sprawdź, czy zapytanie zostało wykonane poprawnie
            if (queryResult.metaData().status() == QueryStatus.SUCCESS) {
                //System.out.println("Wszystkie dane zostały usunięte.");
            } else {
                //System.out.println("Wystąpił błąd podczas usuwania danych.");
            }
        } catch (CouchbaseException ex) {
            ex.printStackTrace();
        }
    }
 }