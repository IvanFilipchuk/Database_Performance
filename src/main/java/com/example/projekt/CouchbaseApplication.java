package com.example.projekt;

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
@RestController
public class CouchbaseApplication {
    //    public static void main(String[] args) {
//        // Konfiguracja połączenia z bazą danych Couchbase
//        Cluster cluster = Cluster.connect("localhost", "root", "root");
//        Collection collection = cluster.bucket("default").defaultCollection();
//
//        // Ścieżka do pliku JSON z danymi
//        String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
//
//        try {
//            // Odczytaj zawartość pliku JSON
//            ObjectMapper objectMapper = new ObjectMapper();
//            JsonNode rootNode = objectMapper.readTree(new File(jsonFilePath));
//
//            // Iteruj przez każdy element w pliku JSON
//            for (JsonNode node : rootNode) {
//                // Konwertuj JsonNode na JsonObject
//                JsonObject jsonObject = JsonObject.fromJson(node.toString());
//
//                // Wstaw dane do bazy danych
//                collection.upsert("person_" + jsonObject.get("id"), jsonObject);
//
//                // Wyświetl informację o poprawnym zaimportowaniu danych
//                System.out.println("Dane dla ID " + jsonObject.get("id") + " zostały zaimportowane pomyślnie.");
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            // Zamknij połączenie z bazą danych
//            cluster.disconnect();
//        }
//    }
//    public static void main(String[] args) {
//        // Adres i dane dostępowe do bazy danych Couchbase
//        String connectionString = "couchbase://127.0.0.1";
//        String username = "test";
//        String password = "password";
//
//        // Nazwa bucketa i kolekcji
//        String bucketName = "default";
//        String collectionName = "_default";
//
//        // Dane do wstawienia
//        String documentId = "1";
//        JsonObject data = JsonObject.create()
//                .put("name", "John")
//                .put("age", 30);
//
//        // Połączenie z bazą danych Couchbase
//        try {
//            Cluster cluster = Cluster.connect(connectionString, username, password);
//            Collection collection = cluster.bucket(bucketName).defaultCollection();
//
//            boolean success = false;
//            int retryCount = 0;
//            final int maxRetries = 10;
//            final int retryIntervalMillis = 1000; // 1 sekunda
//
//            // Pętla prób wstawienia dokumentu
//            while (!success && retryCount < maxRetries) {
//                try {
//                    // Wstawianie danych do kolekcji
//                    MutationResult result = collection.insert(documentId, data, InsertOptions.insertOptions());
//                    success = true; // Ustawienie flagi sukcesu
//                    System.out.println("Dane zostały pomyślnie dodane do kolekcji.");
//                } catch (AmbiguousTimeoutException e) {
//                    // Obsługa błędu TIMEOUT
//                    System.out.println("Timeout: Próba ponownego wstawienia dokumentu...");
//                    Thread.sleep(retryIntervalMillis); // Odczekanie przed kolejną próbą
//                    retryCount++; // Zwiększenie licznika prób
//                }
//            }
//
//            // Sprawdzenie, czy udało się wstawić dokument
//            if (!success) {
//                System.out.println("Nie udało się wstawić danych do kolekcji po " + maxRetries + " próbach.");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//

//    public static void main(String[] args) {
//        String connectionString = "couchbase://127.0.0.1";
//        String username = "test";
//        String password = "password";
//        String bucketName = "default";
//        String collectionName = "_default";
//        String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
//        int importCount = 1000;
//
//        try {
//            Cluster cluster = Cluster.connect(connectionString, username, password);
//            Collection collection = cluster.bucket(bucketName).defaultCollection();
//
//            FileWriter csvWriter = new FileWriter("couchbase_import_execution_time.csv");
//            csvWriter.append("Import Number,Execution Time (ms)\n");
//
//            for (int i = 0; i < importCount; i++) {
//                long totalExecutionTime = 0;
//
//                try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
//                    StringBuilder jsonContent = new StringBuilder();
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        jsonContent.append(line);
//                    }
//
//                    JsonArray jsonArray = JsonArray.fromJson(jsonContent.toString());
//
//                    for (Object obj : jsonArray) {
//                        JsonObject data = (JsonObject) obj;
//
//                        boolean success = false;
//                        int retryCount = 0;
//                        final int maxRetries = 10;
//                        final int retryIntervalMillis = 1000;
//
//                        while (!success && retryCount < maxRetries) {
//                            try {
//                                long startTime = System.nanoTime();
//                                MutationResult result = collection.insert(String.valueOf(documentCount + i), data, InsertOptions.insertOptions());
//                                long endTime = System.nanoTime();
//
//                                float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
//                                totalExecutionTime += elapsedTimeMs;
//                                success = true;
//                            } catch (AmbiguousTimeoutException e) {
//                                Thread.sleep(retryIntervalMillis);
//                                retryCount++;
//                            }
//                        }
//
//                        if (!success) {
//                            System.out.println("Nie udało się wstawić danych do kolekcji po " + maxRetries + " próbach.");
//                        }
//                    }
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//                float averageExecutionTime = totalExecutionTime / 1000f;
//                csvWriter.append(String.valueOf(i)).append(",").append(String.valueOf(averageExecutionTime)).append("\n");
//
//                // Usuń wszystkie dokumenty po zakończeniu importu
//                cluster.query("DELETE FROM `" + bucketName + "`");
//            }
//
//            csvWriter.flush();
//            csvWriter.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


//    public static void main(String[] args) {
//        String connectionString = "couchbase://127.0.0.1";
//        String username = "test";
//        String password = "password";
//        String bucketName = "default";
//        String collectionName = "_default";
//        String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
//
//        try {
//            Cluster cluster = Cluster.connect(connectionString, username, password);
//            Collection collection = cluster.bucket(bucketName).defaultCollection();
//
//            try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
//                StringBuilder jsonContent = new StringBuilder();
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    jsonContent.append(line);
//                }
//
//                JsonArray jsonArray = JsonArray.fromJson(jsonContent.toString());
//                FileWriter csvWriter = new FileWriter("couchbase_import_execution_time.csv");
//                csvWriter.append("1000/Couchbase/Import\n");
//
//                int documentCount = 0;
//                for (Object obj : jsonArray) {
//                    JsonObject data = (JsonObject) obj;
//
//                    boolean success = false;
//                    int retryCount = 0;
//                    final int maxRetries = 10;
//                    final int retryIntervalMillis = 1000;
//
//                    while (!success && retryCount < maxRetries) {
//                        try {
//                            long startTime = System.nanoTime();
//                            MutationResult result = collection.insert(String.valueOf(documentCount), data, InsertOptions.insertOptions());
//                            long endTime = System.nanoTime();
//
//                            float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime)/1000.0);
//                            csvWriter.append(String.valueOf(documentCount)).append(",").append(String.valueOf(elapsedTimeMs)).append("\n");
//
//                            success = true;
//                            System.out.println("Dane zostały pomyślnie dodane do kolekcji. Numer dokumentu: " + documentCount);
//                        } catch (AmbiguousTimeoutException e) {
//                            System.out.println("Timeout: Próba ponownego wstawienia dokumentu...");
//                            Thread.sleep(retryIntervalMillis);
//                            retryCount++;
//                        }
//                    }
//
//                    if (!success) {
//                        System.out.println("Nie udało się wstawić danych do kolekcji po " + maxRetries + " próbach. Numer dokumentu: " + documentCount);
//                    }
//
//                    documentCount++;
//                }
//
//                csvWriter.flush();
//                csvWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    //    public static void main(String[] args) {
//        String connectionString = "couchbase://127.0.0.1";
//        String username = "test";
//        String password = "password";
//        String bucketName = "default";
//        String collectionName = "_default";
//        String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
//        int importCount = 1000;
//
//        try {
//            FileWriter csvWriter = new FileWriter("couchbase_import_execution_time.csv");
//            csvWriter.append("1000/Couchbase/Import\n");
//
//            for (int i = 0; i < importCount; i++) {
//                Cluster cluster = Cluster.connect(connectionString, username, password);
//                Collection collection = cluster.bucket(bucketName).defaultCollection();
//
//                try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
//                    StringBuilder jsonContent = new StringBuilder();
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        jsonContent.append(line);
//                    }
//
//                    JsonArray jsonArray = JsonArray.fromJson(jsonContent.toString());
//
//                    long totalExecutionTime = 0;
//                    int documentCount = 0;
//                    for (Object obj : jsonArray) {
//                        JsonObject data = (JsonObject) obj;
//
//                        boolean success = false;
//                        int retryCount = 0;
//                        final int maxRetries = 10;
//                        final int retryIntervalMillis = 1000;
//
//                        while (!success && retryCount < maxRetries) {
//                            try {
//                                long startTime = System.nanoTime();
//                                MutationResult result = collection.insert(String.valueOf(documentCount), data, InsertOptions.insertOptions());
//                                long endTime = System.nanoTime();
//
//                                float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
//                                totalExecutionTime += elapsedTimeMs;
//                                success = true;
//                            } catch (AmbiguousTimeoutException e) {
//                                Thread.sleep(retryIntervalMillis);
//                                retryCount++;
//                            }
//                        }
//
//                        if (!success) {
//                            System.out.println("Nie udało się wstawić danych do kolekcji po " + maxRetries + " próbach.");
//                        }
//
//                        documentCount++;
//                    }
//
//                    float averageExecutionTime = totalExecutionTime / jsonArray.size();
//                    csvWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(averageExecutionTime)).append("\n");
//
//                    // Usuń wszystkie dokumenty po zakończeniu importu
//                    cluster.query("DELETE FROM `" + bucketName + "`");
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            csvWriter.flush();
//            csvWriter.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//public static void main(String[] args) {
//    String connectionString = "couchbase://127.0.0.1";
//    String username = "test";
//    String password = "password";
//    String bucketName = "default";
//    String collectionName = "_default";
//    String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
//    int importCount = 1000;
//
//    try {
//        FileWriter csvWriter = new FileWriter("couchbase_import_execution_time.csv");
//        csvWriter.append("1000/Couchbase/Import\n");
//
//        for (int i = 0; i < importCount; i++) {
//            System.out.println("Starting import " + (i + 1) + "...");
//            Cluster cluster = Cluster.connect(connectionString, username, password);
//            Collection collection = cluster.bucket(bucketName).defaultCollection();
//
//            try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
//                StringBuilder jsonContent = new StringBuilder();
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    jsonContent.append(line);
//                }
//
//                JsonArray jsonArray = JsonArray.fromJson(jsonContent.toString());
//
//                long totalExecutionTime = 0;
//                int documentCount = 0;
//                for (Object obj : jsonArray) {
//                    JsonObject data = (JsonObject) obj;
//
//                    boolean success = false;
//                    int retryCount = 0;
//                    final int maxRetries = 10;
//                    final int retryIntervalMillis = 1000;
//
//                    while (!success && retryCount < maxRetries) {
//                        try {
//                            long startTime = System.nanoTime();
//                            MutationResult result = collection.insert(String.valueOf(documentCount), data, InsertOptions.insertOptions());
//                            long endTime = System.nanoTime();
//
//                            float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
//                            totalExecutionTime += elapsedTimeMs;
//                            success = true;
//                        } catch (AmbiguousTimeoutException e) {
//                            Thread.sleep(retryIntervalMillis);
//                            retryCount++;
//                        } catch (DocumentExistsException e) {
//                            // Document with the given ID already exists, skip insertion
//                            System.out.println("Document with ID " + documentCount + " already exists, skipping insertion.");
//                            success = true; // Mark as success to proceed to the next document
//                        }
//                    }
//
//                    if (!success) {
//                        System.out.println("Failed to insert data into the collection after " + maxRetries + " attempts.");
//                    }
//
//                    documentCount++;
//                }
//
//                float averageExecutionTime = totalExecutionTime / jsonArray.size();
//                csvWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(averageExecutionTime)).append("\n");
//                System.out.println("Import " + (i + 1) + " completed.");
//
//                // Delete all documents after import
//                cluster.query("DELETE FROM `" + bucketName + "`");
//                System.out.println("Deleted all documents after import.");
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                cluster.disconnect();
//            }
//        }
//
//        csvWriter.flush();
//        csvWriter.close();
//
//    } catch (Exception e) {
//        e.printStackTrace();
//    }
//    }
    public static void main(String[] args) {
        String connectionString = "couchbase://127.0.0.1";
        String username = "test";
        String password = "password";
        String bucketName = "default";
        String collectionName = "_default";
        String jsonFilePath = "INSERT_DATA_COUCHBASE_1000.json";
        int importCount = 1000;

        try {
            FileWriter csvWriter = new FileWriter("couchbase1_import_execution_time.csv");
            csvWriter.append("1000/Couchbase/Import\n");

            for (int i = 0; i < importCount; i++) {
                long startTime = System.nanoTime();
                System.out.println("Starting import " + (i + 1) + "...");
                Cluster cluster = Cluster.connect(connectionString, username, password);
                Collection collection = cluster.bucket(bucketName).defaultCollection();

                try (BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
                    StringBuilder jsonContent = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        jsonContent.append(line);
                    }

                    JsonArray jsonArray = JsonArray.fromJson(jsonContent.toString());

                    long totalExecutionTime = 0;
                    int documentCount = 0;
                    for (Object obj : jsonArray) {
                        JsonObject data = (JsonObject) obj;

                        boolean success = false;
                        int retryCount = 0;
                        final int maxRetries = 10;
                        final int retryIntervalMillis = 1000;

                        while (!success && retryCount < maxRetries) {
                            try {
                                long insertStartTime = System.nanoTime();
                                MutationResult result = collection.insert(String.valueOf(documentCount), data, InsertOptions.insertOptions());
                                long insertEndTime = System.nanoTime();

                                float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(insertEndTime - insertStartTime) / 1000.0);
                                totalExecutionTime += elapsedTimeMs;
                                success = true;
                            } catch (AmbiguousTimeoutException e) {
                                Thread.sleep(retryIntervalMillis);
                                retryCount++;
                            } catch (DocumentExistsException e) {
                                // Document with the given ID already exists, skip insertion
                                System.out.println("Document with ID " + documentCount + " already exists, skipping insertion.");
                                success = true; // Mark as success to proceed to the next document
                            }
                        }

                        if (!success) {
                            System.out.println("Failed to insert data into the collection after " + maxRetries + " attempts.");
                        }

                        documentCount++;
                    }

                    float averageExecutionTime = totalExecutionTime / jsonArray.size();
                    //csvWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(averageExecutionTime)).append("\n");
                    System.out.println("Import " + (i + 1) + " completed.");

                    // Delete all documents after import
                    cluster.query("DELETE FROM `" + bucketName + "`");
                    System.out.println("Deleted all documents after import.");

                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    cluster.disconnect();
                }

                long endTime = System.nanoTime();
                float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
                csvWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(elapsedTimeMs)).append("\n");
            }

            csvWriter.flush();
            csvWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
