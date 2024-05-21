package com.example.projekt;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


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

        FileWriter importWriter = null;
        FileWriter updateWriter1 = null;
        FileWriter updateWriter2 = null;
        FileWriter deleteWriter = null;
        FileWriter[] selectWriters = new FileWriter[5];

        int liczbaDanych = 100;


        try {
            importWriter = new FileWriter("charts/create/couchbase_new_import_time_"+liczbaDanych+".csv");
            updateWriter1 = new FileWriter("charts/update/couchbase_new_update1_time_"+liczbaDanych +".csv");
            updateWriter2 = new FileWriter("charts/update/couchbase_new_update2_time_"+liczbaDanych +".csv");
            deleteWriter = new FileWriter("charts/delete/couchbase_new_delete_time_"+liczbaDanych +".csv");
            for (int i = 0; i < 3; i++) {
                selectWriters[i] = new FileWriter("charts/read/couchbase_new_select" + (i + 1) + "_time_"+liczbaDanych +".csv");
            }

            importWriter.append("Iteration,Time (s)\n");
            updateWriter1.append("Iteration,Time (s)\n");
            updateWriter2.append("Iteration,Time (s)\n");

//            for (int i = 0; i < 3; i++) {
//                selectWriters[i].append("Iteration,Time (ms)\n");
//            }
            deleteWriter.append("Iteration,Time (s)\n");

            for (int i = 0; i < 10; i++) {
                System.out.println("Iteration: " + (i + 1));

                File jsonFile = new File("Couchbase/insert_"+liczbaDanych +".json");
                long importStartTime = System.nanoTime();
                importJsonData(collection, jsonFile).join();
                long importEndTime = System.nanoTime();
                float importElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(importEndTime - importStartTime) / 1000.0);
                importWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(importElapsedTimeMs)).append("\n");
                System.out.println("zaimportowano");

                File jsonFileUpdate = new File("Couchbase/update1_"+liczbaDanych +".json");
                long updateStartTime = System.nanoTime();
                updateJsonData(collection, jsonFileUpdate).join();
                long updateEndTime = System.nanoTime();
                float updateElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateStartTime) / 1000.0);
                updateWriter1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(updateElapsedTimeMs)).append("\n");
                System.out.println("zaktualizowano");


                File jsonFileUpdate2 = new File("Couchbase/update2_"+liczbaDanych +".json");
                long updateStartTime2 = System.nanoTime();
                updatePassengerJsonData(collection, jsonFileUpdate2).join();
                long updateEndTime2 = System.nanoTime();
                float updateElapsedTimeMs2 = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime2 - updateStartTime2) / 1000.0);
                updateWriter2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(updateElapsedTimeMs2)).append("\n");
                System.out.println("zaktualizowano 2 ");

                executeAndLogSelectQuery(cluster, selectWriters[0], i + 1, "SELECT ID, Model FROM `default` WHERE Production_Year > 2000");
                System.out.println("SELECT 1");
                executeAndLogSelectQuery(cluster, selectWriters[1], i + 1, "SELECT ID, Model FROM `default` WHERE Manufacturer = \"Lockheed Martin\"");
                System.out.println("SELECT 2");
                executeAndLogSelectQuery(cluster, selectWriters[2], i + 1, "SELECT ID, Model, Max_Speed FROM `default` WHERE Type = \"Fighter\" AND Max_Speed > 1500");
                System.out.println("SELECT 3");
//                executeAndLogSelectQuery(cluster, selectWriters[3], i + 1, "SELECT ID, Model FROM `default` WHERE Max_Takeoff_Weight > 70000");
//                System.out.println("SELECT 4");
//                executeAndLogSelectQuery(cluster, selectWriters[4], i + 1, "SELECT ID, Model FROM `default` WHERE Max_Payload IS NULL");
//                System.out.println("SELECT 5");

                long deleteStartTime = System.nanoTime();
                deleteAllData(collection, cluster).join();
                long deleteEndTime = System.nanoTime();
                float deleteElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(deleteEndTime - deleteStartTime) / 1000.0);
                deleteWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(deleteElapsedTimeMs)).append("\n");
                System.out.println("usunięto");
                System.out.println();

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (importWriter != null) importWriter.close();
                if (updateWriter1 != null) updateWriter1.close();
                if (updateWriter2 != null) updateWriter2.close();
                for (FileWriter writer : selectWriters) {
                    if (writer != null) writer.close();
                }
                if (deleteWriter != null) deleteWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static CompletableFuture<Void> importJsonData(Collection collection, File jsonFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonFile);
            if (rootNode.isArray()) {
                AsyncCollection asyncCollection = collection.async();
                List<CompletableFuture<MutationResult>> futures = new ArrayList<>();
                Iterator<JsonNode> iterator = rootNode.iterator();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    JsonObject jsonObject = JsonObject.fromJson(node.toString());
                    String id = String.valueOf(jsonObject.getInt("ID"));
                    CompletableFuture<MutationResult> future = ((AsyncCollection) asyncCollection).upsert(id, jsonObject);
                    futures.add(future);
                }
                CompletableFuture<Void>[] voidFutures = futures.toArray(new CompletableFuture[0]);
                return CompletableFuture.allOf(voidFutures);
            } else {
                System.out.println("Plik JSON nie zawiera tablicy obiektów.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(null);
    }

    private static CompletableFuture<Void> updateJsonData(Collection collection, File jsonFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonFile);
            if (rootNode.isArray()) {
                AsyncCollection asyncCollection = collection.async();
                List<CompletableFuture<MutationResult>> futures = new ArrayList<>();
                Iterator<JsonNode> iterator = rootNode.iterator();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    JsonObject jsonObject = JsonObject.fromJson(node.toString());
                    String id = String.valueOf(jsonObject.getInt("ID"));
                    CompletableFuture<MutationResult> future = asyncCollection.replace(id, jsonObject);
                    futures.add(future);
                }
                CompletableFuture<Void>[] voidFutures = futures.toArray(new CompletableFuture[0]);
                return CompletableFuture.allOf(voidFutures);
            } else {
                System.out.println("Plik JSON nie zawiera tablicy obiektów.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(null);
    }


    private static CompletableFuture<Void> updatePassengerJsonData(Collection collection, File jsonFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonFile);
            if (rootNode.isArray()) {
                AsyncCollection asyncCollection = collection.async();
                List<CompletableFuture<MutationResult>> futures = new ArrayList<>();
                Iterator<JsonNode> iterator = rootNode.iterator();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    JsonObject jsonObject = JsonObject.fromJson(node.toString());
                    if (jsonObject.getString("Type").equals("Passenger")) {
                        String id = String.valueOf(jsonObject.getInt("ID"));
                        CompletableFuture<MutationResult> future = asyncCollection.replace(id, jsonObject);
                        futures.add(future);
                    }
                }
                CompletableFuture<Void>[] voidFutures = futures.toArray(new CompletableFuture[0]);
                return CompletableFuture.allOf(voidFutures);
            } else {
                System.out.println("Plik JSON nie zawiera tablicy obiektów.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(null);
    }

//    private static CompletableFuture<Void> deleteAllData(Collection collection, Cluster cluster) {
//        String query = "DELETE FROM `" + collection.bucketName() + "`";
//
//        return CompletableFuture.supplyAsync(() -> {
//            try {
//                QueryResult queryResult = cluster.async().query(query).join();
//                if (queryResult.metaData().status() == QueryStatus.SUCCESS) {
//                    return null;
//                } else {
//                    throw new CouchbaseException("Wystąpił błąd podczas usuwania danych.");
//                }
//            } catch (CouchbaseException ex) {
//                throw new RuntimeException(ex);
//            }
//        });
//    }

    private static CompletableFuture<Void> deleteAllData(Collection collection, Cluster cluster) {
        String query = "DELETE FROM `" + collection.bucketName() + "`";

        return CompletableFuture.runAsync(() -> {
            try {
                QueryResult queryResult = cluster.query(query, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
                if (queryResult.metaData().status() != QueryStatus.SUCCESS) {
                    throw new CouchbaseException("Wystąpił błąd podczas usuwania danych.");
                }
            } catch (CouchbaseException ex) {
                throw new RuntimeException(ex);
            }
        });
    }
//    private static void executeAndLogSelectQuery(Cluster cluster, FileWriter writer, int iteration, String query) throws IOException {
//        long startTime = System.nanoTime();
//        QueryResult result = cluster.async().query(query).join();
//        long endTime = System.nanoTime();
//        float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime)/1000.0);
//        writer.append(iteration + "," + elapsedTimeMs + "\n");
//    }
private static void executeAndLogSelectQuery(Cluster cluster, FileWriter writer, int iteration, String query) throws IOException {
    long startTime = System.nanoTime();
    QueryResult result = cluster.query(query, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
    long endTime = System.nanoTime();
    float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
    writer.append(iteration + "," + elapsedTimeMs + "\n");
}
}