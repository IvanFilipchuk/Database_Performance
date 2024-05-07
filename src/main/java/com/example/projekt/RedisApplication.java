package com.example.projekt;

import redis.clients.jedis.Jedis;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Stream;

public class
RedisApplication {

    public static void main(String[] args) throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);
        ImportDeletes(jedis,100);
        Selects(jedis,100);
        Updates(jedis,100);
        ImportDeletes(jedis,1000);
        Selects(jedis,1000);
        Updates(jedis,1000);
        ImportDeletes(jedis,10000);
        Selects(jedis,10000);
        Updates(jedis,10000);
        jedis.close();

    }
    public static void ImportDeletes(Jedis jedis, int numberOfRecords) throws IOException {
        String csvFilePath = "Redis_data/imports.csv";
        FileWriter writer = new FileWriter("charts/create/redis_import_time_" + numberOfRecords + ".csv");
        FileWriter writerDelete = new FileWriter("charts/delete/redis_delete_time_" + numberOfRecords + ".csv");


        try {
            for (int i = 0; i < 1000; i++) {
                Reader reader = new FileReader(csvFilePath);
                CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
                Map<String, String> batchOps = new HashMap<>();
                for (CSVRecord record : csvParser) {
                    String key = record.get("id_flight");
                    String value = record.get("message");
                    batchOps.put(key, value);
                }
                long startTime = System.currentTimeMillis();
                jedis.hmset("flights",batchOps);
                long endTime = System.currentTimeMillis();
                float totalTime = (float) ((endTime - startTime) / 1000.0);
                System.out.println(totalTime);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");
                csvParser.close();
                reader.close();
                long startTimeDelete = System.currentTimeMillis();
                jedis.flushAll();
                long endTimeDelete = System.currentTimeMillis();
                float totalTimeDelete = (float) ((endTimeDelete - startTimeDelete) / 1000.0);
                writerDelete.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTimeDelete)).append("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.close();
            writerDelete.close();
        }
    }
    public static void Updates(Jedis jedis, int numberOfRecords) throws IOException{
        FileWriter writer = new FileWriter("charts/update/redis_update1_time_" + numberOfRecords + ".csv");
        for (int i=0; i<1000;i++)
        {
            String csvFilePath = "Redis_data/imports.csv";
            Reader reader = new FileReader(csvFilePath);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
            Map<String, String> batchOps = new HashMap<>();
            int linesRead = 0;
            for (CSVRecord record : csvParser) {
                String key = record.get("id_flight");
                String value = record.get("message");
                batchOps.put(key, value);

                linesRead++;
                if (numberOfRecords > 0 && linesRead >= numberOfRecords) {
                    break;
                }
            }

            jedis.hmset("flights",batchOps);
            csvParser.close();
            reader.close();
            long startTime = System.currentTimeMillis();
            if (jedis.hexists("flights", "JRADReeohw819465977006867827")) {
                jedis.hset("flights", "JRADReeohw819465977006867827", "New message for JRADReeohw819465977006867827");
            } else {
                System.out.println("Flight ID does not exist");
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");
            jedis.flushAll();
        }
        writer.close();

        FileWriter writer2 = new FileWriter("charts/update/redis_update2_time_" + numberOfRecords + ".csv");
        for (int i=0; i<1000;i++)
        {
            String csvFilePath = "Redis_data/imports.csv";
            Reader reader = new FileReader(csvFilePath);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
            Map<String, String> batchOps = new HashMap<>();
            int linesRead = 0;
            for (CSVRecord record : csvParser) {
                String key = record.get("id_flight");
                String value = record.get("message");
                batchOps.put(key, value);

                linesRead++;
                if (numberOfRecords > 0 && linesRead >= numberOfRecords) {
                    break;
                }
            }

            jedis.hmset("flights",batchOps);
            csvParser.close();
            reader.close();
            long startTime = System.currentTimeMillis();
            Map<String, String> updates = new HashMap<>();
            updates.put("SACSOatqmg465822704297665190", "New message for SACSOatqmg465822704297665190");
            updates.put("IBAENvymmo607421189038116673", "New message for IBAENvymmo607421189038116673");
            updates.put("TIHETydsns787561467477982045", "New message for TIHETydsns787561467477982045");
            for (Map.Entry<String, String> entry : updates.entrySet()) {
                jedis.hset("flights", entry.getKey(), entry.getValue());
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");
            jedis.flushAll();
        }
        writer2.close();
    }
    public static void Selects(Jedis jedis, int numberOfRecords) throws IOException {
        String csvFilePath = "Redis_data/imports.csv";
        Reader reader = new FileReader(csvFilePath);
        CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
        Map<String, String> batchOps = new HashMap<>();
        int linesRead = 0;
        for (CSVRecord record : csvParser) {
            String key = record.get("id_flight");
            String value = record.get("message");
            batchOps.put(key, value);

            linesRead++;
            if (numberOfRecords > 0 && linesRead >= numberOfRecords) {
                break;
            }
        }

        jedis.hmset("flights",batchOps);
        csvParser.close();
        reader.close();


        FileWriter writer = new FileWriter("charts/read/redis_select1_time_" + numberOfRecords + ".csv");
        for (int i=0; i<1000;i++)
        {
            long startTime = System.currentTimeMillis();
            Map<String, String> flights = jedis.hgetAll("flights");
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer.close();
        FileWriter writer2 = new FileWriter("charts/read/redis_select2_time_" + numberOfRecords + ".csv");
        for (int i=0; i<1000;i++)
        {
            long startTime = System.currentTimeMillis();
            Set<String> flightIds = jedis.keys("M*");
            for (String flightId : flightIds) {
                String message = jedis.hget("flights", flightId);
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer2.close();

        FileWriter writer3 = new FileWriter("charts/read/redis_select3_time_" + numberOfRecords + ".csv");
        for (int i=0; i<1000;i++)
        {
            String[] substrings = {"AAQ0K63", "LUOXXA9"};
            long startTime = System.currentTimeMillis();
            TreeMap<String, String> selectedFlights = new TreeMap<>();
            ScanParams scanParams = new ScanParams().match("*"); // Scan all keys
            String cursor = "0";
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan("flights", cursor, scanParams);
                for (Map.Entry<String, String> entry : scanResult.getResult()) {
                    String flightId = entry.getKey();
                    String message = entry.getValue();
                    for (String substring : substrings) {
                        if (message.contains(substring)) {
                            selectedFlights.put(flightId, message);
                            break;
                        }
                    }
                }
                cursor = scanResult.getCursor();
            } while (!cursor.equals("0"));

            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer3.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer3.close();
    }
}
