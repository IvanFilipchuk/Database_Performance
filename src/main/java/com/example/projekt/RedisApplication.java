package com.example.projekt;

import redis.clients.jedis.Jedis;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import redis.clients.jedis.Tuple;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Stream;

public class RedisApplication {

    public static void main(String[] args) throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);
        String csvFilePath = "Redis_data/redis_imports_1000.csv";
        FileWriter writer = new FileWriter("charts/create/redis_import_time_1000.csv");
        FileWriter writerDelete = new FileWriter("charts/delete/redis_delete_time_1000.csv");
        writer.append("10000/Redis/Import\n");

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
                System.out.println("done "+i);
            }
            Selects(jedis);
            Updates(jedis);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
            writer.close();
            writerDelete.close();
        }

    }
    public static void Updates(Jedis jedis) throws IOException{
        FileWriter writer = new FileWriter("charts/update/redis_update1_time_1000.csv");
        for (int i=0; i<1000;i++)
        {
            String csvFilePath = "Redis_data/redis_imports_1000.csv";
            Reader reader = new FileReader(csvFilePath);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
            Map<String, String> batchOps = new HashMap<>();
            for (CSVRecord record : csvParser) {
                String key = record.get("id_flight");
                String value = record.get("message");
                batchOps.put(key, value);
            }

            jedis.hmset("flights",batchOps);
            csvParser.close();
            reader.close();
            long startTime = System.currentTimeMillis();
            if (jedis.hexists("flights", "SHym6572300931")) {
                jedis.hset("flights", "SHym6572300931", "New message for SHym6572300931");
            } else {
                System.out.println("Flight ID does not exist");
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");
            jedis.flushAll();
        }
        writer.close();

        FileWriter writer2 = new FileWriter("charts/update/redis_update2_time_1000.csv");
        for (int i=0; i<1000;i++)
        {
            String csvFilePath = "Redis_data/redis_imports_1000.csv";
            Reader reader = new FileReader(csvFilePath);
            CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
            Map<String, String> batchOps = new HashMap<>();
            for (CSVRecord record : csvParser) {
                String key = record.get("id_flight");
                String value = record.get("message");
                batchOps.put(key, value);
            }

            jedis.hmset("flights",batchOps);
            csvParser.close();
            reader.close();
            long startTime = System.currentTimeMillis();
            Map<String, String> updates = new HashMap<>();
            updates.put("MBib4354665090", "New message for MBib4354665090");
            updates.put("SHym6572300931", "New message for SHym6572300931");
            updates.put("XRiv7738125438", "New message for XRiv7738125438");
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
    public static void Selects(Jedis jedis) throws IOException {
        String csvFilePath = "Redis_data/redis_imports_1000.csv";
        Reader reader = new FileReader(csvFilePath);
        CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
        List<String[]> batchOps = new ArrayList<>();
        for (CSVRecord record : csvParser) {
            String key = record.get("id_flight");
            String value = record.get("message");
            batchOps.add(new String[]{key, value});
        }
        String[] keyValuePairs = batchOps.stream()
                .flatMap(arr -> Stream.of(arr[0], arr[1]))
                .toArray(String[]::new);
        jedis.mset(keyValuePairs);
        csvParser.close();
        reader.close();


        FileWriter writer = new FileWriter("charts/read/redis_select1_time_1000.csv");
        for (int i=0; i<1000;i++)
        {
            long startTime = System.currentTimeMillis();
            Map<String, String> flights = jedis.hgetAll("flights");
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer.close();
        FileWriter writer2 = new FileWriter("charts/read/redis_select2_time_1000.csv");
        for (int i=0; i<1000;i++)
        {
            long startTime = System.currentTimeMillis();
            Set<String> flightIds = jedis.keys("*");
            for (String flightId : flightIds) {
                if (flightId.startsWith("M")) {
                    String message = jedis.hget("flights", flightId);

                }
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer2.close();

        FileWriter writer3 = new FileWriter("charts/read/redis_select3_time_1000.csv");
        for (int i=0; i<1000;i++)
        {
            long startTime = System.currentTimeMillis();
            Set<String> flightIds = jedis.keys("*");
            TreeMap<String, String> sortedFlights = new TreeMap<>();
            for (String flightId : flightIds) {
                String message = jedis.hget("flights", flightId);
                sortedFlights.put(flightId, message);
            }
            long endTime = System.currentTimeMillis();
            float totalTime = (float) ((endTime - startTime) / 1000.0);
            writer3.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");

        }
        writer3.close();

        jedis.close();
    }
}
