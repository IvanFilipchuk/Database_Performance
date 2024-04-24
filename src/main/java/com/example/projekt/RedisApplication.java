package com.example.projekt;

import redis.clients.jedis.Jedis;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;

public class
RedisApplication {

    public static void main(String[] args) throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);
        String csvFilePath = "redis_imports_1000.csv";
        FileWriter writer = new FileWriter("charts/redis_execution_time_1000.csv");
        writer.append("10000/Redis/Import\n");
        try {

            for (int i = 0; i < 1000; i++) {
                Reader reader = new FileReader(csvFilePath);
                CSVParser csvParser = CSVFormat.DEFAULT.withHeader().parse(reader);
                long startTime = System.currentTimeMillis();
                for (CSVRecord record : csvParser) {
                    String key = record.get("id_flight");
                    String value = record.get("message");

                    jedis.set(key, value);
                }
                long endTime = System.currentTimeMillis();
                float totalTime = (float) ((endTime - startTime)/1000.0);
                System.out.println(totalTime);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTime)).append("\n");
                csvParser.close();
                reader.close();
                jedis.flushAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }
}
