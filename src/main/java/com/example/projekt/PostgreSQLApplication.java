package com.example.projekt;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class PostgreSQLApplication {

    public static void main(String[] args) throws IOException {
        try {
            FileWriter writer = new FileWriter("charts/create/postgresql_import_time_1000.csv");
            FileWriter writerDelete1000 = new FileWriter("charts/delete/postgresql_delete_time_1000.csv");

            writer.append("1000/PostgreSQL/Import\n");
            executeSqlJdbc("PostgreSQL_data/db_structure.sql");
            for (int i = 0; i < 1000; i++) {
                long jdbcStartTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/imports_1000.sql");
                long jdbcEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(jdbcEndTime - jdbcStartTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");
                long startTimeDelete = System.currentTimeMillis();
                executeSqlJdbc("PostgreSQL_data/delete_data.sql");
                long endTimeDelete = System.currentTimeMillis();
                float totalTimeDelete = (float) ((endTimeDelete - startTimeDelete) / 1000.0);
                writerDelete1000.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTimeDelete)).append("\n");

            }
            writer.flush();
            writer.close();
            writerDelete1000.flush();
            writerDelete1000.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileWriter writer = new FileWriter("charts/create/postgresql_import_time_10000.csv");
            FileWriter writerDelete10000 = new FileWriter("charts/delete/postgresql_delete_time_10000.csv");
            writer.append("10000/PostgreSQL/Import\n");
            for (int i = 0; i < 1000; i++) {
                long jdbcStartTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/imports_10000.sql");
                long jdbcEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(jdbcEndTime - jdbcStartTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");
                long startTimeDelete = System.currentTimeMillis();
                executeSqlJdbc("PostgreSQL_data/delete_data.sql");
                long endTimeDelete = System.currentTimeMillis();
                float totalTimeDelete = (float) ((endTimeDelete - startTimeDelete) / 1000.0);
                writerDelete10000.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTimeDelete)).append("\n");

            }
            writer.flush();
            writer.close();
            writerDelete10000.flush();
            writerDelete10000.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Selects();

    }
    public static void Selects() throws IOException {
        try {
            FileWriter writerSelect1 = new FileWriter("charts/read/postgresql_select1_time_1000.csv");
            FileWriter writerSelect2 = new FileWriter("charts/read/postgresql_select2_time_1000.csv");
            FileWriter writerSelect3 = new FileWriter("charts/read/postgresql_select3_time_1000.csv");
            executeSqlJdbc("PostgreSQL_data/db_structure.sql");
            executeSqlJdbc("PostgreSQL_data/imports_1000.sql");
            for (int i = 0; i < 1000; i++) {
                long selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select1.sql");
                long selectEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select2.sql");
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select3.sql");
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

            }
            writerSelect1.flush();
            writerSelect1.close();
            writerSelect2.flush();
            writerSelect2.close();
            writerSelect3.flush();
            writerSelect3.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void writeJdbcAverageTimeToCsv(String fileName, double averageElapsedTime) throws IOException {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.append("Average JDBC Execution Time (milliseconds)\n");
            writer.append(String.valueOf(averageElapsedTime));
        }
    }
    private static void executeSqlJdbc(String sqlFile) {
        String url = "jdbc:postgresql://localhost:5432/air_tickets";
        String user = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to PostgreSQL database!");
            StringBuilder sql = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sql.append(line).append("\n");
                }
            }

            try (Statement statement = connection.createStatement()) {
                boolean result = statement.execute(sql.toString());
                if (result) {
                    System.out.println("Import script executed successfully!");
                } else {
                    System.out.println("Import script execution failed!");
                }
            }
        } catch (Exception e) {
            System.err.println("Error connecting to PostgreSQL database:");
            e.printStackTrace();
        }
    }
}
