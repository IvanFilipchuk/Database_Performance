package com.example.projekt;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class PostgreSQLApplication {

    public static void main(String[] args) throws IOException {
        ImportsDeletes(100);
        Selects(100);
        Updates(100);
        ImportsDeletes(1000);
        Selects(1000);
        Updates(1000);
        ImportsDeletes(10000);
        Selects(10000);
        Updates(10000);

    }
    public static void ImportsDeletes(int numberOfRecords) throws IOException {
        try {
            FileWriter writer = new FileWriter("charts/create/postgresql_import_time_" + numberOfRecords + ".csv");
            FileWriter writerDelete = new FileWriter("charts/delete/postgresql_delete_time_" + numberOfRecords + ".csv");
            executeSqlJdbc("PostgreSQL_data/db_structure.sql",-1);
            for (int i = 0; i < 1000; i++) {
                long jdbcStartTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/imports.sql",numberOfRecords);
                long jdbcEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(jdbcEndTime - jdbcStartTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");
                long startTimeDelete = System.currentTimeMillis();
                executeSqlJdbc("PostgreSQL_data/delete_data.sql",-1);
                long endTimeDelete = System.currentTimeMillis();
                float totalTimeDelete = (float) ((endTimeDelete - startTimeDelete) / 1000.0);
                writerDelete.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTimeDelete)).append("\n");
            }
            writer.flush();
            writer.close();
            writerDelete.flush();
            writerDelete.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    public static void Selects(int numberOfRecords) throws IOException {
        try {
            FileWriter writerSelect1 = new FileWriter("charts/read/postgresql_select1_time_" + numberOfRecords + ".csv");
            FileWriter writerSelect2 = new FileWriter("charts/read/postgresql_select2_time_" + numberOfRecords + ".csv");
            FileWriter writerSelect3 = new FileWriter("charts/read/postgresql_select3_time_" + numberOfRecords + ".csv");
            executeSqlJdbc("PostgreSQL_data/db_structure.sql",-1);
            executeSqlJdbc("PostgreSQL_data/imports.sql",numberOfRecords);
            for (int i = 0; i < 1000; i++) {
                long selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select1.sql",-1);
                long selectEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select2.sql",-1);
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/select3.sql",-1);
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerSelect3.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

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
    public static void Updates(int numberOfRecords) throws IOException {
        try {
            FileWriter writerUpdate1 = new FileWriter("charts/update/postgresql_update1_time_" + numberOfRecords + ".csv");
            FileWriter writerUpdate2 = new FileWriter("charts/update/postgresql_update2_time_" + numberOfRecords + ".csv");

            executeSqlJdbc("PostgreSQL_data/db_structure.sql",-1);
            executeSqlJdbc("PostgreSQL_data/imports.sql",numberOfRecords);
            for (int i = 0; i < 1000; i++) {
                long selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/update1.sql",-1);
                long selectEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerUpdate1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc("PostgreSQL_data/update2.sql",-1);
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime)/1000.0);
                writerUpdate2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");



            }
            writerUpdate1.flush();
            writerUpdate1.close();
            writerUpdate2.flush();
            writerUpdate2.close();
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
    private static void executeSqlJdbc(String sqlFile, int numLines) {
        String url = "jdbc:postgresql://localhost:5432/air_tickets";
        String user = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to PostgreSQL database!");
            StringBuilder sql = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
                String line;
                int linesRead = 0;
                while ((line = reader.readLine()) != null && (numLines <= 0 || linesRead < numLines)) {
                    sql.append(line).append("\n");
                    linesRead++;
                }
            }

            connection.setAutoCommit(false); // Start transaction
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql.toString());
                connection.commit(); // Commit transaction
                System.out.println("Import script executed successfully!");
            } catch (SQLException e) {
                connection.rollback(); // Rollback transaction if an error occurs
                System.out.println("Import script execution failed!");
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("Error connecting to PostgreSQL database:");
            e.printStackTrace();
        }
    }


}
