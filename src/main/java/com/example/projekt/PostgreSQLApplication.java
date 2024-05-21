package com.example.projekt;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.sql.*;
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
        String url = "jdbc:postgresql://localhost:5432/air_tickets";
        String user = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to PostgreSQL database!");

            FileWriter writer = new FileWriter("charts/create/postgresql_import_time_" + numberOfRecords + ".csv");
            FileWriter writerDelete = new FileWriter("charts/delete/postgresql_delete_time_" + numberOfRecords + ".csv");

            for (int i = 0; i < 10; i++) {
                System.out.println("Test");
                System.out.println("Test");
                float totalImportTime = 0;
                totalImportTime += executeSqlJdbc(connection, "PostgreSQL_data/imports.sql",numberOfRecords);
                totalImportTime += executeSqlJdbc(connection, "PostgreSQL_data/imports2.sql",numberOfRecords);
                totalImportTime += executeSqlJdbc(connection, "PostgreSQL_data/pilots.sql",numberOfRecords);
                totalImportTime += executeSqlJdbc(connection, "PostgreSQL_data/passengers.sql",numberOfRecords);
                System.out.println(totalImportTime);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalImportTime/4.0)).append("\n");
                float totalTimeDelete = executeSqlJdbc(connection, "PostgreSQL_data/delete_data.sql",-1);
                writerDelete.append(String.valueOf(i + 1)).append(",").append(String.valueOf(totalTimeDelete/4.0)).append("\n");
            }

            writer.flush();
            writer.close();
            writerDelete.flush();
            writerDelete.close();

        } catch (SQLException e) {
            System.err.println("Error connecting to PostgreSQL database:");
            e.printStackTrace();
        }
    }


    public static void Selects(int numberOfRecords) throws IOException {
        String url = "jdbc:postgresql://localhost:5432/air_tickets";
        String user = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            FileWriter writerSelect1 = new FileWriter("charts/read/postgresql_select1_time_" + numberOfRecords + ".csv");
            FileWriter writerSelect2 = new FileWriter("charts/read/postgresql_select2_time_" + numberOfRecords + ".csv");
            FileWriter writerSelect3 = new FileWriter("charts/read/postgresql_select3_time_" + numberOfRecords + ".csv");

            //executeSqlJdbc(connection, "PostgreSQL_data/db_structure.sql", -1);
            executeSqlJdbc(connection, "PostgreSQL_data/imports.sql", numberOfRecords);

            for (int i = 0; i < 1000; i++) {
                float jdbcElapsedTime;

                long selectTime = System.nanoTime();
                executeSqlJdbc(connection, "PostgreSQL_data/select1.sql", -1);
                long selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime) / 1000.0);
                writerSelect1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc(connection, "PostgreSQL_data/select2.sql", -1);
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime) / 1000.0);
                writerSelect2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                selectTime = System.nanoTime();
                executeSqlJdbc(connection, "PostgreSQL_data/select3.sql", -1);
                selectEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(selectEndTime - selectTime) / 1000.0);
                writerSelect3.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

            }
            executeSqlJdbc(connection, "PostgreSQL_data/delete_data.sql",-1);
            writerSelect1.flush();
            writerSelect1.close();
            writerSelect2.flush();
            writerSelect2.close();
            writerSelect3.flush();
            writerSelect3.close();

        } catch (SQLException e) {
            System.err.println("Error connecting to PostgreSQL database:");
            e.printStackTrace();
        }
    }

    public static void Updates(int numberOfRecords) throws IOException {
        String url = "jdbc:postgresql://localhost:5432/air_tickets";
        String user = "root";
        String password = "root";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            FileWriter writerUpdate1 = new FileWriter("charts/update/postgresql_update1_time_" + numberOfRecords + ".csv");
            FileWriter writerUpdate2 = new FileWriter("charts/update/postgresql_update2_time_" + numberOfRecords + ".csv");

            //executeSqlJdbc(connection, "PostgreSQL_data/db_structure.sql", -1);
            executeSqlJdbc(connection, "PostgreSQL_data/imports.sql", numberOfRecords);

            for (int i = 0; i < 10; i++) {
                float jdbcElapsedTime;

                long updateTime = System.nanoTime();
                executeSqlJdbc(connection, "PostgreSQL_data/update1.sql", -1);
                long updateEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateTime) / 1000.0);
                writerUpdate1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                updateTime = System.nanoTime();
                executeSqlJdbc(connection, "PostgreSQL_data/update2.sql", -1);
                updateEndTime = System.nanoTime();
                jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateTime) / 1000.0);
                writerUpdate2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

            }
            executeSqlJdbc(connection, "PostgreSQL_data/delete_data.sql",-1);
            writerUpdate1.flush();
            writerUpdate1.close();
            writerUpdate2.flush();
            writerUpdate2.close();

        } catch (SQLException e) {
            System.err.println("Error connecting to PostgreSQL database:");
            e.printStackTrace();
        }
    }




    private static float executeSqlJdbc(Connection connection, String sqlFile, int numberOfLines) {
        long startTime = 0;
        try {
            StringBuilder sql = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
                String line;
                int linesRead = 0;
                while ((line = reader.readLine()) != null && (numberOfLines < 0 || linesRead < numberOfLines)) {
                    sql.append(line).append("\n");
                    linesRead++;
                }
            }
            startTime = System.nanoTime();
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql.toString());
                connection.commit();
                System.out.println("SQL script executed successfully!");
            } catch (SQLException e) {
                connection.rollback();
                System.out.println("SQL script execution failed!");
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        return (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime) / 1000.0);
    }







}
