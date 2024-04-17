package com.example.projekt;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
@SpringBootApplication
@RestController
public class MariaDBApplication {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/planes_technical_data";
        String user = "admin";
        String password = "admin";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            System.out.println("Połączenie z bazą danych zostało pomyślnie ustanowione.");


            importTableStructure(stmt, "mDB_structure.sql");
            System.out.println();


            FileWriter writer = new FileWriter("jdbc_mDB_execution_time_1000.csv");
            writer.append("1000/MariaDB/Import\n");
            for (int i = 0; i < 1000; i++) {
                long startTime = System.nanoTime();
                importData(stmt, "mDB_imports_1000.sql");
                long endTime = System.nanoTime();
                float elapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(endTime - startTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(elapsedTimeMs)).append("\n");
                executeSqlJdbc(stmt, "mDB_delete_data.sql");
            }

            writer.flush();
            writer.close();

        } catch (SQLException | IOException e) {
            System.err.println("Błąd podczas próby połączenia z bazą danych lub importu danych:");
            e.printStackTrace();
        }
    }

    private static void importTableStructure(Statement stmt, String filePath) throws IOException, SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            StringBuilder sqlQuery = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sqlQuery.append(line);
                if (line.trim().endsWith(";")) {
                    stmt.executeUpdate(sqlQuery.toString());
                    sqlQuery.setLength(0);
                }
            }
        }
    }

    private static void importData(Statement stmt, String filePath) throws IOException, SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stmt.executeUpdate(line);
            }
        }
    }

    private static void executeSqlJdbc(Statement stmt, String filePath) throws IOException, SQLException {
        // Wykonaj zapytanie SQL z pliku
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            StringBuilder sqlQuery = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sqlQuery.append(line);
                if (line.trim().endsWith(";")) {
                    stmt.executeUpdate(sqlQuery.toString());
                    sqlQuery.setLength(0); // Wyczyść zapytanie
                }
            }
        }
    }
}