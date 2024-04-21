package com.example.projekt;

import com.opencsv.exceptions.CsvValidationException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.TimeUnit;
import com.opencsv.CSVReader;
@SpringBootApplication
@RestController
public class MariaDBApplication {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/planes_technical_data";
        String user = "admin";
        String password = "admin";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            System.out.println("Połączenie z bazą danych zostało pomyślnie ustanowione.");
            importTableStructure(stmt, "MariaDB_data/create_tables.sql");
            System.out.println("stworząne tabeli");
            FileWriter importWriter = new FileWriter("MariaDB_results/import_times.csv");
            FileWriter updateWriter = new FileWriter("MariaDB_results/update_times.csv");
            FileWriter deleteWriter = new FileWriter("MariaDB_results/delete_times.csv");
            FileWriter select1Writer = new FileWriter("MariaDB_results/select_1_times.csv");
            FileWriter select2Writer = new FileWriter("MariaDB_results/select_2_times.csv");
            FileWriter select3Writer = new FileWriter("MariaDB_results/select_3_times.csv");
            FileWriter select4Writer = new FileWriter("MariaDB_results/select_4_times.csv");
            FileWriter select5Writer = new FileWriter("MariaDB_results/select_5_times.csv");
            importWriter.append("Iteration,Time (s)\n");
            deleteWriter.append("Iteration,Time (s)\n");
            updateWriter.append("Iteration,Time (s)\n");
            select1Writer.append("Iteration,Time (s)\n");
            select2Writer.append("Iteration,Time (s)\n");
            select3Writer.append("Iteration,Time (s)\n");
            select4Writer.append("Iteration,Time (s)\n");
            select5Writer.append("Iteration,Time (s)\n");


            for (int i = 0; i < 1000; i++) {
                System.out.println(i);
                long importStartTime = System.nanoTime();
                importData(stmt, "MariaDB_data/insert_1000_pilots.sql");
                importData(stmt, "MariaDB_data/insert_1000_passengers.sql");
                conn.commit();
                long importEndTime = System.nanoTime();
                float importElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(importEndTime - importStartTime)/1000.0);
                importWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(importElapsedTimeMs)).append("\n");
                System.out.println("dane zaimportowane ");

                long updateStartTime = System.nanoTime();
                updateDataFromCSV(conn, stmt, "MariaDB_data/pilots.csv");
                conn.commit();
                long updateEndTime = System.nanoTime();
                float updateElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateStartTime) / 1000.0);
                updateWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(updateElapsedTimeMs)).append("\n");
                System.out.println("dane zaktualizowane");

                long select1StartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/select1.sql");
                conn.commit();
                long select1EndTime = System.nanoTime();
                float select1ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select1EndTime - select1StartTime)/1000.0);
                select1Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select1ElapsedTimeMs)).append("\n");
                System.out.println("select1");


                long select2StartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/select2.sql");
                conn.commit();
                long select2EndTime = System.nanoTime();
                float select2ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select2EndTime - select2StartTime)/1000.0);
                select2Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select2ElapsedTimeMs)).append("\n");
                System.out.println("select2");

                long select3StartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/select3.sql");
                conn.commit();
                long select3EndTime = System.nanoTime();
                float select3ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select3EndTime - select3StartTime)/1000.0);
                select3Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select3ElapsedTimeMs)).append("\n");
                System.out.println("select3");

                long select4StartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/select4.sql");
                conn.commit();
                long select4EndTime = System.nanoTime();
                float select4ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select4EndTime - select4StartTime)/1000.0);
                select4Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select4ElapsedTimeMs)).append("\n");
                System.out.println("select4");

                long select5StartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/select5.sql");
                conn.commit();
                long select5EndTime = System.nanoTime();
                float select5ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select5EndTime - select5StartTime)/1000.0);
                select5Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select5ElapsedTimeMs)).append("\n");
                System.out.println("select5");

                long deleteStartTime = System.nanoTime();
                executeSqlJdbc(stmt, "MariaDB_data/delete.sql");
                conn.commit();
                long deleteEndTime = System.nanoTime();
                float deleteElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(deleteEndTime - deleteStartTime) / 1000.0);
                deleteWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(deleteElapsedTimeMs)).append("\n");
                System.out.println("dane usuniente");
                System.out.println();


            }

            importWriter.flush();
            importWriter.close();

            updateWriter.flush();
            updateWriter.close();

            select1Writer.flush();
            select1Writer.close();

            select2Writer.flush();
            select2Writer.close();

            select3Writer.flush();
            select3Writer.close();

            select4Writer.flush();
            select4Writer.close();

            select5Writer.flush();
            select5Writer.close();

            deleteWriter.flush();
            deleteWriter.close();

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
                    sqlQuery.setLength(0);
                }
            }
        }
    }
    private static void updateDataFromCSV(Connection conn, Statement stmt, String filePath) throws IOException, SQLException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] headers = reader.readNext(); // Pobierz nagłówki kolumn

            // Ustawienie zapytania SQL z parametrami
            String updateQuery = "UPDATE pilots SET first_name = ?, last_name = ?, gender = ?, age = ?, pilot_rating = ?, years_practice = ? WHERE id = ?";
            PreparedStatement pstmt = conn.prepareStatement(updateQuery);

            String line[];
            while ((line = reader.readNext()) != null) {
                int id = Integer.parseInt(line[0]); // Zakładając, że pierwsza kolumna to id
                String firstName = line[1].trim();
                String lastName = line[2].trim();
                String gender = line[3].trim();
                int age = Integer.parseInt(line[4]);
                float pilotRating = Float.parseFloat(line[5]);
                int yearsPractice = Integer.parseInt(line[6]);

                // Ustawienie wartości parametrów zapytania PreparedStatement
                pstmt.setString(1, firstName);
                pstmt.setString(2, lastName);
                pstmt.setString(3, gender);
                pstmt.setInt(4, age);
                pstmt.setFloat(5, pilotRating);
                pstmt.setInt(6, yearsPractice);
                pstmt.setInt(7, id);

                // Wykonanie zapytania
                pstmt.executeUpdate();
            }
        } catch (CsvValidationException e) {
            System.err.println("Błąd podczas walidacji pliku CSV:");
            e.printStackTrace();
        }
    }

}