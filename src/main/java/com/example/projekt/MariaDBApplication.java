package com.example.projekt;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.sql.*;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class MariaDBApplication {
    private static final int NUM_RECORDS = 10000;

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/planes_technical_data";
        String user = "admin";
        String password = "admin";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            System.out.println("Połączenie z bazą danych zostało pomyślnie ustanowione.");
            importTableStructure(stmt, "MariaDB_data/create_tables.sql");
            System.out.println("Tabele zostały stworzone.");

            String importFileName = "charts/create/mariadb_import_time_" + NUM_RECORDS + ".csv";
            String updateFileName1 = "charts/update/mariadb_update1_time_" + NUM_RECORDS + ".csv";
            String updateFileName2 = "charts/update/mariadb_update2_time_" + NUM_RECORDS + ".csv";
            String deleteFileName = "charts/delete/mariadb_delete_time_" + NUM_RECORDS + ".csv";

            String select1FileName = "charts/read/mariadb_select1_time_" + NUM_RECORDS + ".csv";
            String select2FileName = "charts/read/mariadb_select2_time_" + NUM_RECORDS + ".csv";
            String select3FileName = "charts/read/mariadb_select3_time_" + NUM_RECORDS + ".csv";

            try (FileWriter importWriter = new FileWriter(importFileName);
                 FileWriter updateWriter1 = new FileWriter(updateFileName1);
                 FileWriter updateWriter2 = new FileWriter(updateFileName2);
                 FileWriter deleteWriter = new FileWriter(deleteFileName);
                 FileWriter select1Writer = new FileWriter(select1FileName);
                 FileWriter select2Writer = new FileWriter(select2FileName);
                 FileWriter select3Writer = new FileWriter(select3FileName)) {

                for (int i = 0; i < 1000; i++) {
                    System.out.println(i);
                    long importStartTime = System.nanoTime();

                    importPilotsData(conn, "MariaDB_data/pilots.csv", NUM_RECORDS);
                    System.out.println("Inserted " + NUM_RECORDS + " pilots");

                    importPassengersData(conn, "MariaDB_data/passengers.csv", NUM_RECORDS);
                    System.out.println("Inserted " + NUM_RECORDS + " passengers");

                    importAirportData(conn, "MariaDB_data/airport.csv", NUM_RECORDS);
                    System.out.println("Inserted " + NUM_RECORDS + " airports");

                    importFlightData(conn, "MariaDB_data/airport_tickets.csv", NUM_RECORDS);
                    System.out.println("Inserted " + NUM_RECORDS + " airport tickets");

                    long importEndTime = System.nanoTime();
                    float importElapsedTimeMs = (float) ((TimeUnit.NANOSECONDS.toMillis(importEndTime - importStartTime) / 1000.0) / 4.0);
                    importWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(importElapsedTimeMs)).append("\n");
                    System.out.println("Data imported");

                    executeSelectQueries(stmt, conn, select1Writer, select2Writer, select3Writer, i);
                    executeUpdateQueries(stmt, conn, updateWriter1, updateWriter2, i);
                    executeDeleteQuery(stmt, conn, deleteWriter, i);
                }
            }
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

    private static void executeSelectQueries(Statement stmt, Connection conn, FileWriter select1Writer, FileWriter select2Writer, FileWriter select3Writer, int i) throws SQLException, IOException {
        long select1StartTime = System.nanoTime();
        executeSqlJdbc(stmt, "MariaDB_data/select1.sql");
        conn.commit();
        long select1EndTime = System.nanoTime();
        float select1ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select1EndTime - select1StartTime) / 1000.0);
        select1Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select1ElapsedTimeMs)).append("\n");
        System.out.println("select1");

        long select2StartTime = System.nanoTime();
        executeSqlJdbc(stmt, "MariaDB_data/select2.sql");
        conn.commit();
        long select2EndTime = System.nanoTime();
        float select2ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select2EndTime - select2StartTime) / 1000.0);
        select2Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select2ElapsedTimeMs)).append("\n");
        System.out.println("select2");

        long select3StartTime = System.nanoTime();
        executeSqlJdbc(stmt, "MariaDB_data/select3.sql");
        conn.commit();
        long select3EndTime = System.nanoTime();
        float select3ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(select3EndTime - select3StartTime) / 1000.0);
        select3Writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(select3ElapsedTimeMs)).append("\n");
        System.out.println("select3");
    }

    private static void executeUpdateQueries(Statement stmt, Connection conn, FileWriter updateWriter1, FileWriter updateWriter2, int i) throws SQLException, IOException {
        long updateStartTime = System.nanoTime();
        executeSqlJdbc(stmt, "MariaDB_data/update_1.sql");
        conn.commit();
        System.out.println("1 update");
        executeSqlJdbc(stmt, "MariaDB_data/update_1_1.sql");
        conn.commit();
        System.out.println("2 update");
        long updateEndTime = System.nanoTime();
        float updateElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(updateEndTime - updateStartTime) / 1000.0);
        updateWriter1.append(String.valueOf(i + 1)).append(",").append(String.valueOf(updateElapsedTimeMs)).append("\n");
        System.out.println("dane 1 zaktualizowane");

        long update2StartTime = System.nanoTime();
        executeSqlJdbc(stmt, "MariaDB_data/update_2.sql");
        conn.commit();
        System.out.println("3 update");
        executeSqlJdbc(stmt, "MariaDB_data/update_2_2.sql");
        conn.commit();
        System.out.println("4 update");
        long update2EndTime = System.nanoTime();
        float update2ElapsedTimeMs = (float) (TimeUnit.NANOSECONDS.toMillis(update2EndTime - update2StartTime) / 1000.0);
        updateWriter2.append(String.valueOf(i + 1)).append(",").append(String.valueOf(update2ElapsedTimeMs)).append("\n");
        System.out.println("dane 2 zaktualizowane");
    }

    private static void executeDeleteQuery(Statement stmt, Connection conn, FileWriter deleteWriter, int i) throws SQLException, IOException {
        long deleteStartTime = System.nanoTime();
        conn.setAutoCommit(false);
        executeSqlJdbc(stmt, "MariaDB_data/delete.sql");
        conn.commit();
        long deleteEndTime = System.nanoTime();
        float deleteElapsedTimeMs = (float) ((TimeUnit.NANOSECONDS.toMillis(deleteEndTime - deleteStartTime) / 1000.0) / 4.0);
        deleteWriter.append(String.valueOf(i + 1)).append(",").append(String.valueOf(deleteElapsedTimeMs)).append("\n");
        System.out.println("dane usuniente");
        System.out.println();
    }

    private static void executeSqlJdbc(Statement stmt, String filePath) throws IOException, SQLException {
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


    private static void importPilotsData(Connection conn, String filePath, int numRecords) throws IOException, SQLException {
        String sqlQuery = "INSERT INTO pilots (id, first_name, last_name, gender, age, pilot_rating, years_practice) VALUES (?, ?, ?, ?, ?, ?, ?)";
        importCsvData(conn, filePath, sqlQuery, numRecords, new int[]{0, 4}, new int[]{});
    }

    private static void importPassengersData(Connection conn, String filePath, int numRecords) throws IOException, SQLException {
        String sqlQuery = "INSERT INTO passengers (id, first_name, last_name, email, gender, rating, pilots_id) VALUES (?, ?, ?, ?, ?, ?, ?)";
        importCsvData(conn, filePath, sqlQuery, numRecords, new int[]{0, 6}, new int[]{5});
    }

    private static void importAirportData(Connection conn, String filePath, int numRecords) throws IOException, SQLException {
        String sqlQuery = "INSERT INTO airport (id, airport_code, country, passenger_traffice) VALUES (?, ?, ?, ?)";
        importCsvData(conn, filePath, sqlQuery, numRecords, new int[]{0, 3}, new int[]{});
    }

    private static void importFlightData(Connection conn, String filePath, int numRecords) throws IOException, SQLException {
        String sqlQuery = "INSERT INTO air_tickets (id, id_airport_of_departure, id_airport_of_destination, time_of_leave, time_of_arrival, flight_time, price, class_of_ticket) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        importCsvData(conn, filePath, sqlQuery, numRecords, new int[]{0}, new int[]{});
    }


    private static void importCsvData(Connection conn, String filePath, String sqlQuery, int numRecords, int[] intColumns, int[] floatColumns) throws IOException, SQLException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] nextLine;
            int batchSize = 0;
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
            reader.readNext();
            int recordCount = 0;
            while ((nextLine = reader.readNext()) != null && recordCount < numRecords) {
                for (int i = 0; i < nextLine.length; i++) {
                    if (nextLine[i].isEmpty()) {
                        pstmt.setNull(i + 1, Types.NULL);
                    } else {
                        try {
                            if (contains(intColumns, i)) {
                                pstmt.setInt(i + 1, Integer.parseInt(nextLine[i].trim()));
                            } else if (contains(floatColumns, i)) {
                                pstmt.setFloat(i + 1, Float.parseFloat(nextLine[i].trim()));
                            } else {
                                pstmt.setString(i + 1, nextLine[i].trim());
                            }
                        } catch (NumberFormatException e) {
                            System.err.println("Error parsing column " + (i + 1) + " with value: " + nextLine[i]);
                            pstmt.setNull(i + 1, Types.NULL); // Set NULL if parsing error occurs
                        }
                    }
                }
                pstmt.addBatch();
                batchSize++;
                recordCount++;
                if (batchSize % 1000 == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
            pstmt.executeBatch();
            conn.commit();
            pstmt.close();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean contains(int[] array, int value) {
        for (int i : array) {
            if (i == value) {
                return true;
            }
        }
        return false;
    }

}
