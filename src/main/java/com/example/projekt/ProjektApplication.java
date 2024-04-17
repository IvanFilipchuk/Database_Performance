package com.example.projekt;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.WaitContainerResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class ProjektApplication {

    public static void main(String[] args) {
        try {
            FileWriter writer = new FileWriter("charts/jdbc_execution_time_1000.csv");
            writer.append("1000/PostgreSQL/Import\n");
            executeSqlJdbc("db_structure.sql");
            for (int i = 0; i < 1000; i++) {
                long jdbcStartTime = System.nanoTime();
                executeSqlJdbc("imports_1000.sql");
                long jdbcEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(jdbcEndTime - jdbcStartTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                executeSqlJdbc("delete_data.sql");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileWriter writer = new FileWriter("charts/jdbc_execution_time_10000.csv");
            writer.append("10000/PostgreSQL/Import\n");

            for (int i = 0; i < 1000; i++) {
                long jdbcStartTime = System.nanoTime();
                executeSqlJdbc("imports_10000.sql");
                long jdbcEndTime = System.nanoTime();
                float jdbcElapsedTime = (float) (TimeUnit.NANOSECONDS.toMillis(jdbcEndTime - jdbcStartTime)/1000.0);
                writer.append(String.valueOf(i + 1)).append(",").append(String.valueOf(jdbcElapsedTime)).append("\n");

                executeSqlJdbc("delete_data.sql");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void writeJdbcAverageTimeToCsv(String fileName, double averageElapsedTime) throws IOException {
        try (FileWriter writer = new FileWriter(fileName)) {
            // Write the average JDBC execution time to the CSV file
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
            // Read the SQL script from file
            StringBuilder sql = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sql.append(line).append("\n");
                }
            }

            // Execute the SQL script
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
