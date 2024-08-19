package com.datasqrl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.duckdb.DuckDBDriver;

public class DuckDBExample {
    public static void main(String[] args) {
        // Load the DuckDB JDBC driver
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        String url = "jdbc:duckdb:"; // In-memory DuckDB instance or you can specify a file path for persistence
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));

        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            stmt.execute("INSTALL iceberg;");
            stmt.execute("LOAD iceberg;");

            // SQL query
            String query = "SELECT * FROM iceberg_scan('/Users/henneberger/sqrl/sqrl-testing/sqrl-flink-1.18/src/test/default/my_table', allow_moved_paths = true);";
            
            // Execute the query
            ResultSet rs = stmt.executeQuery(query);

            // Process the result set
            while (rs.next()) {
                // Assuming your table has some columns
                // Replace `column_name` with your actual column names
                System.out.println("Column 1: " + rs.getString("name"));
                // Add more columns as needed
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
