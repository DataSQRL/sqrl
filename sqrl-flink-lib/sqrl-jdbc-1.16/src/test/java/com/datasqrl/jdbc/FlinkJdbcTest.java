package com.datasqrl.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.junit.jupiter.api.Test;

@ExtendWith(MiniClusterExtension.class)
@Disabled
public class FlinkJdbcTest {

    @Test
    public void testFlinkWithPostgres() throws Exception {
        // Start PostgreSQL container
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14")) {
            postgres.start();
            // Establish a connection and create the PostgreSQL table
            try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement stmt = conn.createStatement()) {
                String createTableSQL = "CREATE TABLE test_table (" +
                    "    doubleArray JSONB, " +
                    "    arrayOfRows JSONB, " +
                    "    scalarField INT" +
                    ")";
                stmt.executeUpdate(createTableSQL);
            }

            // Set up Flink environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // Define the schema
            String createSourceTable = "CREATE TABLE datagen_source (" +
                                       "    doubleArray ARRAY<DOUBLE>, " +
                                       "    arrayOfRows ARRAY<ROW<field1 INT, field2 STRING>>, " +
                                       "    scalarField INT" +
                                       ") WITH (" +
                                       "    'connector' = 'datagen'" +
                                       ")";

            String createSinkTable = "CREATE TABLE jdbc_sink (" +
                                     "    doubleArray ARRAY<DOUBLE>, " +
                                     "    arrayOfRows ARRAY<ROW<field1 INT, field2 STRING>>, " +
                                     "    scalarField INT" +
                                     ") WITH (" +
                                     "    'connector' = 'jdbc-sqrl', " +
                                     "    'url' = '" + postgres.getJdbcUrl() + "', " +
                                     "    'table-name' = 'test_table', " +
                                     "    'username' = '" + postgres.getUsername() + "', " +
                                     "    'password' = '" + postgres.getPassword() + "'" +
                                     ")";

            // Register tables in the environment
            tableEnv.executeSql(createSourceTable);
            tableEnv.executeSql(createSinkTable);

            // Set up a simple Flink job
            TableResult tableResult = tableEnv.executeSql(
                "INSERT INTO jdbc_sink SELECT * FROM datagen_source");
            tableResult.print();
        }
    }
}
