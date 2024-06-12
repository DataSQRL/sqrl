package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(MiniClusterExtension.class)
public class PostgresCDC {

  PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>(DockerImageName.parse("debezium/postgres:15-alpine").asCompatibleSubstituteFor("postgres"));
//      new PostgreSQLContainer<>("postgres:15.4");

  @BeforeEach
  public void setup() {
    postgres.start();
    createPostgresTableForFlink();
  }

  @SneakyThrows
  private void createPostgresTableForFlink() {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      String createSourceTableSQL =
          "CREATE TABLE IF NOT EXISTS pgsource (" + "id INT);";
      stmt.execute(createSourceTableSQL);
      String createSinkTableSQL =
          "CREATE TABLE IF NOT EXISTS pgsink (" + "id INT);";
      stmt.execute(createSinkTableSQL);
    }
  }

  @SneakyThrows
  private Connection getConn() {
    return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
        postgres.getPassword());
  }

  @SneakyThrows
  private void insertDataIntoPostgresTable() {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      String insertSQL = "INSERT INTO pgsource (id) VALUES "
          + "(1),(2);";
      stmt.execute(insertSQL);
    }
  }

  @SneakyThrows
  @Test
  public void testCdc() {
//    String sourcetable = "CREATE TEMPORARY TABLE `sourcetable` (\n" +
//        "  `id` INT NOT NULL" +
//        ") WITH (" +
//        "    'connector' = 'datagen'," +
//        "    'number-of-rows' = '10'" +
//        ")";

    String connector = "postgres-cdc";
    String hostname = postgres.getHost();
    String port = postgres.getMappedPort(5432).toString();
    String username = postgres.getUsername();
    String password = postgres.getPassword();
    String databaseName = postgres.getDatabaseName();
    String schemaName = "public";

    String sourceTable = String.format(
        "CREATE TABLE source_table (\n" +
            "    id INT,\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = '%s',\n" +
            "    'hostname' = '%s',\n" +
            "    'port' = '%s',\n" +
            "    'username' = '%s',\n" +
            "    'password' = '%s',\n" +
            "    'database-name' = '%s',\n" +
            "    'schema-name' = '%s',\n" +
            "    'table-name' = '%s',\n" +
            "    'slot.name' = 'flink'\n" +
            ")",
        connector, hostname, port, username, password, databaseName, schemaName, "pgsource"
    );

//    String sinkTable =
//        "CREATE TEMPORARY TABLE sink_table (id INT NOT NULL) WITH ('connector' = 'jdbc', 'url' = '"
//            + postgres.getJdbcUrl() + "', 'table-name' = 'target_table', 'username' = '"
//            + postgres.getUsername() + "', 'password' = '" + postgres.getPassword() + "');";

    String sinkTable = String.format(
        "CREATE TEMPORARY TABLE sink_table (\n" +
            "    id INT NOT NULL,\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = '%s',\n" +
            "    'username' = '%s',\n" +
            "    'password' = '%s',\n" +
            "    'table-name' = '%s',\n" +
            "    'sink.buffer-flush.max-rows' = '1'\n" +
            ")",
        postgres.getJdbcUrl(), username, password, "pgsink1"
    );

    // Set up Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(sourceTable);
    tableEnv.executeSql(sinkTable);
    TableResult tableResult = tableEnv.executeSql(
        "INSERT INTO `sink_table` SELECT * FROM `source_table`");
    CompletableFuture<Object> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
      tableResult.print();
      return null;
    });

    insertDataIntoPostgresTable();

    try {
      objectCompletableFuture.get(5, TimeUnit.SECONDS);
    } catch (TimeoutException ignored) {

    }

    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind());

    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      ResultSet sourceRs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM pgsource;");
      sourceRs.next();
      int sourceCount = sourceRs.getInt("cnt");
      assertEquals(2, sourceCount);

      ResultSet sinkRs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM pgsink;");
      sinkRs.next();
      int sinkCount = sinkRs.getInt("cnt");
      assertEquals(2, sinkCount);
    }
  }
}