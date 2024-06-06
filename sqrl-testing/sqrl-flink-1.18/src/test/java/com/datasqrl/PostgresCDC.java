package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
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

@ExtendWith(MiniClusterExtension.class)
public class PostgresCDC {

  PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15.4");

  @BeforeEach
  public void setup() {
    postgres.start();
    createPostgresTableForFlink();
  }

  @SneakyThrows
  private void createPostgresTableForFlink() {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      String createTableSQL =
          "CREATE TABLE IF NOT EXISTS pgtable (" + "id INT);";
      stmt.execute(createTableSQL);
    }
  }

  @SneakyThrows
  private Connection getConn() {
    return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @SneakyThrows
  private void insertDataIntoPostgresTable() {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      String insertSQL = "INSERT INTO pgtable (id) VALUES "
          + "(1),(2);";
      stmt.execute(insertSQL);
    }
  }

  @SneakyThrows
  @Test
  public void testCdc() {
    String sourcetable = "CREATE TEMPORARY TABLE `sourcetable` (\n" +
        "  `id` INT NOT NULL" +
        ") WITH (" +
        "    'connector' = 'datagen'," +
        "    'number-of-rows' = '10'" +
        ")";

    String sinktable = "CREATE TEMPORARY TABLE `sinktable` (\n" +
        "  `id` INT NOT NULL" +
        ") WITH (" +
        "    'connector' = 'blackhole'" +
        ")";

    // Set up Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(sourcetable);
    tableEnv.executeSql(sinktable);
    TableResult tableResult = tableEnv.executeSql(
        "INSERT INTO `sinktable` SELECT * FROM `sourcetable`");
    CompletableFuture<Object> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
      tableResult.print();
      return null;
    });

    insertDataIntoPostgresTable();


    objectCompletableFuture.get();
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind());
  }
}