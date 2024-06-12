package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(MiniClusterExtension.class)
public class PostgresCDC {

  PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>(
          DockerImageName
            .parse("ankane/pgvector:v0.5.0")
            .asCompatibleSubstituteFor("postgres"))
          .withCommand("postgres -c wal_level=logical");


  @BeforeEach
  public void setup() {
    postgres.start();
    createPostgresTableForFlink();
  }

  @SneakyThrows
  private void createPostgresTableForFlink() {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      String createSourceTableSQL =
          "CREATE TABLE IF NOT EXISTS pgsource (id INT PRIMARY KEY);";
      stmt.execute(createSourceTableSQL);
      String createSinkTableSQL =
          "CREATE TABLE IF NOT EXISTS pgsink (id INT PRIMARY KEY);";
      stmt.execute(createSinkTableSQL);

      // Setup notificaion for pgsink table. On each insert we will
      // trigger a notify with the id of the inserted record.
      String createTrigger =
          "CREATE OR REPLACE FUNCTION notify_on_insert()\n" +
              "RETURNS TRIGGER AS $$\n" +
              "BEGIN\n" +
              "   -- Issue a NOTIFY command with the new record's ID as the payload\n" +
              "   PERFORM pg_notify('my_notify', NEW.id::text);\n" +
              "   RETURN NEW;\n" +
              "END;\n" +
              "$$ LANGUAGE plpgsql;\n" +
              "\n" +
              "CREATE TRIGGER insert_notify_trigger\n" +
              "AFTER INSERT ON pgsink\n" +
              "FOR EACH ROW EXECUTE PROCEDURE notify_on_insert();";
      stmt.execute(createTrigger);
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
      String insertSQL = "INSERT INTO pgsource (id) VALUES (1),(2);";
      stmt.execute(insertSQL);
    }
  }

  @SneakyThrows
  @Test
  public void testCdc() {
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
            "    'slot.name' = 'flink_slot',\n" +
            "    'decoding.plugin.name' = 'pgoutput',\n" +
            "    'debezium.slot.drop_on_stop' = 'false'\n" +
            ")",
        connector, hostname, port, username, password, databaseName, schemaName, "pgsource"
    );

    String sinkTable = String.format(
        "CREATE TEMPORARY TABLE sink_table (\n" +
            "    id INT NOT NULL,\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = '%s',\n" +
            "    'username' = '%s',\n" +
            "    'password' = '%s',\n" +
            "    'table-name' = '%s'\n" +
            ")",
        postgres.getJdbcUrl(), username, password, "pgsink"
    );

    // Listen for notifications
    Callable<List<String>> callable = () -> listenFunction(2);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<List<String>> future = executor.submit(callable);

    // Set up Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(sourceTable);
    tableEnv.executeSql(sinkTable);
    TableResult tableResult = tableEnv.executeSql(
        "INSERT INTO sink_table SELECT * FROM source_table");

    CompletableFuture<Object> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
      tableResult.print();
      return null;
    });

    Thread.sleep(1000); //wait for flink to start
    insertDataIntoPostgresTable();

    try {
      objectCompletableFuture.get(2, TimeUnit.SECONDS);
    } catch (TimeoutException ignored) {}

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

    // Assert that we got the notification for the records inserted into pgsink
    assertEquals(2, future.get().size());
  }

  private List<String> listenFunction(int expectedRecordsCount) throws SQLException {
    try (Connection conn = getConn(); Statement stmt = conn.createStatement()) {
      // issue the LISTEN command to get events when a record is inserted into pgsink
      stmt.execute("LISTEN my_notify");

      // Get the connection's PGConnection for receiving notifications
      PGConnection pgconn = conn.unwrap(PGConnection.class);

      List<String> fetchedRecords = new ArrayList<>();

      while (true) {
        // Check for notifications synchronously
        PGNotification[] notifications = pgconn.getNotifications();

        if (notifications != null) {
          for (PGNotification notification : notifications) {
            // Parse the record ID from the notification parameter
            String recordId = notification.getParameter();

            // Fetch the record and store it
            Statement selectStmt = conn.createStatement();
            ResultSet rs = selectStmt.executeQuery("SELECT * FROM pgsink WHERE id = " + recordId);
            if (rs.next()) {
              fetchedRecords.add(rs.getString("id"));
            }
          }
        }

        // Break the loop after receiving the expected notifications
        if (fetchedRecords.size() == expectedRecordsCount) {
          break;
        }

        // Wait a while before checking again
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      return fetchedRecords;
    }
  }

}